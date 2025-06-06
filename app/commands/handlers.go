package commands

import (
	"context"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/services"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/store/stream"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type commandHandler func(c Command, s RequestContext) (result resp.Value, err error)

type commandRouter struct {
	handlers map[string]commandHandler
}

func NewCommandRouter() commandRouter {
	return commandRouter{
		handlers: map[string]commandHandler{
			"PING":     pingHandler,
			"ECHO":     echoHandler,
			"SET":      setHandler,
			"GET":      getHandler,
			"CONFIG":   configHandler,
			"KEYS":     keysHandler,
			"INFO":     infoHandler,
			"REPLCONF": replConfigHandler,
			"PSYNC":    pSyncHandler,
			"COMMAND":  docHandler,
			"WAIT":     waitHandler,
			"TYPE":     typeHandler,
			"XADD":     xAddHandler,
			"XRANGE":   xRangeHandler,
			"XREAD":    xReadHandler,
			"INCR":     incrHandler,
			"MULTI":    multiHandler,
			"EXEC":     execHandler,
			"DISCARD":  discardHandler,
		},
	}
}

func (c *commandRouter) canHandle(cmd string) bool {
	_, ok := c.handlers[cmd]
	return ok
}

func (c *commandRouter) Handle(cmd Command, s RequestContext) (resp.Value, error) {
	typ := strings.ToUpper(cmd.Type)

	if !c.canHandle(typ) {
		return resp.NullValue(), errors.New("unknown Command")
	}

	return c.handlers[typ](cmd, s)
}

var DefaultHandlers = NewCommandRouter()

func discardHandler(c Command, s RequestContext) (resp.Value, error) {
	if !s.Transaction.IsTransaction(s.Conn) {
		return resp.ErrorValue("ERR DISCARD without MULTI"), nil
	}

	err := s.Transaction.Discard(s.Conn)

	if err != nil {
		return resp.ErrorValue("ERR DISCARD without MULTI"), nil
	}

	return resp.StringValue("OK"), nil
}

func execHandler(c Command, s RequestContext) (resp.Value, error) {
	if !s.Transaction.IsTransaction(s.Conn) {
		return resp.ErrorValue("ERR EXEC without MULTI"), nil
	}

	response, err := s.Transaction.Commit(s.Conn, s)

	if response == nil {
		return resp.ArrayValue(), err
	}

	return resp.ArrayValue(response...), err

}

func multiHandler(c Command, s RequestContext) (resp.Value, error) {
	if s.Transaction.IsTransaction(s.Conn) {
		return resp.ErrorValue("ERR MULTI calls can not be nested"), nil
	}

	err := s.Transaction.Begin(s.Conn)

	if err != nil {
		return resp.ErrorValue(err.Error()), nil
	}

	return resp.StringValue("OK"), nil
}

func incrHandler(c Command, s RequestContext) (resp.Value, error) {
	key := c.Args[0]

	v, err := s.Store.Increment(key)

	if err != nil {
		return resp.ErrorValue(err.Error()), nil
	}

	return resp.IntegerValue(v), nil
}

const (
	blocking int64 = 0
)

func xReadHandler(c Command, s RequestContext) (resp.Value, error) {
	keys, ids, blockMillis := parseXReadArgs(c.Args)

	if blockMillis == blocking {
		return handleBlockingRead(keys, ids, s.Store)
	}

	if blockMillis > 0 {
		if slices.Contains(ids, "$") {
			sto := s.Store.Read(keys[0])
			stream, ok := sto.(*stream.Stream)

			if ok {
				ids[0] = stream.TailPrefix
			}
		}

		time.Sleep(time.Duration(blockMillis) * time.Millisecond)
	}

	return readStreams(keys, ids, s.Store)
}

func xRangeHandler(c Command, s RequestContext) (resp.Value, error) {
	key := c.Args[0]
	start := c.Args[1]
	end := c.Args[2]

	fmt.Println("XRANGE: ", key, start, end)

	trie := s.Store.Read(key).(*stream.Stream)

	if trie == nil {
		fmt.Println("Stream not found")
		return resp.NullValue(), nil
	}

	result := trie.Range(start, end)
	r := make([]resp.Value, 0, len(result))

	for _, entry := range result {
		id := resp.BulkStringValue(entry.Id)
		values := make([]resp.Value, 0, 2*len(entry.Elements))

		for k, v := range entry.Elements {
			values = append(values, resp.BulkStringValue(k), resp.BulkStringValue(fmt.Sprintf("%v", v)))
		}

		r = append(r, resp.ArrayValue(id, resp.ArrayValue(values...)))
	}

	return resp.ArrayValue(r...), nil
}

func xAddHandler(c Command, s RequestContext) (resp.Value, error) {
	args := Chunk(c.Args, 2)

	key := args[0]
	entries := args[1:]

	if len(key) == 0 {
		panic("stream has no name arguments")
	}

	fmt.Println("XADD: ", key[0], key[1], entries)

	k, err := s.Store.XAdd(key[0], key[1], entries)

	if err != nil {
		return resp.ErrorValue(err.Error()), nil
	}

	if k != key[0] {
		return resp.BulkStringValue(k), nil
	}

	return resp.StringValue(key[0]), nil
}

func typeHandler(c Command, s RequestContext) (resp.Value, error) {
	key := c.Args[0]

	record := s.Store.Read(key)
	if record == nil {
		return resp.StringValue("none"), nil
	}
	return resp.StringValue(record.GetType()), nil
}

func waitHandler(c Command, s RequestContext) (result resp.Value, err error) {
	if len(c.Args) < 2 {
		return resp.ErrorValue("ERR: not enough arguments"), nil
	}

	var (
		acked           atomic.Int32
		replicaCount, _ = strconv.Atoi(c.Args[0])
		timeout, _      = strconv.Atoi(c.Args[1])
	)

	if replicaCount == 0 {
		return resp.IntegerValue(0), nil
	}

	// check documentation
	if len(s.Store.Keys()) == 0 {
		return resp.IntegerValue(int64(len(s.Replication.Replicas))), nil
	}

	replicas := s.Replication.Replicas
	results := make(chan struct{}, len(replicas))

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
	defer cancel()

	for _, replica := range replicas {
		tcpConn := replica.Conn

		if err := s.Replication.GetAck(tcpConn); err != nil {
			fmt.Println("Failed to send GetAck request to replica:", tcpConn.RemoteAddr())
			continue
		}

		go func(r *services.Replica) {
			select {
			case <-r.Ack:
				results <- struct{}{}
			case <-ctx.Done():
				fmt.Println("Timeout or cancel signal received for replica:", r.Conn.RemoteAddr())
			}
		}(replica)
	}

	go func() {
		for range results {
			acked.Add(1)
		}
	}()

	<-ctx.Done()

	finalAcked := acked.Load()
	fmt.Println("Final acked count:", finalAcked)

	return resp.IntegerValue(int64(finalAcked)), nil
}

func pingHandler(_ Command, _ RequestContext) (resp.Value, error) {
	return resp.StringValue("PONG"), nil
}

func echoHandler(c Command, _ RequestContext) (resp.Value, error) {
	return resp.BulkStringValue(c.Args[0]), nil
}

func getHandler(c Command, context RequestContext) (resp.Value, error) {
	if len(c.Args) == 0 {
		err := errors.New("no key provided")
		return resp.ErrorValue(err.Error()), err
	}

	key := c.Args[0]
	record := context.Store.Read(key)

	if record == nil {
		return resp.BulkStringValue("", true), nil
	}

	return resp.BulkStringValue(record.GetValue()), nil
}

func setHandler(c Command, context RequestContext) (resp.Value, error) {
	now := time.Now()
	args, err := parseSetCommandOptions(c.Args)

	if err != nil {
		return resp.ErrorValue(err.Error()), err
	}

	ttl := args.ExpireMillis
	var unixTTL int64

	if ttl != 0 {
		unixTTL = now.Add(time.Duration(ttl) * time.Millisecond).UnixMilli()
	}

	err = context.Store.Write(args.Key, args.Value, store.Options{
		TTL: unixTTL,
	})

	if err != nil {
		return resp.ErrorValue(err.Error()), err
	}

	return resp.StringValue("OK"), nil
}

func configHandler(c Command, _ RequestContext) (result resp.Value, err error) {
	cmd := c.Args[0]
	arg := c.Args[1]

	if !strings.EqualFold(cmd, "get") {
		return resp.ErrorValue("unknown command"), nil
	}

	switch arg {
	case "dbfilename":
		return resp.ArrayValue(resp.BulkStringValue(arg), resp.BulkStringValue(*services.Config.DbFilename)), nil
	case "dir":
		return resp.ArrayValue(resp.BulkStringValue(arg), resp.BulkStringValue(*services.Config.Dir)), nil
	default:
		return resp.ErrorValue("unknown argument"), nil
	}
}

func keysHandler(c Command, context RequestContext) (resp.Value, error) {
	if len(c.Args) == 0 {
		err := errors.New("no pattern provided")
		return resp.ErrorValue(err.Error()), err
	}

	pattern := c.Args[0]
	keys := context.Store.Keys()

	// support for the "*" pattern for now
	keysToResp := make([]resp.Value, len(keys))

	if pattern == "*" {
		for i, k := range keys {
			keysToResp[i] = resp.BulkStringValue(k)
		}
	}

	return resp.ArrayValue(keysToResp...), nil
}

func infoHandler(c Command, context RequestContext) (resp.Value, error) {
	if len(c.Args) == 0 {
		return resp.ErrorValue("ERR: no arguments provided"), nil
	}
	arg := strings.ToLower(c.Args[0])

	switch arg {
	case "replication":
		return resp.BulkStringValue(context.Replication.String()), nil
	default:
		return resp.BulkStringValue("ERR: unknown argument"), nil
	}
}

func replConfigHandler(c Command, s RequestContext) (resp.Value, error) {
	switch strings.ToUpper(c.Args[0]) {
	case "GETACK":
		return resp.ArrayValue(
			resp.BulkStringValue("REPLCONF"),
			resp.BulkStringValue("ACK"),
			resp.BulkStringValue(strconv.FormatInt(s.Replication.GetReplOffset(), 10)),
		), nil
	case "ACK":
		if s.Conn != nil {
			replica := s.Replication.GetReplica(s.Conn)
			if replica == nil {
				return resp.ErrorValue("ERR: no replica connection"), nil
			}

			fmt.Println("Replica ack received: ", replica.Conn.RemoteAddr().String())
			replica.Ack <- true
		}
		return resp.FlatArrayValue(), nil
	default:
		return resp.StringValue("OK"), nil
	}
}

func pSyncHandler(c Command, s RequestContext) (resp.Value, error) {
	return resp.FlatArrayValue(
		resp.StringValue(
			fmt.Sprintf(
				"FULLRESYNC %s %v",
				s.Replication.MasterReplid,
				"0",
			),
		),
		resp.BulkLikeStringValue(s.Store.Dump()),
	), nil
}

func docHandler(c Command, s RequestContext) (resp.Value, error) {
	switch strings.ToUpper(c.Args[0]) {
	case "DOCS":
		return resp.BulkStringValue("Welcome"), nil
	default:
		return resp.BulkStringValue("Welcome"), nil
	}
}
