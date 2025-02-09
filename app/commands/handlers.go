package commands

import (
	"context"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/services"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type commandHandler func(c Command, s RequestContext) (result resp.Value, err error)

type commandRouter struct {
	handlers map[string]commandHandler
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

var DefaultHandlers = commandRouter{
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
	},
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
		return resp.IntegerValue(len(s.Replication.Replicas)), nil
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

	return resp.IntegerValue(int(finalAcked)), nil
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

	return resp.BulkStringValue(record.String()), nil
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
