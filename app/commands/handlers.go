package commands

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"strings"
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
	},
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
		return resp.ArrayValue(resp.BulkStringValue(arg), resp.BulkStringValue(*config.Config.DbFilename)), nil
	case "dir":
		return resp.ArrayValue(resp.BulkStringValue(arg), resp.BulkStringValue(*config.Config.Dir)), nil
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
		return resp.BulkStringValue(context.Info.String()), nil
	default:
		return resp.BulkStringValue("ERR: unknown argument"), nil
	}
}

func replConfigHandler(c Command, _ RequestContext) (resp.Value, error) {
	return resp.BulkStringValue("OK"), nil
}

func pSyncHandler(c Command, s RequestContext) (resp.Value, error) {
	s.Info.AddReplica(s.Conn)

	return resp.FlatArrayValue(
		resp.BulkStringValue(
			fmt.Sprintf(
				"FULLRESYNC %s %v",
				s.Info.MasterReplid,
				s.Info.MasterReplOffset,
			),
		),
		resp.Value{Type: resp.BulkString, Raw: s.Store.Dump(), BulkLike: true},
	), nil
}

func docHandler(c Command, s RequestContext) (resp.Value, error) {
	switch strings.ToLower(c.Args[0]) {
	case "docs":
		return resp.BulkStringValue("Welcome"), nil
	default:
		return resp.BulkStringValue("Welcome"), nil
	}
}
