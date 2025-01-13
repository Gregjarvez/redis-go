package commands

import (
	"errors"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"strings"
)

type commandHandler func(c Command, s store.DataStore) (result resp.Value, err error)

type CommandRouter struct {
	handlers map[string]commandHandler
}

func (c *CommandRouter) canHandle(cmd string) bool {
	_, ok := c.handlers[cmd]
	return ok
}

func (c *CommandRouter) Handle(cmd Command, s store.DataStore) (result resp.Value, err error) {
	typ := strings.ToUpper(cmd.Type)

	if !c.canHandle(typ) {
		return resp.NullValue(), errors.New("unknown Command")
	}

	return c.handlers[typ](cmd, s)
}

var DefaultHandlers = CommandRouter{
	handlers: map[string]commandHandler{
		"PING": pingHandler,
		"ECHO": echoHandler,
		"SET":  setHandler,
		"GET":  getHandler,
	},
}

func pingHandler(_ Command, _ store.DataStore) (resp.Value, error) {
	return resp.StringValue("PONG"), nil
}

func echoHandler(c Command, _ store.DataStore) (resp.Value, error) {
	return resp.BulkStringValue(c.Args[0]), nil
}

func getHandler(c Command, s store.DataStore) (resp.Value, error) {
	if len(c.Args) == 0 {
		err := errors.New("no key provided")
		return resp.ErrorValue(err.Error()), err
	}

	key := c.Args[0]
	record := s.Read(key)

	if record == nil {
		return resp.NullValue(), nil
	}

	return resp.BulkStringValue(record.String()), nil
}

func setHandler(c Command, s store.DataStore) (resp.Value, error) {
	args, err := parseSetCommandOptions(c.Args)

	if err != nil {
		return resp.ErrorValue(err.Error()), err
	}

	err = s.Write(args.Key, args.Value, store.Options{
		TTL: args.ExpireAtMillis,
	})

	if err != nil {
		return resp.ErrorValue(err.Error()), err
	}

	return resp.StringValue("OK"), nil
}
