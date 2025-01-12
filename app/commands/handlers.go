package commands

import (
	"errors"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"strings"
)

type commandHandler func(c Command) (result resp.Value, err error)

type CommandRouter struct {
	handlers map[string]commandHandler
}

func (c *CommandRouter) canHandle(cmd string) bool {
	_, ok := c.handlers[cmd]
	return ok
}

func (c *CommandRouter) Handle(cmd Command) (result resp.Value, err error) {
	typ := strings.ToUpper(cmd.Type)

	if !c.canHandle(typ) {
		return resp.NullValue(), errors.New("unknown Command")
	}

	return c.handlers[typ](cmd)
}

var DefaultHandlers = CommandRouter{
	handlers: map[string]commandHandler{
		"PING": pingHandler,
		"ECHO": echoHandler,
	},
}

func pingHandler(c Command) (resp.Value, error) {
	return resp.StringValue("PONG"), nil
}

func echoHandler(c Command) (resp.Value, error) {
	return resp.BulkStringValue(c.Args[0]), nil
}
