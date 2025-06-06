package commands

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/services"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"net"
	"slices"
	"strings"
)

type Command struct {
	Type      string
	Args      []string
	Propagate bool
	Raw       []byte
}

type RequestContext struct {
	Store       store.DataStore
	Replication *services.ReplicationService
	Conn        net.Conn

	Transaction *TransactionService
}

var transactionCommands = []string{
	"MULTI",
	"EXEC",
	"DISCARD",
}

func NewCommand(value resp.Value) (Command, error) {
	if value.Type == resp.Null {
		return Command{}, errors.New("invalid command")
	}

	raw, _ := value.Marshal()

	if value.Type == resp.Array {
		var arr []string

		for _, v := range value.Values {
			if s, err := v.AsString(); err == nil {
				arr = append(arr, s)
			}
		}

		return Command{
			Type:      arr[0],
			Args:      arr[1:],
			Propagate: isPropagatedCommand(arr[0]),
			Raw:       raw,
		}, nil
	}

	typ, _ := value.AsString()

	return Command{
		Type:      typ,
		Propagate: isPropagatedCommand(typ),
		Raw:       raw,
	}, nil
}

func isPropagatedCommand(c string) bool {
	s := strings.ToUpper(c)
	return s == "SET" || s == "DEL"
}

func (c *Command) Execute(handler commandRouter, s RequestContext) ([][]byte, error) {
	var responses [][]byte

	if s.Transaction.IsTransaction(s.Conn) && !slices.Contains(transactionCommands, c.Type) {
		if err := s.Transaction.AddCommand(s.Conn, c); err != nil {
			return nil, err
		}

		value := resp.BulkStringValue("QUEUED")
		v, _ := value.Marshal()

		responses = append(responses, v)
		return responses, nil
	}

	res, err := handler.Handle(*c, s)

	if err != nil {
		return nil, err
	}

	if res.Type == resp.Array && res.Flatten {
		for _, v := range res.Values {
			r, err := v.Marshal()

			if err != nil {
				return nil, err
			}

			responses = append(responses, r)
		}
		return responses, nil
	}

	r, err := res.Marshal()

	if err != nil {
		return nil, err
	}

	return append(responses, r), nil
}

func (c *Command) String() string {
	return fmt.Sprintf("Command: [%s %s]", c.Type, strings.Join(c.Args, " "))
}
