package commands

import (
	"errors"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

type Command struct {
	Type string
	Args []string
}

func NewCommand(value resp.Value) (Command, error) {
	if value.Type() == resp.Null {
		return Command{}, errors.New("invalid command")
	}
	if value.Type() == resp.Array {
		var arr []string

		for _, v := range value.Values {
			if s, err := v.AsString(); err == nil {
				arr = append(arr, s)
			}
		}

		return Command{
			Type: arr[0],
			Args: arr[1:],
		}, nil
	}

	typ, _ := value.AsString()

	return Command{
		Type: typ,
	}, nil
}

func (c Command) Execute(handler commandRouter, s store.DataStore) (resp.Value, error) {
	return handler.Handle(c, s)
}
