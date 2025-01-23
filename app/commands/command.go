package commands

import (
	"errors"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

type Command struct {
	Type string
	Args []string
}

type ServerContext struct {
	Store store.DataStore
	Info  config.Info
}

func NewCommand(value resp.Value) (Command, error) {
	if value.Type == resp.Null {
		return Command{}, errors.New("invalid command")
	}
	if value.Type == resp.Array {
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

func (c Command) Execute(handler commandRouter, s ServerContext) ([][]byte, error) {
	res, err := handler.Handle(c, s)

	if err != nil {
		return nil, err
	}

	var responses [][]byte

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
