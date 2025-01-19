package commands

import (
	"bytes"
	"github.com/codecrafters-io/redis-starter-go/app/commands/resp"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewCommand(t *testing.T) {
	t.Run("should return Command with Type and Args when input is an RESP array", func(t *testing.T) {
		mockInput := "*3\r\n$4\r\nPING\r\n$4\r\nKEY1\r\n$4\r\nKEY2\r\n"
		reader := bytes.NewBufferString(mockInput)
		value, _, _ := resp.NewReader(reader).ReadValue()

		cmd, err := NewCommand(value)

		assert.NoError(t, err)
		assert.Equal(t, "PING", cmd.Type)
		assert.Equal(t, []string{"KEY1", "KEY2"}, cmd.Args)
	})

	t.Run("should return Command with only Type when input is a single RESP value", func(t *testing.T) {
		mockInput := "+PONG\r\n" // RESP simple string: "PONG"
		reader := bytes.NewBufferString(mockInput)
		value, _, _ := resp.NewReader(reader).ReadValue()

		cmd, err := NewCommand(value)
		assert.NoError(t, err)
		assert.Equal(t, "PONG", cmd.Type)
		assert.Nil(t, cmd.Args)
	})

	t.Run("should return error on invalid RESP input", func(t *testing.T) {
		mockInput := "INVALID DATA\r\n" // Not a valid RESP format
		reader := bytes.NewBufferString(mockInput)
		value, _, _ := resp.NewReader(reader).ReadValue()

		_, err := NewCommand(value)
		assert.Error(t, err)
	})
}
