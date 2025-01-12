package commands

import (
	"bytes"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/stretchr/testify/assert"
)

func TestNewCommand(t *testing.T) {
	t.Run("should return Command with Type and Args when input is an RESP array", func(t *testing.T) {
		mockInput := "*3\r\n$4\r\nPING\r\n$4\r\nKEY1\r\n$4\r\nKEY2\r\n" // RESP array: ["PING", "KEY1", "KEY2"]
		reader := bytes.NewBufferString(mockInput)

		cmd, err := commands.NewCommand(reader)
		assert.NoError(t, err)
		assert.Equal(t, "PING", cmd.Type)
		assert.Equal(t, []string{"KEY1", "KEY2"}, cmd.Args)
	})

	t.Run("should return Command with only Type when input is a single RESP value", func(t *testing.T) {
		mockInput := "+PONG\r\n" // RESP simple string: "PONG"
		reader := bytes.NewBufferString(mockInput)

		cmd, err := commands.NewCommand(reader)
		assert.NoError(t, err)
		assert.Equal(t, "PONG", cmd.Type)
		assert.Nil(t, cmd.Args)
	})

	t.Run("should return error on invalid RESP input", func(t *testing.T) {
		mockInput := "INVALID DATA\r\n" // Not a valid RESP format
		reader := bytes.NewBufferString(mockInput)

		_, err := commands.NewCommand(reader)
		assert.Error(t, err)
	})
}
