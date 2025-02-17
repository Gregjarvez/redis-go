package rdb

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/assert"
	"os"
	"slices"
	"testing"
)

func TestHeaderParsing(t *testing.T) {
	// Mock RDB Header: "REDIS0006" (Magic: REDIS, Version: 0006)
	data := []byte("REDIS0006")
	reader := bufio.NewReader(bytes.NewReader(data))

	parser := &Parser{Context: &ParserContext{}, State: &Header{}}
	headerState := &Header{}

	nextState, err := headerState.parse(reader, parser)
	assert.NoError(t, err, "Header parsing should not return an error")
	assert.NotNil(t, nextState, "Next state should not be nil")
	assert.Equal(t, "REDIS", parser.Context.Header.Magic, "Magic identifier should be 'REDIS'")
	assert.Equal(t, 6, parser.Context.Header.Version, "Version should be 6")
}

func TestAuxFieldParsing(t *testing.T) {
	// Mock AUX field: (AUX + Key-Length (3) + "key" + Entries-Length (5) + "value")
	data := []byte{
		AUX,                 // AUX marker
		0x03, 'k', 'e', 'y', // Key length + "key"
		0x05, 'v', 'a', 'l', 'u', 'e', // Entries length + "value"
	}
	reader := bufio.NewReader(bytes.NewReader(data))

	parser := &Parser{Context: &ParserContext{}}
	auxState := &Auxiliary{}

	nextState, err := auxState.parse(reader, parser)
	assert.NoError(t, err, "Auxiliary field parsing should not return an error")
	assert.NotNil(t, nextState, "Next state should not be nil")

	assert.NotNil(t, parser.Context.Aux.Fields, "There should be an auxiliary field")
	assert.Equal(t, parser.Context.Aux.Fields["key"], "value", "Auxiliary key should be 'key'")
}

func TestReadLengthWithEncoding(t *testing.T) {
	data := []byte{
		REDIS_RDB_6BITLEN<<6 | 63,          // 6-bit length: 63
		REDIS_RDB_14BITLEN<<6 | 0x03, 0xE8, // 14-bit length: 1000
	}
	reader := bufio.NewReader(bytes.NewReader(data))

	parser := &Parser{}

	length1, isEncoding1, err1 := parser.readLengthWithEncoding(reader)
	assert.NoError(t, err1, "6-bit length decoding should not return an error")
	assert.False(t, isEncoding1, "6-bit length should not be encoded")
	assert.Equal(t, 63, length1, "6-bit length should be 63")

	length2, isEncoding2, err2 := parser.readLengthWithEncoding(reader)
	assert.NoError(t, err2, "14-bit length decoding should not return an error")
	assert.False(t, isEncoding2, "14-bit length should not be encoded")
	assert.Equal(t, 1000, length2, "14-bit length should be 1000")
}

func TestReadString(t *testing.T) {
	t.Run("Plain String", func(t *testing.T) {
		data := []byte{
			0x05, 'h', 'e', 'l', 'l', 'o', // Length (5) + "hello"
		}
		reader := bufio.NewReader(bytes.NewReader(data))

		parser := &Parser{}
		str, err := parser.readString(reader)
		assert.NoError(t, err, "Reading plain string should not return an error")
		assert.Equal(t, "hello", str, "Expected plain string to be 'hello'")
	})

	t.Run("Encoded INT8", func(t *testing.T) {
		data := []byte{
			REDIS_RDB_ENCVAL<<6 | REDIS_RDB_ENC_INT8, // INT8 encoding
			42,                                       // Entries: 42
		}

		reader := bufio.NewReader(bytes.NewReader(data))

		parser := &Parser{}

		str, err := parser.readString(reader)
		assert.NoError(t, err, "Reading INT8-encoded string should not return an error")
		assert.Equal(t, "42", str, "Expected encoded INT8 to return '42'")
	})
}

func TestReadKeyValuePair(t *testing.T) {
	// Mock Key/Entries pair: Key-Length (3) + "key" + Entries-Length (5) + "value"
	data := []byte{
		0x03, 'k', 'e', 'y', // Key length + "key"
		0x05, 'v', 'a', 'l', 'u', 'e', // Entries length + "value"
	}
	reader := bufio.NewReader(bytes.NewReader(data))

	parser := &Parser{}
	kv, err := parser.readKeyValuePair(reader)
	assert.NoError(t, err, "Reading key-value pair should not return an error")
	assert.Equal(t, "key", kv.Key, "Key should be 'key'")
	assert.Equal(t, "value", kv.Value, "Entries should be 'value'")
}

func TestParseDumpRDBFile(t *testing.T) {
	file, err := os.Open("t_dump.rdb")
	assert.NoError(t, err, "Opening dump.rdb should not return an error")

	defer file.Close()

	parser := NewParser(file)

	err = parser.Parse()
	assert.NoError(t, err, "Parsing dump.rdb should not return an error")

	// Verify header
	assert.Equal(t, "REDIS", parser.Context.Header.Magic, "Magic should be 'REDIS'")
	assert.GreaterOrEqual(t, parser.Context.Header.Version, 1, "Version should be greater than or equal to 1")

	assert.NotEmptyf(t, parser.Context.Aux, "There should be auxiliary fields")

	auxFields := []string{"redis-ver", "redis-bits", "ctime", "used-mem", "aof-base"}
	for k := range parser.Context.Aux.Fields {
		assert.Equal(t, slices.Contains(auxFields, string(k)), true, "Auxiliary field key should match")
	}

	// Verify databases
	assert.NotEmpty(t, parser.Context.Databases, "There should be databases")
	assert.Equal(t, 0, parser.Context.Databases[0].ID, "First database ID should be 0")
	assert.Equal(t, 2, len(parser.Context.Databases[0].Entries), "First database should have 2 entries")
}
