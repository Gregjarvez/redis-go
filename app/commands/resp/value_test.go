package resp

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringValue(t *testing.T) {
	v := StringValue("hello")
	assert.Equal(t, SimpleString, v.typ, "StringValue should set the correct DataType")
	assert.Equal(t, []byte("hello"), v.Raw, "StringValue should set the correct Raw value")
}

func TestNullValue(t *testing.T) {
	v := NullValue()
	assert.Equal(t, Null, v.typ, "NullValue should have the DataType Null")
	assert.True(t, v.IsNil, "NullValue should set IsNil to true")
}

func TestDataTypeString(t *testing.T) {
	tests := map[DataType]string{
		SimpleString: "SimpleString",
		SimpleError:  "Error",
		Integer:      "Integer",
		BulkString:   "BulkString",
		Null:         "Null",
		Boolean:      "Boolean",
		Array:        "Array",
		DataType(0):  "Unknown",
	}
	for input, expected := range tests {
		assert.Equal(t, expected, input.String(), "String() should return the correct DataType description")
	}
}

func TestValue_String(t *testing.T) {
	tests := []struct {
		value    Value
		expected string
	}{
		{Value{typ: SimpleString, Raw: []byte("OK")}, "OK"},
		{Value{typ: SimpleError, Raw: []byte("ERROR")}, "ERROR"},
		{Value{typ: Integer, Raw: []byte("123")}, "123"},
		{Value{typ: Array, Values: []Value{StringValue("A"), StringValue("B")}}, "[{false [65] 43 []} {false [66] 43 []}]"}, // fmt-style array output
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.value.String(), "String() should return the expected string representation")
	}
}

func TestValue_AsInt(t *testing.T) {
	tests := []struct {
		value    Value
		expected int
		err      error
	}{
		{Value{typ: Integer, Raw: []byte("123")}, 123, nil},
		{Value{typ: Integer, IsNil: true}, 0, errors.New("value not an integer or is nil")},
		{Value{typ: SimpleString, Raw: []byte("NOT_INT")}, 0, errors.New("value not an integer or is nil")},
	}
	for _, tt := range tests {
		result, err := tt.value.AsInt()
		if tt.err != nil {
			assert.EqualError(t, err, tt.err.Error(), "AsInt() should return the correct error")
		} else {
			assert.NoError(t, err, "AsInt() should not return an error")
		}
		assert.Equal(t, tt.expected, result, "AsInt() should return the correct integer value")
	}
}

func TestValue_AsString(t *testing.T) {
	tests := []struct {
		value    Value
		expected string
		err      error
	}{
		{Value{typ: SimpleString, Raw: []byte("OK")}, "OK", nil},
		{Value{typ: BulkString, Raw: []byte("BULK")}, "BULK", nil},
		{Value{typ: Integer, Raw: []byte("123")}, "123", nil},
		{Value{typ: Array, Values: []Value{}}, "", errors.New("value not a string")},
	}
	for _, tt := range tests {
		result, err := tt.value.AsString()
		if tt.err != nil {
			assert.EqualError(t, err, tt.err.Error(), "AsString() should return the correct error")
		} else {
			assert.NoError(t, err, "AsString() should not return an error")
		}
		assert.Equal(t, tt.expected, result, "AsString() should return the correct string value")
	}
}

func TestValue_AsBool(t *testing.T) {
	tests := []struct {
		value    Value
		expected bool
		err      error
	}{
		{Value{typ: Boolean, Raw: []byte(TrueValue)}, true, nil},
		{Value{typ: Boolean, Raw: []byte("f")}, false, nil},
		{Value{typ: Boolean, IsNil: true}, false, errors.New("value not a boolean or is nil")},
		{Value{typ: SimpleString, Raw: []byte("not_bool")}, false, errors.New("value not a boolean or is nil")},
	}
	for _, tt := range tests {
		result, err := tt.value.AsBool()
		if tt.err != nil {
			assert.EqualError(t, err, tt.err.Error(), "AsBool() should return the correct error")
		} else {
			assert.NoError(t, err, "AsBool() should not return an error")
		}
		assert.Equal(t, tt.expected, result, "AsBool() should return the correct boolean value")
	}
}

func TestValue_AsArray(t *testing.T) {
	tests := []struct {
		value    Value
		expected []Value
		err      error
	}{
		{Value{typ: Array, Values: []Value{StringValue("A"), StringValue("B")}}, []Value{StringValue("A"), StringValue("B")}, nil},
		{Value{typ: Array, IsNil: true}, nil, errors.New("value not an array or is nil")},
		{Value{typ: SimpleString, Raw: []byte("not_array")}, nil, errors.New("value not an array or is nil")},
	}
	for _, tt := range tests {
		result, err := tt.value.AsArray()
		if tt.err != nil {
			assert.EqualError(t, err, tt.err.Error(), "AsArray() should return the correct error")
		} else {
			assert.NoError(t, err, "AsArray() should not return an error")
		}
		assert.Equal(t, tt.expected, result, "AsArray() should return the correct array value")
	}
}

func TestValue_Marshal(t *testing.T) {
	tests := []struct {
		value    Value
		expected string
		err      error
	}{
		{Value{typ: SimpleString, Raw: []byte("OK")}, "+OK\r\n", nil},
		{Value{typ: SimpleError, Raw: []byte("ERROR")}, "-ERROR\r\n", nil},
		{Value{typ: Integer, Raw: []byte("123")}, ":123\r\n", nil},
		{Value{typ: Null, IsNil: true}, "_\r\n", nil},
		{Value{typ: BulkString, Raw: []byte("BULK")}, "$4\r\nBULK\r\n", nil},
		{Value{typ: BulkString, IsNil: true}, "$-1\r\n", nil},
		{Value{typ: Array, Values: []Value{StringValue("A"), StringValue("B")}}, "*2\r\n+A\r\n+B\r\n", nil},
	}
	for _, tt := range tests {
		result, err := tt.value.Marshal()
		if tt.err != nil {
			assert.EqualError(t, err, tt.err.Error(), "Marshal() should return the correct error")
		} else {
			assert.NoError(t, err, "Marshal() should not return an error")
		}
		assert.Equal(t, tt.expected, string(result), "Marshal() should return the correct RESP serialization")
	}
}
