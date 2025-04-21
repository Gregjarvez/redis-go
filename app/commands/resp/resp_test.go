package resp

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRespReader_ReadSimpleValue(t *testing.T) {
	// Define test cases
	tests := []struct {
		name     string
		input    string
		size     int
		expected Value
		isNil    bool
	}{
		{
			name:  "SimpleString",
			input: "+OK\r\n",
			expected: Value{
				Type: SimpleString,
				Raw:  []byte("OK"),
			},
			size:  5,
			isNil: false,
		},
		{
			name:  "Error",
			input: "-This is an error\r\n",
			expected: Value{
				Type: SimpleError,
				Raw:  []byte("This is an error"),
			},
			size:  19,
			isNil: false,
		},
		{
			name:  "Null",
			input: "_\r\n",
			expected: Value{
				Type:  Null,
				IsNil: true,
			},
			size:  0,
			isNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new RespReader with the test input
			reader := NewReader(bytes.NewBufferString(tt.input))

			// Read the value
			value, n, err := reader.ReadValue()

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Check if the value is nil
			if value.IsNil != tt.isNil {
				t.Errorf("Expected IsNil = %v, got %v", tt.isNil, value.IsNil)
			}

			// Check the type
			if value.Type != tt.expected.Type {
				t.Errorf("Expected RespDataType = %v, got %v", tt.expected.Type, value.Type)
			}

			// Check the raw value
			if string(value.Raw) != string(tt.expected.Raw) {
				t.Errorf("Expected Raw = %s, got %s", tt.expected.Raw, value.Raw)
			}

			if n != tt.size {
				t.Errorf("Expected size = %d, got %d", tt.size, n)
			}
		})
	}
}

func TestReader_ReadIntegerValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		size     int
		expected Value
		isNil    bool
	}{
		{
			name:  "Integer",
			input: ":12345\r\n",
			expected: Value{
				Type: Integer,
				Raw:  []byte("12345"),
			},
			size:  8,
			isNil: false,
		},
		{
			name:  "Negative Integer",
			input: ":-12345\r\n",
			expected: Value{
				Type: Integer,
				Raw:  []byte("-12345"),
			},
			size:  9,
			isNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(bytes.NewBufferString(tt.input))
			value, _, err := reader.ReadValue()

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if value.Type != tt.expected.Type {
				t.Errorf("Expected RespDataType = %v, got %v", tt.expected.Type, value.Type)
			}

			if string(value.Raw) != string(tt.expected.Raw) {
				t.Errorf("Expected Raw = %s, got %s", tt.expected.Raw, value.Raw)
			}
		})
	}
}

func TestReader_ReadBulkStringValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		size     int
		expected Value
		isNil    bool
	}{
		{
			name:  "BulkString",
			input: "$12\r\nHello World\r\n",
			expected: Value{
				Type:  BulkString,
				Raw:   []byte("Hello World"),
				IsNil: false,
			},
			size:  15,
			isNil: false,
		},
		{
			name:  "Empty BulkString",
			input: "$0\r\n\r\n",
			expected: Value{
				Type:  Null,
				Raw:   []byte(""),
				IsNil: true,
			},
			size:  3,
			isNil: true,
		},
		{
			name:  "BulkString with Missing CRLF",
			input: "$12\r\nHello World",
			expected: Value{
				Type:  BulkString,
				Raw:   []byte("Hello World"),
				IsNil: false,
			},
			size:  12,
			isNil: false,
		},
		{
			name:  "BulkString with Invalid Byte Length",
			input: "$12\r\nHello\r\n",
			expected: Value{
				Type:  BulkString,
				Raw:   []byte("Hello"),
				IsNil: false,
			},
			size:  0,
			isNil: false,
		},
		{
			name:  "BulkString with Incorrect byte length",
			input: "$5\r\nHelloExtraData\r\n",
			expected: Value{
				Type:  BulkString,
				Raw:   []byte("HelloExtraData"),
				IsNil: false,
			},
			size:  7, // Only the valid part is considered
			isNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(bytes.NewBufferString(tt.input))
			value, _, err := reader.ReadValue()

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if value.Type != tt.expected.Type {
				t.Errorf("Expected RespDataType = %v, got %v", tt.expected.Type, value.Type)
			}

			if string(value.Raw) != string(tt.expected.Raw) {
				t.Errorf("Expected Raw = %s, got %s", tt.expected.Raw, value.Raw)
			}

			if value.IsNil != tt.isNil {
				t.Errorf("Expected IsNil = %v, got %v", tt.isNil, value.IsNil)
			}

			if tt.size != tt.size {
				t.Errorf("Expected size = %d, got %d", tt.size, tt.size)
			}
		})
	}
}

func TestReader_ReadBulkStringValueInvalid(t *testing.T) {
	test := struct {
		name     string
		input    string
		size     int
		expected Value
		isNil    bool
	}{
		name:     "Invalid string format",
		input:    "$ABC\\r\\nHello\\r\\n",
		expected: nullValue,
		size:     0,
	}

	t.Run(test.name, func(t *testing.T) {
		reader := NewReader(bytes.NewBufferString(test.input))
		value, _, err := reader.ReadValue()

		if value.Type != Null {
			t.Errorf("Expected RespDataType = %v, got %v", Null, value.Type)
		}

		expectedError := errors.New("invalid bulk string")

		if err.Error() != expectedError.Error() {
			t.Errorf("Expected error = %v, got %v", expectedError, err)
		}
	})
}

func TestReader_ReadArrayValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		size     int
		expected Value
		isNil    bool
	}{
		{
			name:  "Array",
			input: "*3\r\n$12\r\nHello World\r\n:12345\r\n$-1\r\n",
			expected: Value{
				Type: Array,
				Values: []Value{
					{
						Type:  BulkString,
						Raw:   []byte("Hello World"),
						IsNil: false,
					},
					{
						Type: Integer,
						Raw:  []byte("12345"),
					},
					{
						Type:  Null,
						IsNil: true,
					},
				},
			},
		},
		{
			name:  "Array Sample",
			input: "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
			expected: Value{
				Type: Array,
				Values: []Value{
					BulkStringValue("ECHO"),
					BulkStringValue("hey"),
				},
			},
		},
		{
			name:  "Valid Array",
			input: "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
			expected: Value{
				Type: Array,
				Values: []Value{
					BulkStringValue("PSYNC"),
					BulkStringValue("?"),
					BulkStringValue("-1"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(bytes.NewBufferString(tt.input))
			value, _, err := reader.ReadValue()

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if value.Type != Array {
				t.Errorf("Expected RespDataType = %v, got %v", Array, value.Type)
			}

			if value.IsNil != tt.isNil {
				t.Errorf("Expected IsNil = %v, got %v", tt.isNil, value.IsNil)
			}

			for i, v := range value.Values {
				assert.Equal(t, tt.expected.Values[i].Type, v.Type, "Expected RespDataType = %v, got %v", tt.expected.Values[i].Type, v.Type)
				assert.Equal(t, string(tt.expected.Values[i].Raw), string(v.Raw), "Expected Raw = %s, got %s", tt.expected.Values[i].Raw, v.Raw)
			}
		})
	}
}

func TestReader_ReadBooleanValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		output   bool
	}{
		{
			name:  "Boolean - True",
			input: "#t\r\n",
			expected: Value{
				Type: Boolean,
				Raw:  []byte("t"),
			},
			output: true,
		},
		{
			name:  "Boolean - False",
			input: "#f\r\n",
			expected: Value{
				Type: Boolean,
				Raw:  []byte("f"),
			},
			output: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(bytes.NewBufferString(tt.input))
			value, _, _ := reader.ReadValue()

			if value.Type != tt.expected.Type {
				t.Errorf("Expected RespDataType = %v, got %v", tt.expected.Type, value.Type)
			}

			b, _ := value.AsBool()

			if b != tt.output {
				t.Errorf("Expect boolean value to be = %v, got %v", tt.output, b)
			}
		})
	}
}

func TestConstructAndMarshalComplexNestedArrays(t *testing.T) {
	firstArrayElements := []Value{
		BulkStringValue("temperature"),
		BulkStringValue("36"),
		BulkStringValue("humidity"),
		BulkStringValue("95"),
	}
	firstNestedArray := ArrayValue(
		BulkStringValue("1526985054069-0"),
		ArrayValue(firstArrayElements...),
	)

	secondArrayElements := []Value{
		BulkStringValue("temperature"),
		BulkStringValue("37"),
		BulkStringValue("humidity"),
		BulkStringValue("94"),
	}
	secondNestedArray := ArrayValue(
		BulkStringValue("1526985054079-0"),
		ArrayValue(secondArrayElements...),
	)

	topLevelArray := ArrayValue(firstNestedArray, secondNestedArray)

	respBytes, err := topLevelArray.Marshal()
	assert.NoError(t, err)
	fmt.Println(string(respBytes))

	// Expected RESP representation
	expectedResp := "*2\r\n" +
		"*2\r\n" +
		"$14\r\n" +
		"1526985054069-0\r\n" +
		"*4\r\n" +
		"$11\r\n" +
		"temperature\r\n" +
		"$2\r\n" +
		"36\r\n" +
		"$8\r\n" +
		"humidity\r\n" +
		"$2\r\n" +
		"95\r\n" +
		"*2\r\n" +
		"$14\r\n" +
		"1526985054079-0\r\n" +
		"*4\r\n" +
		"$11\r\n" +
		"temperature\r\n" +
		"$2\r\n" +
		"37\r\n" +
		"$8\r\n" +
		"humidity\r\n" +
		"$2\r\n" +
		"94\r\n"

	assert.Equal(t, expectedResp, string(respBytes))
}

func TestNestedComplexArrays(t *testing.T) {
	input := "*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$8\r\nhumidity\r\n$1\r\n2\r\n*2\r\n$6\r\nbanana\r\n*1\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$8\r\nhumidity\r\n$1\r\n2\r\n"
	reader := NewReader(bytes.NewBufferString(input))
	value, _, err := reader.ReadValue()

	assert.NoError(t, err)
	assert.Equal(t, Array, value.Type)
}
