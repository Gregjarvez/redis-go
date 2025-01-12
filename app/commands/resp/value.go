package resp

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type DataType byte

type Value struct {
	IsNil  bool     // To denote a null value
	Raw    []byte   // Raw byte data for strings, integers, and booleans
	typ    DataType // Type of the data (e.g., Integer, BulkString, Boolean, etc.)
	Values []Value  // For arrays, a slice of values (allows recursive definition)
}

const (
	SimpleString DataType = '+'
	SimpleError  DataType = '-'
	Integer      DataType = ':'
	BulkString   DataType = '$'
	Array        DataType = '*'
	Null         DataType = '_'
	Boolean      DataType = '#'
)

const TrueValue = "t"

var nullValue = Value{
	IsNil: true,
	typ:   Null,
}

func (v *Value) Type() DataType {
	return v.typ
}

func StringValue(s string) Value {
	return Value{
		typ: SimpleString,
		Raw: []byte(s),
	}
}

func BulkStringValue(s string) Value {
	return Value{
		typ: BulkString,
		Raw: []byte(s),
	}
}

func NullValue() Value {
	return nullValue
}

func (t DataType) String() string {
	switch t {
	case SimpleString:
		return "SimpleString"
	case SimpleError:
		return "Error"
	case Integer:
		return "Integer"
	case BulkString:
		return "BulkString"
	case Null:
		return "Null"
	case Boolean:
		return "Boolean"
	case Array:
		return "Array"
	default:
		return "Unknown"
	}
}

func (v *Value) String() string {
	switch v.typ {
	case SimpleString, SimpleError, Integer:
		return string(v.Raw)
	case Array:
		return fmt.Sprintf("%v", v.Values)
	default:
		return fmt.Sprintf("%v", v.Raw)
	}
}

func (v *Value) AsInt() (int, error) {
	if v.typ != Integer || v.IsNil {
		return 0, errors.New("value not an integer or is nil")
	}
	return strconv.Atoi(string(v.Raw))
}

func (v *Value) AsString() (string, error) {
	switch v.typ {
	case SimpleString, SimpleError, Integer, BulkString:
		return string(v.Raw), nil
	default:
		return "", errors.New("value not a string")
	}
}

func (v *Value) AsBool() (bool, error) {
	if v.typ != Boolean || v.IsNil {
		return false, errors.New("value not a boolean or is nil")
	}

	return strings.EqualFold(string(v.Raw), TrueValue), nil
}

func (v *Value) AsArray() ([]Value, error) {
	if v.typ != Array || v.IsNil {
		return nil, errors.New("value not an array or is nil")
	}
	return v.Values, nil
}

func (v *Value) Marshal() ([]byte, error) {
	switch v.typ {
	case SimpleString:
		return format(SimpleString, v.Raw), nil
	case SimpleError:
		return format(SimpleError, v.Raw), nil
	case Integer:
		return format(Integer, v.Raw), nil
	case Null:
		return format(Null, nil), nil
	case BulkString:
		if v.IsNil {
			return format(BulkString, []byte("-1")), nil
		}
		return format(BulkString, []byte(fmt.Sprintf("%d\r\n%s", len(v.Raw), v.Raw))), nil
	case Array:
		var b strings.Builder
		b.Write([]byte(fmt.Sprintf("*%d\r\n", len(v.Values))))

		for _, v := range v.Values {
			resp, err := v.Marshal()

			if err != nil {
				return nil, err
			}
			b.Write(resp)
		}

		return []byte(b.String()), nil
	}

	return nil, errors.New("invalid data type")
}

func format(prefix DataType, raw []byte) []byte {
	var b bytes.Buffer
	b.Write([]byte(string(prefix)))
	b.Write(raw)
	b.Write([]byte("\r\n"))

	return b.Bytes()
}
