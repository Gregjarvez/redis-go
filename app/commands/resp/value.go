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
	IsNil    bool     // To denote a null value
	Raw      []byte   // Raw byte data for strings, integers, and booleans
	Type     DataType // Type of the data (e.g., Integer, BulkString, Boolean, etc.)
	Values   []Value  // For arrays, a slice of values (allows recursive definition)
	Flatten  bool
	BulkLike bool
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
	Type:  Null,
}

func StringValue(s string) Value {
	return Value{
		Type: SimpleString,
		Raw:  []byte(s),
	}
}

func IntegerValue(i int64) Value {
	return Value{
		Type:  Integer,
		Raw:   []byte(strconv.FormatInt(i, 10)),
		IsNil: i == 0,
	}
}

func BulkLikeStringValue(s []byte) Value {
	return Value{
		Type:     BulkString,
		Raw:      s,
		BulkLike: true,
	}
}

func BulkStringValue(s string, isNil ...bool) Value {
	return Value{
		Type:  BulkString,
		Raw:   []byte(s),
		IsNil: len(isNil) > 0 && isNil[0],
	}
}

func BulkNullStringValue() Value {
	return Value{
		Type:  BulkString,
		IsNil: true,
	}
}

func ArrayValue(values ...Value) Value {
	return Value{
		Type:   Array,
		Values: values,
	}
}

func FlatArrayValue(values ...Value) Value {
	return Value{
		Type:    Array,
		Values:  values,
		Flatten: true,
	}
}

func NullValue() Value {
	return nullValue
}

func ErrorValue(s string) Value {
	return Value{
		Type: SimpleError,
		Raw:  []byte(s),
	}
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
	switch v.Type {
	case Array:
		return fmt.Sprintf("%v", v.Values)
	default:
		return string(v.Raw)
	}
}

func (v *Value) AsInt() (int, error) {
	if v.Type != Integer || v.IsNil {
		return 0, errors.New("value not an integer or is nil")
	}
	return strconv.Atoi(string(v.Raw))
}

func (v *Value) AsString() (string, error) {
	switch v.Type {
	case SimpleString, SimpleError, Integer, BulkString:
		return string(v.Raw), nil
	default:
		return "", errors.New("value not a string")
	}
}

func (v *Value) AsBool() (bool, error) {
	if v.Type != Boolean || v.IsNil {
		return false, errors.New("value not a boolean or is nil")
	}

	return strings.EqualFold(string(v.Raw), TrueValue), nil
}

func (v *Value) AsArray() ([]Value, error) {
	if v.Type != Array || v.IsNil {
		return nil, errors.New("value not an array or is nil")
	}
	return v.Values, nil
}

func (v *Value) Marshal() ([]byte, error) {
	switch v.Type {
	case SimpleString, SimpleError, Integer, Null:
		return v.format(), nil
	case BulkString:
		if v.IsNil {
			return []byte("$-1\r\n"), nil
		}
		return v.format(), nil
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

func (v *Value) format() []byte {
	var b bytes.Buffer
	b.WriteString(string(v.Type))

	if v.Type == BulkString && v.IsNil {
		b.WriteString("$-1\r\n")
		return b.Bytes()
	}

	if v.Type == BulkString {
		b.WriteString(fmt.Sprintf("%d\r\n", len(v.Raw)))
	}

	b.Write(v.Raw)

	if !v.BulkLike {
		b.WriteString("\r\n")
	}

	return b.Bytes()
}
