package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type Reader struct {
	rd   *bufio.Reader
	size *int
}

var InvalidDataType = errors.New("invalid Data Type")

func NewReader(input io.Reader) *Reader {
	c := ByteCounter{size: 0}
	return &Reader{
		rd:   bufio.NewReader(io.TeeReader(input, &c)),
		size: &c.size,
	}
}

func (r *Reader) ReadValue() (value Value, n int, err error) {
	rpType, err := r.rd.ReadByte()

	if err != nil {
		return nullValue, *r.size, err
	}

	t := DataType(rpType)

	switch t {
	case SimpleString, SimpleError, Null, Boolean:
		return r.readSimpleValue(t)
	case Integer:
		return r.readInteger()
	case BulkString:
		return r.readBulkString()
	case Array:
		return r.readArrayValue()
	}

	return nullValue, *r.size, InvalidDataType
}

func (r *Reader) readSimpleValue(typ DataType) (Value, int, error) {
	if typ == Null {
		return nullValue, 0, nil
	}

	line, _, err := r.rd.ReadLine()
	size := *r.size

	if err != nil {
		return nullValue, size, err
	}

	return Value{
		typ:   typ,
		Raw:   line,
		IsNil: false,
	}, size, nil

}

func (r *Reader) readInteger() (Value, int, error) {
	line, _, err := r.rd.ReadLine()

	if err != nil {
		return nullValue, 0, errors.New("invalid integer")
	}

	return Value{
		typ: Integer,
		Raw: line,
	}, *r.size, nil
}

func (r *Reader) readBulkString() (Value, int, error) {
	length, err := r.readInt()

	if err != nil {
		return nullValue, 0, errors.New("invalid bulk string")
	}

	if length <= 0 {
		return nullValue, 0, nil
	}

	content, _, err := r.rd.ReadLine()

	if err != nil {
		return nullValue, 0, errors.New("failed to read bulk string")
	}

	return Value{
		typ:   BulkString,
		Raw:   content,
		IsNil: len(content) == 0,
	}, *r.size, nil
}

func (r *Reader) readInt() (x int, err error) {
	line, _, err := r.rd.ReadLine()

	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseInt(string(line), 10, 64)

	if err != nil {
		return 0, err
	}

	return int(i), nil
}

func (r *Reader) readArrayValue() (Value, int, error) {
	length, err := r.readInt()

	if err != nil {
		return nullValue, 0, errors.New("invalid array")
	}

	values := make([]Value, 0, length)

	for i := 0; i < length; i++ {
		value, _, err := r.ReadValue()

		if err != nil {
			fmt.Println(err)
			return nullValue, 0, errors.New(err.Error())
		}

		values = append(values, value)
	}

	return Value{
		typ:    Array,
		Values: values,
	}, *r.size, nil
}