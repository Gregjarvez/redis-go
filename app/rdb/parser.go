package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

type ParserContext struct {
	Header    Header
	Aux       Auxiliary
	Databases map[int]Database
}

type Parser struct {
	reader  io.Reader
	State   ParserState
	Context *ParserContext
}

const (
	REDIS_RDB_6BITLEN  = 0
	REDIS_RDB_14BITLEN = 1
	REDIS_RDB_32BITLEN = 0x80
	REDIS_RDB_ENCVAL   = 3
	REDIS_RDB_64BITLEN = 0x81
)

const (
	REDIS_RDB_ENC_INT8  = 0
	REDIS_RDB_ENC_INT16 = 1
	REDIS_RDB_ENC_INT32 = 2
	REDIS_RDB_ENC_LZF   = 3
)

func NewParser(r io.Reader) *Parser {
	initialState := &Header{}

	return &Parser{
		reader: r,
		Context: &ParserContext{
			Databases: make(map[int]Database),
			Aux:       Auxiliary{},
		},
		State: initialState,
	}
}

func (p *Parser) Parse() (err error) {
	reader := bufio.NewReader(p.reader)

	for {
		if p.State == nil {
			return nil
		}

		nextState, err := p.State.parse(reader, p)

		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if nextState == nil {
			break
		}

		p.State = *nextState
	}

	return nil
}

func (p *Parser) readLengthWithEncoding(reader *bufio.Reader) (length int, isEncoding bool, err error) {
	l, err := reader.ReadByte()
	encodeType := (l & 0xC0) >> 6

	switch encodeType {
	case REDIS_RDB_ENCVAL:
		isEncoding = true
		length = int(l & 0x3F)
	case REDIS_RDB_6BITLEN:
		length = int(l & 0x3F) // mask - 0b00111111
	case REDIS_RDB_14BITLEN:
		additional, _ := reader.ReadByte()
		length = int(uint16(l&0x3F)<<8 | uint16(additional))
	case REDIS_RDB_32BITLEN:
		var d uint32
		_ = binary.Read(reader, binary.BigEndian, &d)
		length = int(d)
	}

	return
}

func (p *Parser) readString(reader *bufio.Reader) (string, error) {
	length, isEncoding, err := p.readLengthWithEncoding(reader)

	if err != nil {
		return "", err
	}

	if isEncoding {
		return decodeInteger(reader, length)
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(reader, buf)

	return string(buf), err
}

func (p *Parser) readInt(reader *bufio.Reader) (int, error) {
	v, err := p.readString(reader)

	if err != nil {
		return 0, err
	}
	return strconv.Atoi(v)
}

func (p *Parser) readInt64(reader *bufio.Reader) (int64, error) {
	v, err := p.readString(reader)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(v, 10, 64)
}

func (p *Parser) readLength(reader *bufio.Reader) (int, error) {
	l, _, err := p.readLengthWithEncoding(reader)
	return l, err
}

func (p *Parser) readKeyValuePair(reader *bufio.Reader) (property, error) {
	key, err := p.readString(reader)

	if err != nil {
		return property{}, err
	}

	value, err := p.readString(reader)

	if err != nil {
		return property{}, err
	}

	return property{key, value}, nil
}

func decodeInteger(reader *bufio.Reader, length int) (string, error) {
	switch length {
	case REDIS_RDB_ENC_INT8:
		b, _ := reader.ReadByte()
		return strconv.Itoa(int(b)), nil
	case REDIS_RDB_ENC_INT16:
		var i uint16
		_ = binary.Read(reader, binary.LittleEndian, &i)
		return strconv.Itoa(int(i)), nil
	case REDIS_RDB_ENC_INT32:
		var i uint32
		_ = binary.Read(reader, binary.LittleEndian, &i)
		return strconv.Itoa(int(i)), nil
	case REDIS_RDB_ENC_LZF:
		return "", fmt.Errorf("LZF encoding not implemented")
	}

	return "", fmt.Errorf("unknown encoding")
}
