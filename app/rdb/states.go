package rdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"strconv"
)

type ParserState interface {
	parse(r *bufio.Reader, p *Parser) (*ParserState, error)
}

type property struct {
	Key, Value string
}

type Header struct {
	Magic   string
	Version int
}

type AuxiliaryFieldKey string

const (
	EOF                = 0xFF // End of the RDB file
	SELECTDB           = 0xFE // DB number
	EXPIRETIME_SECONDS = 0xFD // Millisecond expire time
	EXPIRETIME_MS      = 0xFC // Second expire time
	RESIZEDB           = 0xFB // Resize DB
	AUX                = 0xFA // Aux field
)

const (
	RedisVersion AuxiliaryFieldKey = "redis-ver"  // Redis version
	RedisBits    AuxiliaryFieldKey = "redis-bits" // System architecture (32/64 bits)
	CreationTime AuxiliaryFieldKey = "ctime"      // Creation time of the RDB
	UsedMemory   AuxiliaryFieldKey = "used-mem"   // Used memory
)

type Auxiliary struct {
	Fields map[AuxiliaryFieldKey]string
}

type expiry struct {
	Type  byte
	Value int64
}

type databaseEntry struct {
	Key    string
	Value  string
	Type   string
	Expiry expiry
}

type Database struct {
	ID                  int
	HashTableSize       int
	ExpiryHashTableSize int
	Entries             []databaseEntry
}

var InvalidFile = errors.New("invalid RDB file")

func (h *Header) parse(reader *bufio.Reader, parser *Parser) (*ParserState, error) {
	magic := make([]byte, 5)
	version := make([]byte, 4)

	if _, err := reader.Read(magic); err != nil || string(magic) != "REDIS" {
		panic(InvalidFile)
	}

	if _, err := reader.Read(version); err != nil {
		panic(InvalidFile)
	}

	v, err := strconv.Atoi(string(version))

	if err != nil {
		panic(InvalidFile)
	}

	parser.Context.Header = Header{
		Magic:   string(magic),
		Version: v,
	}

	var nextState ParserState = &Auxiliary{
		Fields: make(map[AuxiliaryFieldKey]string),
	}

	return &nextState, nil
}

func (a *Auxiliary) parse(reader *bufio.Reader, parser *Parser) (*ParserState, error) {
	for {
		if typ, err := reader.Peek(1); typ[0] != AUX {
			if typ[0] == SELECTDB {
				var nextState ParserState = &Database{}
				return &nextState, nil
			}

			return nil, err
		}
		_, err := reader.Discard(1)
		if err != nil {
			return nil, err
		}

		keyVal, err := parser.readKeyValuePair(reader)

		if err != nil {
			return nil, err
		}

		parser.Context.Aux.addField(keyVal.Key, keyVal.Value)

		var nextState ParserState = &parser.Context.Aux

		return &nextState, nil
	}
}

func (db *Database) parse(reader *bufio.Reader, parser *Parser) (*ParserState, error) {
	c, err := reader.Peek(1)
	if err != nil || c[0] != SELECTDB || c[0] == EOF {
		return nil, nil // @todo return next state
	}

	var id int
	var dbHashTableSize int
	var dbExpiryHashTableSize int

	_, err = reader.Discard(1) // discard SELECTDB

	if err != nil {
		return nil, err
	}

	id, err = parser.readLength(reader)

	if err != nil {
		return nil, err
	}

	rsdb, _ := reader.Peek(1)

	if rsdb[0] == RESIZEDB {
		reader.Discard(1)
		dbHashTableSize, _ = parser.readLength(reader)
		dbExpiryHashTableSize, _ = parser.readLength(reader)
	}

	database := Database{
		ID:                  id,
		HashTableSize:       dbHashTableSize,
		ExpiryHashTableSize: dbExpiryHashTableSize,
		Entries:             make([]databaseEntry, 0),
	}

	for {
		var expiryType byte
		var expiryValue int64

		if hasExpiry(reader) {
			expiryType, expiryValue, err = db.readExpiry(reader)

			if err != nil {
				return nil, err
			}
		}

		valueType, err := reader.ReadByte()

		if err != nil {
			return nil, err
		}

		kv, err := parser.readKeyValuePair(reader)

		if err != nil {
			return nil, err
		}

		database.Entries = append(database.Entries, databaseEntry{
			Key:   kv.Key,
			Value: kv.Value,
			Type:  string(valueType),
			Expiry: expiry{
				Type:  expiryType,
				Value: expiryValue,
			},
		})

		eod, _ := reader.Peek(1)

		if eod[0] == EOF {
			parser.Context.Databases[database.ID] = database
			break
		}

		if eod[0] == SELECTDB {
			parser.Context.Databases[database.ID] = database
			var nextState ParserState = &database
			return &nextState, nil
		}
	}

	return nil, nil
}

func (db *Database) readExpiry(reader *bufio.Reader) (byte, int64, error) {
	expiryType, _ := reader.ReadByte()
	var v int64
	var err error

	switch expiryType {
	case EXPIRETIME_MS:
		err = binary.Read(reader, binary.LittleEndian, &v)
	case EXPIRETIME_SECONDS:
		var v32 int32
		err = binary.Read(reader, binary.LittleEndian, &v32)
		v = int64(v32)
	}

	return expiryType, v, err
}

func hasExpiry(reader *bufio.Reader) bool {
	typ, _ := reader.Peek(1)
	if len(typ) == 0 {
		return false
	}

	return typ[0] == EXPIRETIME_MS || typ[0] == EXPIRETIME_SECONDS
}

func (a *Auxiliary) addField(key, value string) {
	if a.Fields == nil {
		a.Fields = make(map[AuxiliaryFieldKey]string) // Initialize the map
	}
	a.Fields[AuxiliaryFieldKey(key)] = value
}
