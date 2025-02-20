package store

import "io"

type Options struct {
	TTL int64
}

type DataStore interface {
	Read(key string) Recordable
	Write(key string, value string, params ...Options) error
	Keys() []string
	Dump() []byte
	Hydrate(r io.Reader) error
	XAdd(name, id string, entries [][]string) (string, error)
}
