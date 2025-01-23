package store

type Options struct {
	TTL int64
}

type DataStore interface {
	Read(key string) *Record
	Write(key string, value string, params ...Options) error
	Keys() []string
	Dump() []byte
}
