package store

import (
	"time"
)

type Record struct {
	Value string
	TTL   int64
	Type  string
}

func NewRecord(value string, ttl int64, valueType string) *Record {
	return &Record{
		Value: value,
		TTL:   ttl,
		Type:  valueType,
	}
}

func (r *Record) String() string {
	return r.Value
}

func (r *Record) IsExpired() bool {
	return r.TTL != 0 && time.Now().UnixMilli() > r.TTL
}
