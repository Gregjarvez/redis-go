package store

import (
	"time"
)

type Recordable interface {
	GetType() string
	GetValue() string
	IsExpired() bool
}

type SimpleRecord struct {
	Value string
	TTL   int64
	typ   string
}

func NewRecord(value string, ttl int64, valueType string) *SimpleRecord {
	return &SimpleRecord{
		Value: value,
		TTL:   ttl,
		typ:   valueType,
	}
}

func (r *SimpleRecord) IsExpired() bool {
	return r.TTL != 0 && time.Now().UnixMilli() > r.TTL
}

func (r *SimpleRecord) GetValue() string {
	return r.Value
}

func (r *SimpleRecord) GetType() string {
	return r.typ
}
