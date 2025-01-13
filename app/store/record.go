package store

import "time"

type Record struct {
	Value   string
	Created time.Time
	TTL     int64
}

func NewRecord(value string, ttl int64) *Record {
	return &Record{
		Value:   value,
		Created: time.Now(),
		TTL:     ttl,
	}
}

func (r *Record) String() string {
	return r.Value
}

func (r *Record) IsExpired() bool {
	return r.TTL != 0 && time.Now().After(r.Created.Add(time.Duration(r.TTL)*time.Millisecond))
}
