package store

import (
	"encoding/hex"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/store/stream"
	"io"
	"sync"
	"time"
)

type Memory struct {
	mu    *sync.RWMutex
	Store map[string]Recordable
}

func NewMemory() *Memory {
	return &Memory{
		mu:    &sync.RWMutex{},
		Store: make(map[string]Recordable),
	}
}

func (m *Memory) Read(key string) Recordable {
	v, ok := m.Store[key]

	if !ok {
		return nil
	}

	if v.IsExpired() {
		delete(m.Store, key)
		return nil
	}

	return v
}

func (m *Memory) Write(key string, value string, params ...Options) error {
	var ttl int64

	if len(params) > 0 {
		ttl = params[0].TTL
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.Store[key] = NewRecord(value, ttl, "string") // other data types not implemented yet, this will always be a string

	return nil
}

func (m *Memory) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.Store))

	for k := range m.Store {
		keys = append(keys, k)
	}

	return keys
}

func (m *Memory) Hydrate(r io.Reader) error {
	parser := rdb.NewParser(r)
	err := parser.Parse()

	if err != nil {
		fmt.Println("Error parsing dumpFile file")
		return err
	}

	if len(parser.Context.Databases) == 0 {
		fmt.Println("No databases found in dumpFile file")
		return nil
	}

	for _, record := range parser.Context.Databases[0].Entries {
		ttl := record.Expiry.Value

		if record.Expiry.Type == rdb.EXPIRETIME_SECONDS {
			ttl = time.Unix(record.Expiry.Value, 0).UnixMilli()
		}

		m.Store[record.Key] = NewRecord(record.Value, ttl, "string")
	}

	return nil
}

func (m *Memory) Dump() []byte {
	// @todo create dump from memory store - currently just a hex encoded empty redis dump file
	file, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	return file
}

func (m *Memory) XAdd(name, id string, e [][]string) {
	entries := make(map[string]interface{})
	for _, v := range e {
		entries[v[0]] = v[1]
	}

	if trie, ok := m.Store[name]; !ok {
		s := stream.NewTrieStream(name)
		s.Add(id, entries)
		m.Store[name] = s
	} else {
		node := trie.(*stream.Stream)
		node.Add(id, entries)
	}
}
