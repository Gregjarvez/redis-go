package store

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"os"
	"sync"
	"time"
)

type Memory struct {
	mu    *sync.RWMutex
	Store map[string]*Record
}

func NewMemory() *Memory {
	return &Memory{
		mu:    &sync.RWMutex{},
		Store: make(map[string]*Record),
	}
}

func (m *Memory) Read(key string) *Record {
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
	m.Store[key] = NewRecord(value, ttl)

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

func (m *Memory) Hydrate() {
	dumpFile := fmt.Sprintf("%s/%s", *config.Config.Dir, *config.Config.DbFilename)
	if dumpFile == "" {
		fmt.Println("No dumpFile file provided")
		return
	}

	if _, err := os.Stat(dumpFile); os.IsNotExist(err) {
		fmt.Println("No dumpFile file found")
		return
	}

	fmt.Println("Hydrating memory store from dumpFile file")
	f, err := os.Open(dumpFile)

	if err != nil {
		fmt.Println("Error opening dumpFile file")
		return
	}

	defer f.Close()

	parser := rdb.NewParser(f)
	err = parser.Parse()

	if err != nil {
		fmt.Println("Error parsing dumpFile file")
	}
	if len(parser.Context.Databases) == 0 {
		fmt.Println("No databases found in dumpFile file")
		return
	}

	for _, record := range parser.Context.Databases[0].Entries {
		ttl := record.Expiry.Value

		if record.Expiry.Type == rdb.EXPIRETIME_SECONDS {
			ttl = time.Unix(record.Expiry.Value, 0).UnixMilli()
		}

		m.Store[record.Key] = &Record{
			Value: record.Value,
			TTL:   ttl,
		}
	}
}
