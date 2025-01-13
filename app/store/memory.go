package store

type Memory struct {
	Store map[string]*Record
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

	m.Store[key] = NewRecord(value, ttl)

	return nil
}
