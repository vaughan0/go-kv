package kv

// Returns a new, empty Database that holds everything in memory.
func NewMemory() Database {
	return make(memDatabase)
}

func NewMemoryTable() Table {
	return make(memTable)
}

type memDatabase map[string]memTable

func (m memDatabase) Open(table string) (Table, error) {
	t := m[table]
	if t == nil {
		t = make(memTable)
		m[table] = t
	}
	return t, nil
}

func (m memDatabase) Remove(table string) error {
	delete(m, table)
	return nil
}

func (m memDatabase) Close() {}

type memTable map[string][]byte

func (m memTable) Get(key []byte) ([]byte, error) {
	return m[string(key)], nil
}

func (m memTable) Store(key, value []byte) error {
	if value == nil {
		delete(m, string(key))
	} else {
		m[string(key)] = value
	}
	return nil
}

func (m memTable) Close() {}
