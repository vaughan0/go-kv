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

func (m memDatabase) List() ([]string, error) {
	names := make([]string, len(m))
	i := 0
	for name, _ := range m {
		names[i] = name
		i++
	}
	return names, nil
}

func (m memDatabase) Close() {}

type memTable map[string][]byte

func clone(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}

func (m memTable) Get(key []byte) (value []byte, err error) {
	if data := m[string(key)]; data != nil {
		value = clone(data)
	}
	return
}

func (m memTable) Store(key, value []byte) error {
	if value == nil {
		delete(m, string(key))
	} else {
		m[string(key)] = clone(value)
	}
	return nil
}

func (m memTable) List() ([][]byte, error) {
	keys := make([][]byte, len(m))
	i := 0
	for key, _ := range m {
		keys[i] = []byte(key)
		i++
	}
	return keys, nil
}

func (m memTable) Close() {}
