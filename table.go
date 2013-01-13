package kv

import (
	"bytes"
	"errors"
)

// Returns a Database that uses simple table name prefixes to support multiple tables within a single table.
func FromTable(t Table) Database {
	return kvDatabase{t}
}

type kvDatabase struct {
	backend Table
}

func (k kvDatabase) encodePrefix(table string) []byte {
	var result bytes.Buffer
	for _, ch := range table {
		if ch == '\\' || ch == '_' {
			result.WriteRune('\\')
		}
		result.WriteRune(ch)
	}
	result.WriteRune('_')
	return result.Bytes()
}

func (k kvDatabase) Open(table string) (Table, error) {
	return &kvTable{
		backend: k.backend,
		prefix:  k.encodePrefix(table),
	}, nil
}

func (k kvDatabase) Remove(table string) error {
	return errors.New("cannot remove tables from a table-backed database")
}

func (k kvDatabase) Close() {
	k.backend.Close()
}

type kvTable struct {
	backend Table
	prefix  []byte
}

func (k *kvTable) prefixKey(key []byte) []byte {
	result := make([]byte, len(k.prefix)+len(key))
	copy(result, k.prefix)
	copy(result[len(k.prefix):], key)
	return result
}

func (k *kvTable) Get(key []byte) ([]byte, error) {
	key = k.prefixKey(key)
	return k.backend.Get(key)
}

func (k *kvTable) Store(key, value []byte) error {
	key = k.prefixKey(key)
	return k.backend.Store(key, value)
}

func (k *kvTable) Close() {
	// Do nothing; no resources to free
}
