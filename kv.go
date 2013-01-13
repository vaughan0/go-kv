// Package kv provides interfaces for working with key-value databases.
package kv

// A key-value table.
type Table interface {
	// Retrieves a value associated with a key. value will be nil if the key was not found.
	Get(key []byte) (value []byte, err error)
	// Stores a value, possibly overwriting any previous value associated with the key.
	// If value is nil, the key will be removed.
	Store(key, value []byte) error
	// Closes the table.
	Close()
}

// Calls t.Get with the byte slice version of key. result will be the empty string if the key was not present.
func StrGet(t Table, key string) (result string, err error) {
	value, err := t.Get([]byte(key))
	if value != nil {
		result = string(value)
	}
	return
}

// Calls t.Store with the byte slice version of key and value, unless value is the empty string, in
// which case it will be passed as nil (for deletion).
func StrStore(t Table, key, value string) error {
	var byteValue []byte
	if value != "" {
		byteValue = []byte(value)
	}
	return t.Store([]byte(key), byteValue)
}

// A key-value database exposes multiple key-value tables.
type Database interface {
	// Opens a named table, creating it if it does not already exist.
	Open(table string) (Table, error)
	// Removes a table.
	Remove(table string) error
	// Closes the database.
	Close()
}
