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

// A key-value database exposes multiple key-value tables.
type Database interface {
	// Opens a named table, creating it if it does not already exist.
	Open(table string) (Table, error)
	// Removes a table.
	Remove(table string) error
	// Closes the database.
	Close()
}
