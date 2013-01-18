package kv

import (
	"database/sql"
	"errors"
)

var errSqlNotFound = errors.New("sql row not found")

type SQLDatabase struct {
	db            *sql.DB
	table         string
	getStmt       *sql.Stmt
	updateStmt    *sql.Stmt
	insertStmt    *sql.Stmt
	removeStmt    *sql.Stmt
	removeAllStmt *sql.Stmt
}

func NewSQL(db *sql.DB, table string) (s *SQLDatabase, err error) {
	return &SQLDatabase{db: db, table: table}, nil
}

func (s *SQLDatabase) check() (err error) {
	db := s.db
	table := s.table
	if s.getStmt == nil {
		if s.getStmt, err = db.Prepare("SELECT id, value FROM " + table + " WHERE `group` = ? AND key = ? LIMIT 1"); err != nil {
			goto Error
		}
		if s.updateStmt, err = db.Prepare("UPDATE " + table + " SET value = ? WHERE id = ?"); err != nil {
			goto Error
		}
		if s.insertStmt, err = db.Prepare("INSERT INTO " + table + " (`group`, key, value) VALUES (?, ?, ?)"); err != nil {
			goto Error
		}
		if s.removeStmt, err = db.Prepare("DELETE FROM " + table + " WHERE `group` = ? AND key = ?"); err != nil {
			goto Error
		}
		if s.removeAllStmt, err = db.Prepare("DELETE FROM " + table + " WHERE `group` = ?"); err != nil {
			goto Error
		}
	}
	return nil
Error:
	for _, ptr := range []**sql.Stmt{&s.getStmt, &s.updateStmt, &s.insertStmt, &s.removeStmt, &s.removeAllStmt} {
		if *ptr != nil {
			(*ptr).Close()
			*ptr = nil
		}
	}
	return
}

// Creates a new SQL table to hold the key-value data if one does not exist already.
// If autoinc is true, then the id field will use the AUTOINCREMENT keyword (you don't need this for sqlite).
// tabletype is the data type for the (kv) table name column.
// keytype is the data type for the key column.
func (s *SQLDatabase) CreateTable(autoinc bool, tabletype, keytype string) (err error) {

	query := "CREATE TABLE IF NOT EXISTS " + s.table + " ("
	query += "id INTEGER PRIMARY KEY"
	if autoinc {
		query += " AUTOINCREMENT"
	}
	query += ", `group` " + tabletype
	query += ", key " + keytype
	query += ", value BLOB)"

	if _, err = s.db.Exec(query); err != nil {
		return
	}

	if _, err = s.db.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS group_key ON ` + s.table + " (`group`, key)",
	); err != nil {
		return
	}
	return nil
}

func (s *SQLDatabase) Open(table string) (result Table, err error) {
	if err = s.check(); err != nil {
		return
	}
	return &sqlTable{
		db:    s,
		table: table,
	}, nil
}

func (s *SQLDatabase) Remove(table string) (err error) {
	if err = s.check(); err != nil {
		return
	}
	_, err = s.removeAllStmt.Exec(table)
	return
}

func (s *SQLDatabase) get(table, key string) (id int, value []byte, err error) {
	rows, err := s.getStmt.Query(table, key)
	if err != nil {
		return
	}
	defer rows.Close()
	if !rows.Next() {
		// No results, could be an error
		err = rows.Err()
		if err == nil {
			err = errSqlNotFound
		}
		return
	}
	err = rows.Scan(&id, &value)
	return
}

func (s *SQLDatabase) store(table, key string, value []byte) (err error) {
	// First check if the key exists
	id, _, err := s.get(table, key)
	if err != nil {
		if err != errSqlNotFound {
			return
		}
		err = nil
		// INSERT new row
		_, err = s.insertStmt.Exec(table, key, value)
		return
	}
	// Update existing key
	_, err = s.updateStmt.Exec(value, id)
	return
}

func (s *SQLDatabase) Close() {
	s.getStmt.Close()
	s.updateStmt.Close()
	s.insertStmt.Close()
	s.removeStmt.Close()
	s.removeAllStmt.Close()
}

type sqlTable struct {
	db    *SQLDatabase
	table string
}

func (s *sqlTable) Get(key []byte) (value []byte, err error) {
	_, value, err = s.db.get(s.table, string(key))
	if err != nil {
		if err == errSqlNotFound {
			err = nil
		}
	}
	return
}

func (s *sqlTable) Store(key, value []byte) (err error) {
	return s.db.store(s.table, string(key), value)
}

func (s *sqlTable) Close() {}
