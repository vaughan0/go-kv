package gdbm

/*
#cgo LDFLAGS: -lgdbm
#include <gdbm.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"github.com/vaughan0/go-kv"
	"os"
	"path"
	"unsafe"
)

// Opens a gdbm file as a single Table.
func OpenTable(file string) (kv.Table, error) {
	return gdbmOpen(file)
}

// Returns a database that uses a directory to hold multiple GDBM tables.
func OpenDatabase(root string) (kv.Database, error) {
	// Check if root exists
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("root is not a directory: " + root)
	}

	return &gdbmDB{
		root:   root,
		opened: make(map[string]*gdbm),
	}, nil
}

/* Database */

type gdbmDB struct {
	root   string
	opened map[string]*gdbm
}

func (d *gdbmDB) getFile(name string) string {
	return path.Join(d.root, name+".db")
}

func (d *gdbmDB) Open(name string) (_ kv.Table, err error) {
	table := d.opened[name]
	if table == nil {
		if table, err = gdbmOpen(d.getFile(name)); err != nil {
			return
		}
		d.opened[name] = table
		table.closer = func() {
			delete(d.opened, name)
		}
	}
	return table, nil
}

func (d *gdbmDB) Remove(name string) (err error) {
	if table := d.opened[name]; table != nil {
		table.Close()
		delete(d.opened, name)
	}
	// Remove actual database file
	err = os.Remove(d.getFile(name))
	if os.IsNotExist(err) {
		err = nil
	}
	return
}

func (d *gdbmDB) Close() {
	for _, table := range d.opened {
		table.closer = nil
		table.Close()
	}
}

/* Table */

type gdbm struct {
	closer func()
	dbf    C.GDBM_FILE
}

func gdbmOpen(file string) (*gdbm, error) {
	cstr := C.CString(file)
	defer C.free(unsafe.Pointer(cstr))
	dbf := C.gdbm_open(cstr, 0, C.GDBM_WRCREAT, 0644, nil)
	if dbf == nil {
		return nil, err()
	}
	return &gdbm{
		dbf: dbf,
	}, nil
}

func (d *gdbm) Get(keydata []byte) ([]byte, error) {
	key := toDatum(keydata)
	value := C.gdbm_fetch(d.dbf, key)
	if value.dptr == nil {
		return nil, nil
	}
	data := fromDatum(value)
	return data, nil
}

func (d *gdbm) Store(keydata, valuedata []byte) error {
	key := toDatum(keydata)
	if valuedata == nil {
		C.gdbm_delete(d.dbf, key)
		return nil
	}
	value := toDatum(valuedata)
	C.gdbm_store(d.dbf, key, value, C.GDBM_REPLACE)
	return nil
}

func (d *gdbm) Close() {
	C.gdbm_close(d.dbf)
	if d.closer != nil {
		d.closer()
	}
}

func err() error {
	errstr := C.gdbm_strerror(C.gdbm_errno)
	return errors.New(C.GoString(errstr))
}

func toDatum(data []byte) C.datum {
	if len(data) == 0 {
		return C.datum{
			dptr:  nil,
			dsize: 0,
		}
	}
	return C.datum{
		dptr:  (*C.char)(unsafe.Pointer(&data[0])),
		dsize: C.int(len(data)),
	}
}

func fromDatum(datum C.datum) []byte {
	data := C.GoBytes(unsafe.Pointer(datum.dptr), datum.dsize)
	C.free(unsafe.Pointer(datum.dptr))
	return data
}
