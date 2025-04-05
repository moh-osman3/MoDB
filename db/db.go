package db

import (
	"fmt"
	"sync"

	"go.uber.org/multierr"
)

type db struct {
	tables    map[string]*table
	numTables int64
	lock      sync.Mutex
}

func NewDb() *db {
	return &db{
		tables:    make(map[string]*table),
		numTables: 0,
	}
}

func (db *db) CreateTable(tblName string) (*table, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	if _, ok := db.tables[tblName]; ok {
		return nil, fmt.Errorf("Can't create db with name %s: Table already exists", tblName)
	}

	tbl := NewTable()
	db.tables[tblName] = tbl
	db.numTables += 1
	return tbl, nil
}
func (db *db) DeleteTable(tblName string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	return db.DeleteTableInternal(tblName)
}

func (db *db) DeleteTableInternal(tblName string) error {
	if _, ok := db.tables[tblName]; !ok {
		return fmt.Errorf("Cannot delete table with name %s: does not exist", tblName)
	}

	err := db.tables[tblName].DeleteColumns()
	if err != nil {
		return fmt.Errorf("Cannot delete table with name %s: %v", tblName, err)
	}

	delete(db.tables, tblName)
	return nil
}

func (db *db) DeleteTables() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	var err, errors error
	for name, tbl := range db.tables {
		err = tbl.DeleteColumns()
		errors = multierr.Append(errors, err)
		err = db.DeleteTableInternal(name)
		errors = multierr.Append(errors, err)
	}
	return errors
}
