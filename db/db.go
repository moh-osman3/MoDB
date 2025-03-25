package db

import (
	"fmt"
	"sync"
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
