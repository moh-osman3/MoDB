package db

import (
	"fmt"
	"sync"
)

type table struct {
	cols map[string]*column
	numCols int64
	numRows int64
	lock sync.Mutex
}

func NewTable() *table {
	return &table{
		cols: make(map[string]*column),
		numCols: 0,
		numRows: 0,
	}
}

func (tbl *table) CreateColumn(colName string) (*column, error) {
	tbl.lock.Lock()
	defer tbl.lock.Unlock()
	if _, ok := tbl.cols[colName]; ok {
		return nil, fmt.Errorf("Can't create column with name %s: Column already exists", colName)
	}

	col := NewColumn(colName)
	tbl.cols[colName] = col
	tbl.numCols += 1
	return col, nil
}
