package db

import (
	"fmt"
	"sync"

	"go.uber.org/multierr"
)

type table struct {
	cols    map[string]*column
	numCols int64
	numRows int64
	deletes map[int64]bool
	lock    sync.Mutex
}

func NewTable() *table {
	return &table{
		cols:    make(map[string]*column),
		numCols: 0,
		numRows: 0,
		deletes: make(map[int64]bool),
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

func (tbl *table) InsertRow(colNames []string, vals []int64) error {
	tbl.lock.Lock()
	defer tbl.lock.Unlock()

	if len(colNames) != len(vals) {
		return fmt.Errorf("InsertRow: validation failed number of column names does not match number of values: %d != %d", len(colNames), len(vals))
	}

	for i, name := range colNames {
		if _, ok := tbl.cols[name]; !ok {
			return fmt.Errorf("InsertRow: column name does not exist in table: %s", name)
		}

		tbl.cols[name].InsertItem(vals[i])
	}
	tbl.numRows += 1

	return nil
}

func (tbl *table) LoadColumns(colNames []string, cols ...[]int64) error {
	tbl.lock.Lock()
	defer tbl.lock.Unlock()

	if len(colNames) != len(cols) {
		return fmt.Errorf("LoadColumns: validation failed: number of column names does not match number of values: %d != %d", len(colNames), len(cols))
	}

	if len(cols) == 0 {
		return nil
	}

	length := len(cols[0])

	// validate incoming columns for length consistency
	for _, col := range cols[1:] {
		if len(col) != length {
			return fmt.Errorf("LoadColumns: cannot insert: inconsistent column lengths")
		}
	}

	if tbl.numRows != int64(0) && tbl.numRows != int64(length) {
		return fmt.Errorf("LoadColumns: cannot insert: inconsistent column lengths with existing columns")
	}

	for i, name := range colNames {
		if _, ok := tbl.cols[name]; !ok {
			_, err := tbl.CreateColumn(name)
			if err != nil {
				return fmt.Errorf("LoadColumns: %v", err)
			}
		}

		err := tbl.cols[name].LoadColumn(cols[i])
		if err != nil {
			return fmt.Errorf("LoadColumns: %v", err)
		}
	}
	tbl.numRows = int64(length)

	return nil
}

func (tbl *table) Get(c *condition, cols []*column) ([][]int64, error) {
	tbl.lock.Lock()
	defer tbl.lock.Unlock()

	var errors error
	var existingCols []*column
	for _, col := range cols {
		if _, ok := tbl.cols[col.name]; ok {
			existingCols = append(existingCols, col)
			continue
		}
		errors = multierr.Append(errors, fmt.Errorf("Could not fetch column %s: column deleted", col.name))
	}

	c.lock.Lock()
	for id := range c.ids {
		if _, ok := tbl.deletes[id]; ok {
			delete(c.ids, id)
			c.numResults -= 1
		}
	}
	c.lock.Unlock()

	return c.Get(existingCols), errors
}

func (tbl *table) Select(col *column, lower int64, upper int64) (*condition, error) {
	tbl.lock.Lock()
	defer tbl.lock.Unlock()

	if _, ok := tbl.cols[col.name]; !ok {
		return nil, fmt.Errorf("Could not select from column %s: column not found", col.name)
	}

	c := NewCondition()
	c.Select(col, lower, upper)
	return c, nil
}

func (tbl *table) DeleteColumnInternal(colName string) error {
	if _, ok := tbl.cols[colName]; !ok {
		return fmt.Errorf("Cannot delete column with name %s: does not exist", colName)
	}

	delete(tbl.cols, colName)
	tbl.numCols -= 1
	return nil
}

func (tbl *table) DeleteColumn(colName string) error {
	tbl.lock.Lock()
	defer tbl.lock.Unlock()

	return tbl.DeleteColumnInternal(colName)
}

func (tbl *table) DeleteColumns() error {
	tbl.lock.Lock()
	defer tbl.lock.Unlock()

	var errors, err error
	for name := range tbl.cols {
		err = tbl.DeleteColumnInternal(name)
		errors = multierr.Append(errors, err)
	}

	return errors
}

func (tbl *table) DeleteRows(ids []int64) error {
	var errors error
	for _, idx := range ids {
		if idx > tbl.numRows {
			errors = multierr.Append(errors, fmt.Errorf("Cannot delete row with id %d: does not exist", idx))
		}

		tbl.deletes[idx] = true
	}
	return errors
}
