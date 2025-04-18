package db

import (
	"sync"
)

const defaultColumnSize = 10000

type column struct {
	name     string
	data     []int64
	numItems int64
	lock     sync.Mutex
}

func NewColumn(colName string) *column {
	return &column{
		name:     colName,
		data:     make([]int64, defaultColumnSize),
		numItems: 0,
	}
}

func (col *column) LoadColumn(vals []int64) error {
	col.lock.Lock()
	defer col.lock.Unlock()

	if len(vals) > len(col.data) {
		col.data = make([]int64, 2*len(vals))
	}
	for i, val := range vals {
		col.data[i] = val
	}

	col.numItems = int64(len(vals))

	return nil
}

func (col *column) InsertItem(item int64) error {
	col.lock.Lock()
	defer col.lock.Unlock()

	if int(col.numItems+1) > len(col.data) {
		col.resizeData(int64(2 * len(col.data)))
	}
	col.data[int(col.numItems)] = item
	col.numItems += 1

	return nil
}

func (col *column) resizeData(newLength int64) {
	newData := make([]int64, newLength)

	for i := 0; i < int(col.numItems); i++ {
		newData[i] = col.data[i]
	}

	col.data = newData
}
