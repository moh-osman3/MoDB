package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestInsertColumn(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)

	tbl1, err := db1.CreateTable("tbl1")
	assert.NoError(t, err)

	col1, err := tbl1.CreateColumn("col1")
	assert.NoError(t, err)
	col2, err := tbl1.CreateColumn("col2")
	assert.NoError(t, err)

	vals1 := make([]int64, defaultColumnSize-10)
	vals2 := make([]int64, defaultColumnSize-10)
	for i := range defaultColumnSize - 10 {
		vals1[i] = int64(i)
		vals2[i] = int64(i * i)
	}

	err = col1.LoadColumn(vals1)
	assert.NoError(t, err)
	err = col2.LoadColumn(vals2)
	assert.NoError(t, err)

	assert.Equal(t, int64(defaultColumnSize-10), col1.numItems)
	assert.Equal(t, defaultColumnSize-10, len(col1.data)-10)
	assert.Equal(t, vals1, col1.data[0:defaultColumnSize-10])

	assert.Equal(t, int64(defaultColumnSize-10), col2.numItems)
	assert.Equal(t, defaultColumnSize-10, len(col2.data)-10)
	assert.Equal(t, vals2, col2.data[0:defaultColumnSize-10])

	// testing resizing works as expected
	vals1 = make([]int64, defaultColumnSize+10)
	vals2 = make([]int64, defaultColumnSize+10)
	for i := range defaultColumnSize + 10 {
		vals1[i] = int64(i)
		vals2[i] = int64(i * i)
	}

	err = col1.LoadColumn(vals1)
	assert.NoError(t, err)
	err = col2.LoadColumn(vals2)
	assert.NoError(t, err)

	assert.Equal(t, int64(defaultColumnSize+10), col1.numItems)
	assert.Equal(t, 2*(defaultColumnSize+10), len(col1.data))
	assert.Equal(t, vals1, col1.data[0:defaultColumnSize+10])

	assert.Equal(t, int64(defaultColumnSize+10), col2.numItems)
	assert.Equal(t, 2*(defaultColumnSize+10), len(col2.data))
	assert.Equal(t, vals2, col2.data[0:defaultColumnSize+10])
}

func TestInsertItem(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)

	tbl1, err := db1.CreateTable("tbl1")
	assert.NoError(t, err)

	col1, err := tbl1.CreateColumn("col1")
	assert.NoError(t, err)

	vals1 := make([]int64, defaultColumnSize)
	for i := range defaultColumnSize {
		vals1[i] = int64(i)
	}

	err = col1.LoadColumn(vals1)
	assert.NoError(t, err)

	// add an overflow item
	col1.InsertItem(int64(defaultColumnSize + 1))
	vals1 = append(vals1, defaultColumnSize+1)

	assert.Equal(t, int64(defaultColumnSize+1), col1.numItems)
	assert.Equal(t, 2*(defaultColumnSize), len(col1.data))
	assert.Equal(t, vals1, col1.data[0:defaultColumnSize+1])
}
