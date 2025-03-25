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
	for i := range(defaultColumnSize-10) {
		vals1[i] = int64(i)
		vals2[i] = int64(i*i)
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
}