package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCreateColumn(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)

	tbl1, err := db1.CreateTable("tbl1")
	assert.NoError(t, err)
	tbl2, err := db1.CreateTable("tbl2")
	assert.NoError(t, err)

	col1, err := tbl1.CreateColumn("col1")
	assert.NoError(t, err)
	col2, err := tbl1.CreateColumn("col2")
	assert.NoError(t, err)

	col3, err := tbl2.CreateColumn("col1")
	assert.NoError(t, err)
	col4, err := tbl2.CreateColumn("col2")
	assert.NoError(t, err)

	assert.Equal(t, int64(2), tbl1.numCols)
	assert.Equal(t, 2, len(tbl1.cols))
	assert.Equal(t, col1, tbl1.cols["col1"])
	assert.Equal(t, col2, tbl1.cols["col2"])

	assert.Equal(t, int64(2), tbl2.numCols)
	assert.Equal(t, 2, len(tbl2.cols))
	assert.Equal(t, col3, tbl2.cols["col1"])
	assert.Equal(t, col4, tbl2.cols["col2"])
}