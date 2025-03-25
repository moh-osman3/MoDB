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

func TestInsertRow(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)

	tbl1, err := db1.CreateTable("tbl1")
	assert.NoError(t, err)

	col1, err := tbl1.CreateColumn("col1")
	assert.NoError(t, err)
	col2, err := tbl1.CreateColumn("col2")
	assert.NoError(t, err)

	err = tbl1.InsertRow([]string{"col1", "col2"},[]int64{1})
	assert.ErrorContains(t, err, "number of column names does not match number of values")

	err = tbl1.InsertRow([]string{"col7", "col2"},[]int64{1,2})
	assert.ErrorContains(t, err, "column name does not exist in table")

	err = tbl1.InsertRow([]string{"col1", "col2"},[]int64{1,2})
	assert.NoError(t, err)
	err = tbl1.InsertRow([]string{"col1", "col2"},[]int64{3,4})
	assert.NoError(t, err)

	expectCol1 := []int64{1,3}
	expectCol2 := []int64{2,4}

	assert.Equal(t, int64(2), tbl1.numRows)
	assert.Equal(t, expectCol1, col1.data[0:2])
	assert.Equal(t, expectCol2, col2.data[0:2])
}

func TestLoadColumns(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)

	tbl1, err := db1.CreateTable("tbl1")
	assert.NoError(t, err)

	col1, err := tbl1.CreateColumn("col1")
	assert.NoError(t, err)
	col2, err := tbl1.CreateColumn("col2")
	assert.NoError(t, err)

	vals1 := []int64{1,2,3,4,5,6,7,8,9}
	vals2 := []int64{1,4,9,16,25,36,49,64,81}

	err = tbl1.LoadColumns([]string{"col1", "col2"},[][]int64{{1,2}, {3}}...)
	assert.ErrorContains(t, err, "inconsistent column lengths")

	err = tbl1.LoadColumns([]string{"col1", "col2"},[][]int64{{3,4}}...)
	assert.ErrorContains(t, err, "number of column names does not match number of values")

	err = tbl1.LoadColumns([]string{"col1", "col2"}, [][]int64{vals1, vals2}...)
	assert.NoError(t, err)

	assert.Equal(t, vals1, col1.data[0:len(vals1)])
	assert.Equal(t, vals2, col2.data[0:len(vals2)])

	err = tbl1.LoadColumns([]string{"col1", "col2"},[][]int64{{3,4}, {5,6}}...)
	assert.ErrorContains(t, err, "inconsistent column lengths with existing columns")
}
