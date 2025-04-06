package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func setupTable(t *testing.T, valsSlice [][]int64) *table {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)

	tbl1, err := db1.CreateTable("tbl1")
	assert.NoError(t, err)

	nameSlice := make([]string, len(valsSlice))
	for i := range valsSlice {
		colName := fmt.Sprintf("col%d", i+1)
		nameSlice[i] = colName

		_, err = tbl1.CreateColumn(colName)
		assert.NoError(t, err)
	}

	err = tbl1.LoadColumns(nameSlice, valsSlice...)
	assert.NoError(t, err)
	return tbl1
}

func TestCondition(t *testing.T) {
	vals1 := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	vals2 := []int64{1, 4, 9, 16, 25, 36, 49, 64, 81}
	tbl := setupTable(t, [][]int64{vals1, vals2})

	c := NewCondition()

	col1 := tbl.cols["col1"]
	col2 := tbl.cols["col2"]
	t.Run("basic select and get", func(t *testing.T) {
		c.Select(col1, 2, 6)

		expectIds := []int64{1, 2, 3, 4}
		actualIds := []int64{}
		for key := range c.ids {
			actualIds = append(actualIds, key)
		}
		assert.ElementsMatch(t, expectIds, actualIds)
		expectVals := [][]int64{vals2[1:5]}
		actualVals := c.Get([]*column{col2})

		assert.EqualValues(t, expectVals, actualVals)
	})

	c2 := NewCondition()
	t.Run("Or condition", func(t *testing.T) {
		c2.Select(col2, 30, 81)
		expectIds := []int64{5, 6, 7}
		actualIds := []int64{}
		for key := range c2.ids {
			actualIds = append(actualIds, key)
		}
		assert.ElementsMatch(t, expectIds, actualIds)

		c.Or(c2)
		expectIds = []int64{1, 2, 3, 4, 5, 6, 7}
		actualIds = []int64{}
		for key := range c.ids {
			actualIds = append(actualIds, key)
		}
		assert.ElementsMatch(t, expectIds, actualIds)
		expectVals := [][]int64{vals1[1:8], vals2[1:8]}
		actualVals := c.Get([]*column{col1, col2})

		assert.EqualValues(t, expectVals, actualVals)
	})

	t.Run("And condition", func(t *testing.T) {
		c.And(c2)
		expectIds := []int64{5, 6, 7}
		actualIds := []int64{}
		for key := range c2.ids {
			actualIds = append(actualIds, key)
		}
		assert.ElementsMatch(t, expectIds, actualIds)
		expectVals := [][]int64{vals1[5:8], vals2[5:8]}
		actualVals := c.Get([]*column{col1, col2})

		assert.EqualValues(t, expectVals, actualVals)
	})
}
