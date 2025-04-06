package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCreateTable(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)
	db2, err := manager.CreateDb("testdb2")
	assert.NoError(t, err)

	tbl1, err := db1.CreateTable("tbl1")
	assert.NoError(t, err)
	tbl2, err := db1.CreateTable("tbl2")
	assert.NoError(t, err)

	tbl3, err := db2.CreateTable("tbl1")
	assert.NoError(t, err)
	tbl4, err := db2.CreateTable("tbl2")
	assert.NoError(t, err)

	assert.Equal(t, int64(2), db1.numTables)
	assert.Equal(t, 2, len(db1.tables))
	assert.Equal(t, tbl1, db1.tables["tbl1"])
	assert.Equal(t, tbl2, db1.tables["tbl2"])

	assert.Equal(t, int64(2), db2.numTables)
	assert.Equal(t, 2, len(db2.tables))
	assert.Equal(t, tbl3, db2.tables["tbl1"])
	assert.Equal(t, tbl4, db2.tables["tbl2"])
}

func TestDeleteTables(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)

	_, err = db1.CreateTable("tbl1")
	assert.NoError(t, err)
	_, err = db1.CreateTable("tbl2")
	assert.NoError(t, err)

	err = db1.DeleteTables()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(db1.tables))
}

func TestDeleteTable(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)

	_, err = db1.CreateTable("tbl1")
	assert.NoError(t, err)
	tbl2, err := db1.CreateTable("tbl2")
	assert.NoError(t, err)

	err = db1.DeleteTable("tbl1")
	assert.NoError(t, err)

	err = db1.DeleteTable("tbl1")
	assert.ErrorContains(t, err, "does not exist")

	assert.Equal(t, int64(1), db1.numTables)
	assert.Equal(t, 1, len(db1.tables))
	assert.Equal(t, tbl2, db1.tables["tbl2"])
}
