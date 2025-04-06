package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCreateDb(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	db1, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)
	db2, err := manager.CreateDb("testdb2")
	assert.NoError(t, err)

	assert.Equal(t, int64(2), manager.numDbs)
	assert.Equal(t, 2, len(manager.dbs))

	assert.Equal(t, db1, manager.dbs["testdb1"])
	assert.Equal(t, db2, manager.dbs["testdb2"])
}

func TestDeleteDb(t *testing.T) {
	manager := NewDefaultManager(zap.NewNop())

	_, err := manager.CreateDb("testdb1")
	assert.NoError(t, err)
	db2, err := manager.CreateDb("testdb2")
	assert.NoError(t, err)

	err = manager.DeleteDb("testdb1")
	assert.NoError(t, err)

	err = manager.DeleteDb("testdb1")
	assert.ErrorContains(t, err, "does not exist")

	assert.Equal(t, int64(1), manager.numDbs)
	assert.Equal(t, 1, len(manager.dbs))

	assert.Equal(t, db2, manager.dbs["testdb2"])
}
