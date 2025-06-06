package db

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type Manager interface {
	Start(ctx context.Context) error
	End() error
	CreateDb(dbName string) (*db, error)
	DeleteDb(dbName string) error
}

type defaultManager struct {
	dbs    map[string]*db
	numDbs int64
	logger *zap.Logger
	lock   sync.Mutex
}

func NewDefaultManager(logger *zap.Logger) *defaultManager {
	return &defaultManager{
		dbs:    make(map[string]*db),
		numDbs: 0,
		logger: logger,
	}
}

func (dbm *defaultManager) Start(_ context.Context) error {
	return nil
}

func (dbm *defaultManager) End() error {
	return nil
}

func (dbm *defaultManager) CreateDb(dbName string) (*db, error) {
	dbm.lock.Lock()
	defer dbm.lock.Unlock()
	if _, ok := dbm.dbs[dbName]; ok {
		return nil, fmt.Errorf("Can't create db with name %s: Db already exists", dbName)
	}

	db := NewDb()
	dbm.dbs[dbName] = db
	dbm.numDbs += 1
	return db, nil
}

func (dbm *defaultManager) DeleteDb(dbName string) error {
	dbm.lock.Lock()
	defer dbm.lock.Unlock()

	if _, ok := dbm.dbs[dbName]; !ok {
		return fmt.Errorf("Cannot delete db with name %s: does not exist", dbName)
	}

	err := dbm.dbs[dbName].DeleteTables()
	if err != nil {
		return fmt.Errorf("Cannot delete db with name %s: %v", dbName, err)
	}

	delete(dbm.dbs, dbName)
	dbm.numDbs -= 1
	return nil
}
