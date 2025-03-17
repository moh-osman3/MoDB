# MoDB - a simple relational database in Go

## Phase 1 - Basic Operations

The goal of the initial phase is to support simple database operations. Users will initialize a dbManager object with modb.new() which will create an interface that can manage multiple dbs and their operations.

### Create

```
dbManager := modb.New(args)
dbManager.Start()
dbManager.CreateDb(dbName)
dbManager.CreateTable(DbName, tblName)
dbManager.CreateColumn(dbName, tblName, colName1)
dbManager.CreateColumn(dbName, tblName, colName2)
```

### Insert

```
dbManager.LoadColumn(dbName, tblName, colName, colValSlice)
dbManager.InsertRow(dbName, tblName, rowValSlice)
```

### Get

```
dbManager.Get(dbName, tableName, colNameSlice, idxSlice)
```

### Delete

```
dbManager.DeleteRow(dbName, tableName, idxSlice)
dbManager.DeleteColumn(dbName, tableName, colName)
dbManager.DeleteTable(dbName, tableName)
dbManager.DeleteDb(dbName)
```


### Persistence

```
dbManager.Save()
dbManager.Shutdown()

dbManager.Start()
dbManager.Get(dbName, tableName, colNameSlice, idxSlice)
```