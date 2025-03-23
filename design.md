# MoDB - a simple relational database in Go

## Phase 1 - Basic Operations

The goal of the initial phase is to support simple database operations. Users will initialize a dbManager object with modb.new() which will create an interface that can manage multiple dbs and their operations.

### Create

```
dbManager := db.NewManager(args)
dbManager.Start()
database, _ := dbManager.CreateDb(dbName)
tbl, _ := database.CreateTable(tblName)
col1, _ := tbl.CreateColumn(colName1)
col2, _ := tbl.CreateColumn(colName2)
```

### Insert

```
col1.LoadColumn(colValSlice)
tbl.InsertRow(rowValSlice)
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