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
col1.LoadColumn(vals []int64)
col2.InsertItem(val int64)
tbl.LoadColumns(colNames []string, colVals ...[]int64)
tbl.InsertRow(colNames []string, rowVals []int64)
```

### Get

```
c1 := NewCondition(logger)
c1.Select(col1, 0, 100)

c2 := NewCondition(logger)
c2.Select(col2, -30, 10)

c3 := NewCondition(logger)
c3.Select(col3, 50, 60)

// c1 && (c2 || c3)
c2.Or(c3)
c1.And(c2)

// Fetches col1 and col2 that match the condition c1
tbl.Get(c1, []*column{col1, col2})
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