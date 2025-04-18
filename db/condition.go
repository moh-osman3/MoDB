package db

import (
	"sort"
	"sync"
)

type condition struct {
	ids        map[int64]bool
	numResults int
	cols       []string
	lock       sync.RWMutex
}

func NewCondition() *condition {
	return &condition{
		numResults: 0,
		ids:        make(map[int64]bool),
	}
}

// Get will fetch the ids that match the condition in the provided column names.
func (c *condition) Get(cols []*column) [][]int64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.numResults == 0 {
		return [][]int64{}
	}

	sortedIds := make([]int64, len(c.ids))
	i := 0
	for key := range c.ids {
		sortedIds[i] = key
		i += 1
	}
	sort.Slice(sortedIds, func(i, j int) bool { return sortedIds[i] < sortedIds[j] })

	res := make([][]int64, len(cols))
	for k, col := range cols {
		colRes := make([]int64, c.numResults)
		for i := 0; i < c.numResults; i++ {
			resIdx := sortedIds[i]
			colRes[i] = col.data[resIdx]
		}
		res[k] = colRes
	}

	return res
}

func (c *condition) Select(col *column, lower int64, upper int64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	col.lock.Lock()
	defer col.lock.Unlock()

	for i := 0; i < int(col.numItems); i++ {
		item := col.data[i]
		if item >= lower && item < upper {
			c.ids[int64(i)] = true
			c.numResults += 1
		}
	}
}

func (c *condition) Or(newCond *condition) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// avoid deadlock and return immediately if self referential
	if c == newCond {
		return
	}

	newCond.lock.RLock()
	defer newCond.lock.RUnlock()
	for newId := range newCond.ids {
		if _, ok := c.ids[newId]; !ok {
			c.ids[newId] = true
			c.numResults += 1
		}
	}
}

func (c *condition) And(newCond *condition) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// avoid deadlock and return immediately if self referential
	if c == newCond {
		return
	}

	newCond.lock.RLock()
	defer newCond.lock.RUnlock()
	for id := range c.ids {
		if _, ok := newCond.ids[id]; !ok {
			delete(c.ids, id)
			c.numResults -= 1
		}
	}
}
