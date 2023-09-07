package main

import (
	"sort"
	"sync"
)

type Element struct {
	data      int64
	timestamp int64
}

type Counter struct {
	arr           []Element
	readWriteLock *sync.RWMutex
}

func (c *Counter) Read() []int64 {
	c.readWriteLock.RLock()
	defer c.readWriteLock.RUnlock()

	result := []int64{}

	for _, element := range c.arr {
		result = append(result, element.data)
	}

	return result
}

func (c *Counter) Insert(value int64, currentTimestamp int64) bool {
	c.readWriteLock.Lock()
	defer c.readWriteLock.Unlock()

	i, found := sort.Find(len(c.arr), func(i int) int {
		if currentTimestamp < c.arr[i].timestamp {
			return -1
		} else if currentTimestamp > c.arr[i].timestamp {
			return 1
		} else {
			return 0
		}
	})

	if !found {
		if i == len(c.arr) {
			c.arr = append(c.arr, Element{
				data:      value,
				timestamp: currentTimestamp,
			})
		} else {
			c.arr = append(c.arr[:i+1], c.arr[i:]...)
			c.arr[i] = Element{
				data:      value,
				timestamp: currentTimestamp,
			}
		}
	}

	return !found
}
