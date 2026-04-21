/*
File: memtable.go
Description: Implements a concurrent, lock-protected SkipList to serve as the
in-memory buffer (MemTable) for the LSM tree. It maintains keys in strictly
sorted order to allow for sequential flushing to disk. Deletions are handled
by inserting tombstone records (nil values).
*/

package storage

import (
	"bytes"
	"math/rand"
	"sync"
)

const maxLevel = 16
const p = 0.5

type node struct {
	key   []byte
	value []byte
	next  []*node
}

// MemTable is an in-memory storage layer using a SkipList.
// It is thread-safe and supports Put, Get, and Delete operations.
type MemTable struct {
	mu    sync.RWMutex
	head  *node
	level int
	size  int // Total bytes in MemTable (keys + values)
}

// NewMemTable creates a new MemTable.
func NewMemTable() *MemTable {
	return &MemTable{
		head:  &node{next: make([]*node, maxLevel)},
		level: 1,
	}
}

// randomLevel generates a random level for a new node.
func randomLevel() int {
	lvl := 1
	for rand.Float64() < p && lvl < maxLevel {
		lvl++
	}
	return lvl
}

// Put inserts or updates a key-value pair in the MemTable.
func (m *MemTable) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	update := make([]*node, maxLevel)
	curr := m.head

	for i := m.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	curr = curr.next[0]

	if curr != nil && bytes.Equal(curr.key, key) {
		// Update existing node
		m.size -= len(curr.value)
		curr.value = value
		m.size += len(value)
		return
	}

	// Insert new node
	lvl := randomLevel()
	if lvl > m.level {
		for i := m.level; i < lvl; i++ {
			update[i] = m.head
		}
		m.level = lvl
	}

	newNode := &node{
		key:   key,
		value: value,
		next:  make([]*node, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	m.size += len(key) + len(value)
}

// Get retrieves the value for a given key.
// Returns (value, true) if found, (nil, true) if found tombstone, (nil, false) if not found.
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	curr := m.head
	for i := m.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]

	if curr != nil && bytes.Equal(curr.key, key) {
		return curr.value, true
	}

	return nil, false
}

// Delete marks a key as deleted by inserting a tombstone (nil value).
func (m *MemTable) Delete(key []byte) {
	// In LSM-Trees, a delete is just a put with a special value (tombstone).
	m.Put(key, nil)
}

// Size returns the total number of bytes stored in the MemTable.
func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Entry represents a key-value pair.
type Entry struct {
	Key   []byte
	Value []byte
}

// Entries returns all key-value pairs in the MemTable in sorted order.
func (m *MemTable) Entries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]Entry, 0)
	curr := m.head.next[0]
	for curr != nil {
		entries = append(entries, Entry{
			Key:   curr.key,
			Value: curr.value,
		})
		curr = curr.next[0]
	}
	return entries
}
