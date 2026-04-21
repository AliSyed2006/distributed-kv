/*
Package storage provides the core Log-Structured Merge (LSM) tree database engine.

File: engine.go
Description: The central orchestrator for the storage engine. It manages the concurrency
model, routing incoming writes through the Write-Ahead Log (WAL) and into the active
MemTable. It handles the lifecycle of immutable MemTables, applies backpressure during
heavy I/O disk flushes, and safely coordinates background SSTable compactions.
*/

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type StorageEngine struct {
	mu         sync.RWMutex
	memTable   *MemTable
	immTable   *MemTable
	wal        *WAL
	dir        string
	maxMemSize int
	sstables   []*SSTableReader
	nextSSTId  int
	stopChan   chan struct{}
}

type EngineOptions struct {
	Dir        string
	MaxMemSize int
}

func NewStorageEngine(opts EngineOptions) (*StorageEngine, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(opts.Dir, "wal.log")
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	memTable := NewMemTable()

	err = wal.Recovery(func(op OpType, key, value []byte) error {
		if op == OpPut {
			memTable.Put(key, value)
		} else if op == OpDelete {
			memTable.Delete(key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	files, err := os.ReadDir(opts.Dir)
	if err != nil {
		return nil, err
	}

	var sstFiles []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".sst") {
			sstFiles = append(sstFiles, f.Name())
		}
	}

	sort.Strings(sstFiles)

	var readers []*SSTableReader
	maxId := 0
	for i := len(sstFiles) - 1; i >= 0; i-- {
		path := filepath.Join(opts.Dir, sstFiles[i])
		reader, err := NewSSTableReader(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open SSTable %s: %w", path, err)
		}
		readers = append(readers, reader)

		var id int
		fmt.Sscanf(sstFiles[i], "%d.sst", &id)
		if id > maxId {
			maxId = id
		}
	}

	engine := &StorageEngine{
		memTable:   memTable,
		wal:        wal,
		dir:        opts.Dir,
		maxMemSize: opts.MaxMemSize,
		sstables:   readers,
		nextSSTId:  maxId + 1,
		stopChan:   make(chan struct{}),
	}

	go engine.runBackgroundCompaction()

	return engine, nil
}

func (e *StorageEngine) Put(key, value []byte) error {
	e.mu.RLock()
	wal := e.wal
	wal.IncWriters()
	e.mu.RUnlock()

	err := wal.Append(OpPut, key, value)
	wal.DecWriters()

	if err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}

	e.mu.Lock()

	for e.immTable != nil && e.memTable.Size() >= e.maxMemSize {
		e.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		e.mu.Lock()
	}

	e.memTable.Put(key, value)

	if e.memTable.Size() >= e.maxMemSize && e.immTable == nil {
		e.triggerBackgroundFlush()
	}

	e.mu.Unlock()
	return nil
}

func (e *StorageEngine) Get(key []byte) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if val, ok := e.memTable.Get(key); ok {
		if val == nil {
			return nil, false
		}
		return val, true
	}

	if e.immTable != nil {
		if val, ok := e.immTable.Get(key); ok {
			if val == nil {
				return nil, false
			}
			return val, true
		}
	}

	for _, sst := range e.sstables {
		val, ok, err := sst.Get(key)
		if err != nil {
			continue
		}
		if ok {
			if val == nil {
				return nil, false
			}
			return val, true
		}
	}

	return nil, false
}

func (e *StorageEngine) Delete(key []byte) error {
	e.mu.RLock()
	wal := e.wal
	wal.IncWriters()
	e.mu.RUnlock()

	err := wal.Append(OpDelete, key, nil)
	wal.DecWriters()

	if err != nil {
		return fmt.Errorf("wal delete failed: %w", err)
	}

	e.mu.Lock()

	for e.immTable != nil && e.memTable.Size() >= e.maxMemSize {
		e.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		e.mu.Lock()
	}

	e.memTable.Delete(key)

	if e.memTable.Size() >= e.maxMemSize && e.immTable == nil {
		e.triggerBackgroundFlush()
	}

	e.mu.Unlock()
	return nil
}

func (e *StorageEngine) triggerBackgroundFlush() {
	e.immTable = e.memTable
	e.memTable = NewMemTable()

	if err := e.wal.Close(); err == nil {
		walPath := filepath.Join(e.dir, "wal.log")
		oldPath := filepath.Join(e.dir, "wal.log.old")
		os.Remove(oldPath)
		os.Rename(walPath, oldPath)
		e.wal, _ = NewWAL(walPath)
	}

	sstId := e.nextSSTId
	e.nextSSTId++

	go e.flushImmutableToDisk(sstId, e.immTable)
}

func (e *StorageEngine) flushImmutableToDisk(sstId int, tableToFlush *MemTable) {
	entries := tableToFlush.Entries()

	sstName := fmt.Sprintf("%05d.sst", sstId)
	sstPath := filepath.Join(e.dir, sstName)

	if writer, err := NewSSTableWriter(sstPath, len(entries)); err == nil {
		if len(entries) > 0 {
			writer.Write(entries)
			if reader, err := NewSSTableReader(sstPath); err == nil {
				e.mu.Lock()
				e.sstables = append([]*SSTableReader{reader}, e.sstables...)
				e.mu.Unlock()
			}
		}
	}

	e.mu.Lock()
	e.immTable = nil
	e.mu.Unlock()
}

func (e *StorageEngine) TriggerCompaction() error {
	e.mu.Lock()
	rc := make([]*SSTableReader, len(e.sstables))
	copy(rc, e.sstables)
	if len(rc) <= 1 {
		e.mu.Unlock()
		return nil
	}

	cid := e.nextSSTId
	e.nextSSTId++
	e.mu.Unlock()

	c := NewCompactor(e.dir)
	n := fmt.Sprintf("%05d.sst", cid)
	p := filepath.Join(e.dir, n)

	if err := c.Compact(rc, p); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	nr, err := NewSSTableReader(p)
	if err != nil {
		return err
	}

	cm := make(map[*SSTableReader]bool)
	for _, r := range rc {
		cm[r] = true
	}

	var ps []*SSTableReader
	for _, r := range e.sstables {
		if !cm[r] {
			ps = append(ps, r)
		}
	}

	e.sstables = append(ps, nr)

	for _, r := range rc {
		fp := r.file.Name()
		r.Close()
		os.Remove(fp)
	}

	return nil
}

func (e *StorageEngine) runBackgroundCompaction() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.mu.RLock()
			count := len(e.sstables)
			e.mu.RUnlock()

			if count > 4 {
				if err := e.TriggerCompaction(); err != nil {
					fmt.Printf("Background compaction failed: %v\n", err)
				}
			}
		case <-e.stopChan:
			return
		}
	}
}

func (e *StorageEngine) Close() error {
	close(e.stopChan)

	for {
		e.mu.RLock()
		isFlushing := e.immTable != nil
		e.mu.RUnlock()
		if !isFlushing {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	var errs []error
	if err := e.wal.Close(); err != nil {
		errs = append(errs, err)
	}
	for _, sst := range e.sstables {
		if err := sst.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	return nil
}

type EngineStats struct {
	MemTableSize int
	SSTableCount int
	MaxMemSize   int
}

func (e *StorageEngine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return EngineStats{
		MemTableSize: e.memTable.Size(),
		SSTableCount: len(e.sstables),
		MaxMemSize:   e.maxMemSize,
	}
}
