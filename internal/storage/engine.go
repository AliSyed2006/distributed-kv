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

// StorageEngine coordinates the WAL and MemTable to provide a unified API.
// It ensures every write is persisted to the WAL before being applied to the MemTable.
type StorageEngine struct {
	mu         sync.RWMutex
	memTable   *MemTable
	wal        *WAL
	dir        string
	maxMemSize int
	sstables   []*SSTableReader
	nextSSTId  int
	stopChan   chan struct{}
}

// EngineOptions contains configuration for the StorageEngine.
type EngineOptions struct {
	Dir        string
	MaxMemSize int
}

// NewStorageEngine creates a new StorageEngine with the given options.
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

	// Recover MemTable from WAL
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

	// Scan for existing SSTables
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

	// Sort SSTables alphabetically (00001.sst, 00002.sst...)
	sort.Strings(sstFiles)

	var readers []*SSTableReader
	maxId := 0
	// Open in reverse order (newest first)
	for i := len(sstFiles) - 1; i >= 0; i-- {
		path := filepath.Join(opts.Dir, sstFiles[i])
		reader, err := NewSSTableReader(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open SSTable %s: %w", path, err)
		}
		readers = append(readers, reader)

		// Parse ID from name (assuming format 0000X.sst)
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

	// Start background compaction worker
	go engine.runBackgroundCompaction()

	return engine, nil
}

// Put writes a key-value pair to the engine.
func (e *StorageEngine) Put(key, value []byte) error {
	// 1. Get current WAL pointer safely using a read lock
	e.mu.RLock()
	wal := e.wal
	e.mu.RUnlock()

	// 2. Write to WAL before acquiring the write lock.
	// This allows multiple goroutines to queue their writes concurrently,
	// unlocking the throughput of the Group Commit logic.
	if err := wal.Append(OpPut, key, value); err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 3. Apply to MemTable
	e.memTable.Put(key, value)

	// 4. Check if MemTable exceeds max size
	if e.memTable.Size() >= e.maxMemSize {
		if err := e.flushToSSTable(); err != nil {
			return fmt.Errorf("flush failed: %w", err)
		}
	}

	return nil
}

// Get retrieves the value for a given key.
// It checks the MemTable first, then searches through SSTables from newest to oldest.
func (e *StorageEngine) Get(key []byte) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 1. Check MemTable
	if val, ok := e.memTable.Get(key); ok {
		if val == nil {
			return nil, false // Tombstone found in MemTable
		}
		return val, true
	}

	// 2. Check SSTables (newest first)
	for _, sst := range e.sstables {
		val, ok, err := sst.Get(key)
		if err != nil {
			// In a real system we might log this, for now we just skip.
			continue
		}
		if ok {
			if val == nil {
				return nil, false // Tombstone found in SSTable
			}
			return val, true
		}
	}

	return nil, false
}

// Delete removes a key from the engine.
func (e *StorageEngine) Delete(key []byte) error {
	// 1. Get current WAL pointer safely using a read lock
	e.mu.RLock()
	wal := e.wal
	e.mu.RUnlock()

	// 2. Write to WAL first
	if err := wal.Append(OpDelete, key, nil); err != nil {
		return fmt.Errorf("wal delete failed: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 3. Apply to MemTable
	e.memTable.Delete(key)

	// 4. Check size
	if e.memTable.Size() >= e.maxMemSize {
		if err := e.flushToSSTable(); err != nil {
			return fmt.Errorf("flush failed: %w", err)
		}
	}

	return nil
}

// flushToSSTable 'freezes' the current MemTable and starts a new one.
func (e *StorageEngine) flushToSSTable() error {
	// 1. Write MemTable to a new SSTable
	sstName := fmt.Sprintf("%05d.sst", e.nextSSTId)
	sstPath := filepath.Join(e.dir, sstName)
	writer, err := NewSSTableWriter(sstPath)
	if err != nil {
		return err
	}
	
	entries := e.memTable.Entries()
	if len(entries) > 0 {
		if err := writer.Write(entries); err != nil {
			return err
		}

		// 2. Open the new SSTable for reading and add to the list (newest first)
		reader, err := NewSSTableReader(sstPath)
		if err != nil {
			return err
		}
		e.sstables = append([]*SSTableReader{reader}, e.sstables...)
		e.nextSSTId++
	}

	// 3. Close current WAL
	if err := e.wal.Close(); err != nil {
		return err
	}

	// 4. Rotate WAL (for now, we'll just move it to a .old file and create a fresh one)
	walPath := filepath.Join(e.dir, "wal.log")
	// Note: In a real LSM, we'd delete the WAL after flush, but let's follow the .old prompt
	oldPath := filepath.Join(e.dir, "wal.log.old")
	_ = os.Remove(oldPath) // Ignore error if not exists
	if err := os.Rename(walPath, oldPath); err != nil {
		return err
	}

	e.wal, err = NewWAL(walPath)
	if err != nil {
		return err
	}

	// 5. Clear the active MemTable
	e.memTable = NewMemTable()

	return nil
}

// TriggerCompaction merges all current SSTables into one.
func (e *StorageEngine) TriggerCompaction() error {
	e.mu.Lock()
	// Capture the current readers to compact
	readersToCompact := make([]*SSTableReader, len(e.sstables))
	copy(readersToCompact, e.sstables)
	if len(readersToCompact) <= 1 {
		e.mu.Unlock()
		return nil // Nothing to compact
	}
	e.mu.Unlock()

	compactor := NewCompactor(e.dir)
	sstName := fmt.Sprintf("%05d.sst", e.nextSSTId)
	sstPath := filepath.Join(e.dir, sstName)

	if err := compactor.Compact(readersToCompact, sstPath); err != nil {
		return err
	}

	// Atomic Swap
	e.mu.Lock()
	defer e.mu.Unlock()

	newReader, err := NewSSTableReader(sstPath)
	if err != nil {
		return err
	}

	// Replace the compacted readers with the new one
	// We assume we compacted ALL current ones for this simple implementation
	// In a real one we'd only replace the specific ones.
	oldSSTs := e.sstables
	e.sstables = []*SSTableReader{newReader}
	e.nextSSTId++

	// Close and delete old files
	for _, r := range oldSSTs {
		path := r.file.Name()
		r.Close()
		os.Remove(path)
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
					// In a real system, we'd log this error.
					fmt.Printf("Background compaction failed: %v\n", err)
				}
			}
		case <-e.stopChan:
			return
		}
	}
}

// Close gracefully shuts down the engine.
func (e *StorageEngine) Close() error {
	close(e.stopChan)
	
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

// EngineStats contains runtime statistics for the StorageEngine.
type EngineStats struct {
	MemTableSize int
	SSTableCount int
	MaxMemSize   int
}

// Stats returns the current statistics of the engine.
func (e *StorageEngine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return EngineStats{
		MemTableSize: e.memTable.Size(),
		SSTableCount: len(e.sstables),
		MaxMemSize:   e.maxMemSize,
	}
}
