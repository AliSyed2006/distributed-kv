package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestStorageEngine_BasicOps(t *testing.T) {
	dir, err := os.MkdirTemp("", "engine_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewStorageEngine(EngineOptions{
		Dir:        dir,
		MaxMemSize: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	key := []byte("hello")
	val := []byte("world")

	if err := engine.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, ok := engine.Get(key)
	if !ok || !bytes.Equal(got, val) {
		t.Errorf("Expected world, got %s (ok=%v)", got, ok)
	}

	if err := engine.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if _, ok := engine.Get(key); ok {
		t.Error("Expected key to be deleted")
	}
}

func TestStorageEngine_Recovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "engine_recovery_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// 1. Write data and close engine
	engine, err := NewStorageEngine(EngineOptions{
		Dir:        dir,
		MaxMemSize: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	
	engine.Put([]byte("k1"), []byte("v1"))
	engine.Put([]byte("k2"), []byte("v2"))
	engine.Close()

	// 2. Reopen engine and check if data is recovered from WAL
	engine2, err := NewStorageEngine(EngineOptions{
		Dir:        dir,
		MaxMemSize: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	if v, ok := engine2.Get([]byte("k1")); !ok || !bytes.Equal(v, []byte("v1")) {
		t.Errorf("Recovery failed for k1: got %s (ok=%v)", v, ok)
	}
	if v, ok := engine2.Get([]byte("k2")); !ok || !bytes.Equal(v, []byte("v2")) {
		t.Errorf("Recovery failed for k2: got %s (ok=%v)", v, ok)
	}
}

func TestStorageEngine_FlushAndGet(t *testing.T) {
	dir, err := os.MkdirTemp("", "engine_flush_get_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Set a small maxMemSize to trigger flush
	engine, err := NewStorageEngine(EngineOptions{
		Dir:        dir,
		MaxMemSize: 100,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. Put data that will be flushed to SSTable
	key1 := []byte("key1")
	val1 := []byte("value1-large-enough-to-trigger-flush-eventually")
	engine.Put(key1, val1)
	
	// Trigger flush by adding more data
	engine.Put([]byte("key2"), []byte("value2-large-enough-to-trigger-flush-eventually"))
	
	// 2. Verify SSTable exists
	files, _ := os.ReadDir(dir)
	sstFound := false
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			sstFound = true
			break
		}
	}
	if !sstFound {
		t.Error("Expected at least one SSTable to be created")
	}

	// 3. Get data from SSTable
	got, ok := engine.Get(key1)
	if !ok || !bytes.Equal(got, val1) {
		t.Errorf("Failed to get data from SSTable: got %s (ok=%v)", got, ok)
	}

	// 4. Delete data (tombstone in MemTable)
	engine.Delete(key1)
	if _, ok := engine.Get(key1); ok {
		t.Error("Expected key1 to be deleted (tombstone in MemTable)")
	}

	// 5. Flush again to move tombstone to SSTable
	engine.Put([]byte("key3"), []byte("value3-large-enough-to-trigger-flush-eventually"))
	
	// 6. Get data (tombstone in SSTable)
	if _, ok := engine.Get(key1); ok {
		t.Error("Expected key1 to be deleted (tombstone in SSTable)")
	}

	engine.Close()

	// 7. Restart and verify tombstones persist
	engine2, err := NewStorageEngine(EngineOptions{
		Dir:        dir,
		MaxMemSize: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	if _, ok := engine2.Get(key1); ok {
		t.Error("Expected key1 to remain deleted after restart")
	}
}
