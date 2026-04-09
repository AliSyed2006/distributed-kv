package storage

import (
	"bytes"
	"os"
	"testing"
)

func TestWAL_Recovery_With_MemTable(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "wal_recovery_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	wal, err := NewWAL(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	// 1. Write some operations to the WAL
	ops := []struct {
		op    OpType
		key   string
		value string
	}{
		{OpPut, "k1", "v1"},
		{OpPut, "k2", "v2"},
		{OpDelete, "k1", ""},
		{OpPut, "k3", "v3"},
	}

	for _, o := range ops {
		err := wal.Append(o.op, []byte(o.key), []byte(o.value))
		if err != nil {
			t.Fatal(err)
		}
	}
	wal.Close()

	// 2. Simulate recovery by reading from the WAL and populating a new MemTable
	wal, err = NewWAL(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

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
		t.Fatalf("Recovery failed: %v", err)
	}

	// 3. Verify MemTable state
	// k1 should be a tombstone (val is nil, ok is true)
	if v, ok := memTable.Get([]byte("k1")); !ok || v != nil {
		t.Errorf("k1 should have been a tombstone, got %v (ok=%v)", v, ok)
	}

	// k2 should be "v2"
	if v, ok := memTable.Get([]byte("k2")); !ok || !bytes.Equal(v, []byte("v2")) {
		t.Errorf("Expected k2=v2, got %s (ok=%v)", v, ok)
	}

	// k3 should be "v3"
	if v, ok := memTable.Get([]byte("k3")); !ok || !bytes.Equal(v, []byte("v3")) {
		t.Errorf("Expected k3=v3, got %s (ok=%v)", v, ok)
	}
}

func TestMemTable_Operations(t *testing.T) {
	m := NewMemTable()

	m.Put([]byte("apple"), []byte("fruit"))
	m.Put([]byte("banana"), []byte("yellow fruit"))

	if v, ok := m.Get([]byte("apple")); !ok || string(v) != "fruit" {
		t.Errorf("Expected fruit, got %s", v)
	}

	m.Delete([]byte("apple"))
	if v, ok := m.Get([]byte("apple")); !ok || v != nil {
		t.Errorf("Expected apple to be a tombstone, got %v (ok=%v)", v, ok)
	}

	if v, ok := m.Get([]byte("banana")); !ok || string(v) != "yellow fruit" {
		t.Errorf("Expected yellow fruit, got %s", v)
	}
}
