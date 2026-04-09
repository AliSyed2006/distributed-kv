package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestSSTable_WriteRead(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "sstable_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// 1. Prepare sorted entries
	entries := []Entry{
		{Key: []byte("apple"), Value: []byte("red fruit")},
		{Key: []byte("banana"), Value: []byte("yellow fruit")},
		{Key: []byte("cherry"), Value: []byte("small red fruit")},
		{Key: []byte("date"), Value: []byte("sweet fruit")},
		{Key: []byte("eggplant"), Value: []byte("purple vegetable")},
	}

	// 2. Write SSTable
	writer, err := NewSSTableWriter(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(entries); err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	// 3. Read SSTable and verify
	reader, err := NewSSTableReader(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	for _, e := range entries {
		val, ok, err := reader.Get(e.Key)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", e.Key, err)
		}
		if !ok {
			t.Errorf("Key %s not found", e.Key)
			continue
		}
		if !bytes.Equal(val, e.Value) {
			t.Errorf("Expected %s for key %s, got %s", e.Value, e.Key, val)
		}
	}

	// 4. Test non-existent key
	_, ok, err := reader.Get([]byte("fig"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("Expected fig to not be found")
	}
}

func TestSSTable_MultipleBlocks(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "sstable_blocks_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Create enough entries to trigger multiple blocks (BlockSize is 4096)
	var entries []Entry
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		entries = append(entries, Entry{Key: key, Value: val})
	}

	writer, err := NewSSTableWriter(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(entries); err != nil {
		t.Fatal(err)
	}

	reader, err := NewSSTableReader(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if len(reader.index) <= 1 {
		t.Errorf("Expected multiple blocks, got index size %d", len(reader.index))
	}

	// Spot check
	keysToCheck := []int{0, 250, 500, 750, 999}
	for _, i := range keysToCheck {
		e := entries[i]
		val, ok, err := reader.Get(e.Key)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || !bytes.Equal(val, e.Value) {
			t.Errorf("Mismatch for %s: got %s (ok=%v)", e.Key, val, ok)
		}
	}
}
