package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"
)

func TestWAL_Append(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	wal, err := NewWAL(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	key := []byte("key1")
	value := []byte("value1")
	err = wal.Append(OpPut, key, value)
	if err != nil {
		t.Fatalf("Failed to append to WAL: %v", err)
	}

	// Verify the contents of the file
	file, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Layout: OpType [1], KeyLen [4], Key, ValueLen [4], Value.
	header := make([]byte, 1+4)
	if _, err := io.ReadFull(file, header); err != nil {
		t.Fatal(err)
	}

	if OpType(header[0]) != OpPut {
		t.Errorf("Expected OpPut, got %v", header[0])
	}

	keyLen := binary.LittleEndian.Uint32(header[1:5])
	if keyLen != uint32(len(key)) {
		t.Errorf("Expected keyLen %d, got %d", len(key), keyLen)
	}

	readKey := make([]byte, keyLen)
	if _, err := io.ReadFull(file, readKey); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readKey, key) {
		t.Errorf("Expected key %s, got %s", key, readKey)
	}

	valLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(file, valLenBuf); err != nil {
		t.Fatal(err)
	}
	valLen := binary.LittleEndian.Uint32(valLenBuf)
	if valLen != uint32(len(value)) {
		t.Errorf("Expected valLen %d, got %d", len(value), valLen)
	}

	readValue := make([]byte, valLen)
	if _, err := io.ReadFull(file, readValue); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readValue, value) {
		t.Errorf("Expected value %s, got %s", value, readValue)
	}
}
