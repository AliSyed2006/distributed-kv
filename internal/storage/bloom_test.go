package storage

import (
	"testing"
)

func TestBloomFilter(t *testing.T) {
	f := NewBloomFilter(1000, 7)
	
	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}
	
	for _, k := range keys {
		f.Add(k)
	}
	
	for _, k := range keys {
		if !f.MayContain(k) {
			t.Errorf("Expected filter to contain %s", k)
		}
	}
	
	// Test non-existent key
	if f.MayContain([]byte("date")) {
		// Note: This could occasionally fail due to false positives, 
		// but with 1000 bits and 3 keys, it's very unlikely.
		t.Log("False positive for 'date' (probabilistic but unlikely here)")
	}
}

func TestBloomFilter_Serialization(t *testing.T) {
	f := NewBloomFilter(1000, 7)
	f.Add([]byte("test-key"))
	
	data := f.Serialize()
	f2 := NewBloomFilterFromData(data)
	
	if f2.m != f.m || f2.k != f.k {
		t.Errorf("Mismatch after serialization: m=%d, k=%d vs m=%d, k=%d", f2.m, f2.k, f.m, f.k)
	}
	
	if !f2.MayContain([]byte("test-key")) {
		t.Error("Reconstructed filter lost key")
	}
}
