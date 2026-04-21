/*
File: bloom.go
Description: A custom implementation of a probabilistic Bloom Filter using FNV
hashing. Appended to the footer of every SSTable to allow the engine to bypass
expensive disk reads for keys that definitely do not exist in a given file.
*/

package storage

import (
	"encoding/binary"
	"hash/fnv"
)

// BloomFilter is a probabilistic data structure used to test set membership.
// It can return "false" (definitely not in set) or "true" (might be in set).
type BloomFilter struct {
	bits []byte
	k    int // Number of hash functions
	m    int // Number of bits
}

// NewBloomFilter creates a new BloomFilter with m bits and k hash functions.
func NewBloomFilter(m, k int) *BloomFilter {
	return &BloomFilter{
		bits: make([]byte, (m+7)/8),
		k:    k,
		m:    m,
	}
}

// Add adds a key to the filter.
func (f *BloomFilter) Add(key []byte) {
	h1, h2 := f.hashes(key)
	for i := 0; i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(f.m)
		f.bits[idx/8] |= (1 << (idx % 8))
	}
}

// MayContain returns true if the key might be in the filter, false if it definitely is not.
func (f *BloomFilter) MayContain(key []byte) bool {
	if f.m == 0 {
		return true
	}
	h1, h2 := f.hashes(key)
	for i := 0; i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(f.m)
		if (f.bits[idx/8] & (1 << (idx % 8))) == 0 {
			return false
		}
	}
	return true
}

// hashes returns two 64-bit hash values for the key using FNV.
func (f *BloomFilter) hashes(key []byte) (uint64, uint64) {
	h := fnv.New64a()
	h.Write(key)
	h1 := h.Sum64()

	h.Write([]byte{0}) // Slightly change the key for second hash
	h2 := h.Sum64()

	return h1, h2
}

// Serialize converts the filter to a byte slice.
// Layout: m [4], k [4], bits...
func (f *BloomFilter) Serialize() []byte {
	buf := make([]byte, 8+len(f.bits))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(f.m))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(f.k))
	copy(buf[8:], f.bits)
	return buf
}

// NewBloomFilterFromData reconstructs a BloomFilter from serialized data.
func NewBloomFilterFromData(data []byte) *BloomFilter {
	if len(data) < 8 {
		return nil
	}
	m := int(binary.LittleEndian.Uint32(data[0:4]))
	k := int(binary.LittleEndian.Uint32(data[4:8]))
	return &BloomFilter{
		m:    m,
		k:    k,
		bits: data[8:],
	}
}
