package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	// BlockSize defines the target size for each data block.
	BlockSize = 4096
	// FooterSize: BloomOffset (8 bytes) + IndexOffset (8 bytes) + MagicNumber (8 bytes).
	FooterSize = 24
	// MagicNumber identifies the file as an SSTable.
	MagicNumber uint64 = 0x53535441424C4531 // "SSTABLE1"
)

// SSTableWriter creates an immutable SSTable from sorted entries.
type SSTableWriter struct {
	file         *os.File
	index        []indexEntry
	currBlockOff uint64
	currBlockLen uint32
	bloom        *BloomFilter
}

type indexEntry struct {
	key    []byte
	offset uint64
}

// NewSSTableWriter creates a new SSTableWriter.
func NewSSTableWriter(path string) (*SSTableWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &SSTableWriter{
		file: file,
	}, nil
}

// Write takes sorted entries and persists them to the SSTable file.
func (sw *SSTableWriter) Write(entries []Entry) error {
	if len(entries) == 0 {
		return fmt.Errorf("no entries to write")
	}

	// Initialize Bloom Filter: ~10 bits per key, 7 hash functions.
	sw.bloom = NewBloomFilter(len(entries)*10, 7)

	for i, entry := range entries {
		// Add key to bloom filter.
		sw.bloom.Add(entry.Key)

		// If it's the start of a new block, record it in the index.
		if sw.currBlockLen == 0 {
			sw.index = append(sw.index, indexEntry{
				key:    entry.Key,
				offset: sw.currBlockOff,
			})
		}

		// Write [KeyLen (4), Key, ValueLen (4), Value]
		if err := sw.writeEntry(entry); err != nil {
			return err
		}

		// Check if we should start a new block.
		// We ensure at least one entry per block, and then check size.
		if sw.currBlockLen >= BlockSize && i < len(entries)-1 {
			sw.currBlockOff += uint64(sw.currBlockLen)
			sw.currBlockLen = 0
		}
	}

	// Finalize file: write Bloom Filter, Index and Footer.
	return sw.finalize()
}

func (sw *SSTableWriter) writeEntry(e Entry) error {
	keyLen := uint32(len(e.Key))
	valLen := uint32(len(e.Value))
	
	// Write KeyLen
	if err := binary.Write(sw.file, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	// Write Key
	if _, err := sw.file.Write(e.Key); err != nil {
		return err
	}
	// Write ValueLen
	if err := binary.Write(sw.file, binary.LittleEndian, valLen); err != nil {
		return err
	}
	// Write Value
	if _, err := sw.file.Write(e.Value); err != nil {
		return err
	}

	sw.currBlockLen += 4 + keyLen + 4 + valLen
	return nil
}

func (sw *SSTableWriter) finalize() error {
	// 1. Write Bloom Filter
	bloomOff, err := sw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	bloomData := sw.bloom.Serialize()
	if err := binary.Write(sw.file, binary.LittleEndian, uint32(len(bloomData))); err != nil {
		return err
	}
	if _, err := sw.file.Write(bloomData); err != nil {
		return err
	}

	// 2. Record index offset
	indexOff, err := sw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// 3. Write Index: [NumEntries (4), {KeyLen (4), Key, Offset (8)}...]
	if err := binary.Write(sw.file, binary.LittleEndian, uint32(len(sw.index))); err != nil {
		return err
	}
	for _, ie := range sw.index {
		if err := binary.Write(sw.file, binary.LittleEndian, uint32(len(ie.key))); err != nil {
			return err
		}
		if _, err := sw.file.Write(ie.key); err != nil {
			return err
		}
		if err := binary.Write(sw.file, binary.LittleEndian, ie.offset); err != nil {
			return err
		}
	}

	// 4. Write Footer: [BloomOffset (8), IndexOffset (8), MagicNumber (8)]
	if err := binary.Write(sw.file, binary.LittleEndian, uint64(bloomOff)); err != nil {
		return err
	}
	if err := binary.Write(sw.file, binary.LittleEndian, uint64(indexOff)); err != nil {
		return err
	}
	if err := binary.Write(sw.file, binary.LittleEndian, MagicNumber); err != nil {
		return err
	}

	return sw.file.Close()
}

// SSTableReader provides read access to an SSTable file.
type SSTableReader struct {
	file  *os.File
	index []indexEntry
	bloom *BloomFilter
}

// NewSSTableReader opens an SSTable file and reads its index and bloom filter.
func NewSSTableReader(path string) (*SSTableReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Read Footer
	if stat.Size() < FooterSize {
		return nil, fmt.Errorf("file too small")
	}
	if _, err := file.Seek(-FooterSize, io.SeekEnd); err != nil {
		return nil, err
	}

	var bloomOff, indexOff, magic uint64
	if err := binary.Read(file, binary.LittleEndian, &bloomOff); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &indexOff); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}
	if magic != MagicNumber {
		return nil, fmt.Errorf("invalid magic number")
	}

	// Read Bloom Filter
	if _, err := file.Seek(int64(bloomOff), io.SeekStart); err != nil {
		return nil, err
	}
	var bloomLen uint32
	if err := binary.Read(file, binary.LittleEndian, &bloomLen); err != nil {
		return nil, err
	}
	bloomData := make([]byte, bloomLen)
	if _, err := io.ReadFull(file, bloomData); err != nil {
		return nil, err
	}
	bloom := NewBloomFilterFromData(bloomData)

	// Read Index
	if _, err := file.Seek(int64(indexOff), io.SeekStart); err != nil {
		return nil, err
	}
	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return nil, err
	}

	index := make([]indexEntry, numEntries)
	for i := uint32(0); i < numEntries; i++ {
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(file, key); err != nil {
			return nil, err
		}
		var offset uint64
		if err := binary.Read(file, binary.LittleEndian, &offset); err != nil {
			return nil, err
		}
		index[i] = indexEntry{key: key, offset: offset}
	}

	return &SSTableReader{
		file:  file,
		index: index,
		bloom: bloom,
	}, nil
}

// Get searches for a key in the SSTable using the Bloom Filter and then binary search on the index.
func (sr *SSTableReader) Get(key []byte) ([]byte, bool, error) {
	// 1. Check Bloom Filter
	if !sr.bloom.MayContain(key) {
		return nil, false, nil
	}

	// 2. Binary search on the index to find the block that might contain the key.
	blockIdx := -1
	low, high := 0, len(sr.index)-1
	for low <= high {
		mid := low + (high-low)/2
		cmp := bytes.Compare(sr.index[mid].key, key)
		if cmp <= 0 {
			blockIdx = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if blockIdx == -1 {
		return nil, false, nil
	}

	// 3. Read the block
	offset := sr.index[blockIdx].offset
	if _, err := sr.file.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, false, err
	}

	// Scan the block for the key.
	var limit uint64
	if blockIdx+1 < len(sr.index) {
		limit = sr.index[blockIdx+1].offset
	} else {
		// Re-read footer to get bloom offset (where data blocks end)
		stat, _ := sr.file.Stat()
		sr.file.Seek(stat.Size()-FooterSize, io.SeekStart)
		var bloomOff uint64
		binary.Read(sr.file, binary.LittleEndian, &bloomOff)
		limit = bloomOff
		sr.file.Seek(int64(offset), io.SeekStart)
	}

	for {
		currOff, _ := sr.file.Seek(0, io.SeekCurrent)
		if uint64(currOff) >= limit {
			break
		}

		var keyLen uint32
		if err := binary.Read(sr.file, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF { break }
			return nil, false, err
		}
		currKey := make([]byte, keyLen)
		if _, err := io.ReadFull(sr.file, currKey); err != nil {
			return nil, false, err
		}

		var valLen uint32
		if err := binary.Read(sr.file, binary.LittleEndian, &valLen); err != nil {
			return nil, false, err
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(sr.file, val); err != nil {
			return nil, false, err
		}

		cmp := bytes.Compare(currKey, key)
		if cmp == 0 {
			return val, true, nil
		}
		if cmp > 0 {
			break
		}
	}

	return nil, false, nil
}

// SSTableIterator allows sequential access to entries in an SSTable.
type SSTableIterator struct {
	reader *SSTableReader
	off    int64
	limit  int64
	curr   Entry
	err    error
}

// Iterator returns a new iterator for the SSTable.
func (sr *SSTableReader) Iterator() *SSTableIterator {
	stat, _ := sr.file.Stat()
	// The data blocks end where the bloom filter begins.
	// We re-read the footer to find the bloom offset.
	sr.file.Seek(stat.Size()-FooterSize, io.SeekStart)
	var bloomOff uint64
	binary.Read(sr.file, binary.LittleEndian, &bloomOff)

	return &SSTableIterator{
		reader: sr,
		off:    0,
		limit:  int64(bloomOff),
	}
}

// Next advances the iterator to the next entry. Returns true if successful.
func (it *SSTableIterator) Next() bool {
	if it.off >= it.limit {
		return false
	}

	it.reader.file.Seek(it.off, io.SeekStart)

	var keyLen uint32
	if err := binary.Read(it.reader.file, binary.LittleEndian, &keyLen); err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(it.reader.file, key); err != nil {
		it.err = err
		return false
	}

	var valLen uint32
	if err := binary.Read(it.reader.file, binary.LittleEndian, &valLen); err != nil {
		it.err = err
		return false
	}
	val := make([]byte, valLen)
	if _, err := io.ReadFull(it.reader.file, val); err != nil {
		it.err = err
		return false
	}

	it.curr = Entry{Key: key, Value: val}
	currOff, _ := it.reader.file.Seek(0, io.SeekCurrent)
	it.off = currOff
	return true
}

// Entry returns the current entry pointed to by the iterator.
func (it *SSTableIterator) Entry() Entry {
	return it.curr
}

// Error returns any error encountered during iteration.
func (it *SSTableIterator) Error() error {
	return it.err
}

// Close closes the underlying SSTable file.
func (sr *SSTableReader) Close() error {
	return sr.file.Close()
}
