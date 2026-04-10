package storage

import (
	"bufio"
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
	buf          *bufio.Writer
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
// estimatedEntries is used to size the Bloom Filter.
func NewSSTableWriter(path string, estimatedEntries int) (*SSTableWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	if estimatedEntries <= 0 {
		estimatedEntries = 1000 // Default fallback
	}

	return &SSTableWriter{
		file:  file,
		buf:   bufio.NewWriterSize(file, 4*1024*1024),
		bloom: NewBloomFilter(estimatedEntries*10, 7),
	}, nil
}

// Add appends a single entry to the SSTable. Entries must be added in sorted order.
func (sw *SSTableWriter) Add(entry Entry) error {
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
	if sw.currBlockLen >= BlockSize {
		sw.currBlockOff += uint64(sw.currBlockLen)
		sw.currBlockLen = 0
	}

	return nil
}

// Write takes sorted entries and persists them to the SSTable file.
// Deprecated: Use Add and Close for incremental writes to avoid OOM.
func (sw *SSTableWriter) Write(entries []Entry) error {
	if len(entries) == 0 {
		return fmt.Errorf("no entries to write")
	}

	for _, entry := range entries {
		if err := sw.Add(entry); err != nil {
			return err
		}
	}

	return sw.Close()
}

func (sw *SSTableWriter) writeEntry(e Entry) error {
	keyLen := uint32(len(e.Key))
	valLen := uint32(len(e.Value))
	
	// Write KeyLen
	if err := binary.Write(sw.buf, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	// Write Key
	if _, err := sw.buf.Write(e.Key); err != nil {
		return err
	}
	// Write ValueLen
	if err := binary.Write(sw.buf, binary.LittleEndian, valLen); err != nil {
		return err
	}
	// Write Value
	if _, err := sw.buf.Write(e.Value); err != nil {
		return err
	}

	sw.currBlockLen += 4 + keyLen + 4 + valLen
	return nil
}

// Close finalizes the SSTable file by writing Bloom Filter, Index and Footer.
func (sw *SSTableWriter) Close() error {
	// Flush the buffer before writing footers and closing.
	if err := sw.buf.Flush(); err != nil {
		return err
	}

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
	file     *os.File
	index    []indexEntry
	bloom    *BloomFilter
	bloomOff uint64
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
	
	footerOff := stat.Size() - FooterSize
	buf := make([]byte, FooterSize)
	if _, err := file.ReadAt(buf, footerOff); err != nil {
		return nil, err
	}

	bloomOff := binary.LittleEndian.Uint64(buf[0:8])
	indexOff := binary.LittleEndian.Uint64(buf[8:16])
	magic := binary.LittleEndian.Uint64(buf[16:24])
	
	if magic != MagicNumber {
		return nil, fmt.Errorf("invalid magic number")
	}

	// Read Bloom Filter
	var bloomLen uint32
	lenBuf := make([]byte, 4)
	if _, err := file.ReadAt(lenBuf, int64(bloomOff)); err != nil {
		return nil, err
	}
	bloomLen = binary.LittleEndian.Uint32(lenBuf)
	
	bloomData := make([]byte, bloomLen)
	if _, err := file.ReadAt(bloomData, int64(bloomOff)+4); err != nil {
		return nil, err
	}
	bloom := NewBloomFilterFromData(bloomData)

	// Read Index
	if _, err := file.ReadAt(lenBuf, int64(indexOff)); err != nil {
		return nil, err
	}
	numEntries := binary.LittleEndian.Uint32(lenBuf)

	index := make([]indexEntry, numEntries)
	currOff := int64(indexOff) + 4
	for i := uint32(0); i < numEntries; i++ {
		if _, err := file.ReadAt(lenBuf, currOff); err != nil {
			return nil, err
		}
		keyLen := binary.LittleEndian.Uint32(lenBuf)
		currOff += 4

		key := make([]byte, keyLen)
		if _, err := file.ReadAt(key, currOff); err != nil {
			return nil, err
		}
		currOff += int64(keyLen)

		offBuf := make([]byte, 8)
		if _, err := file.ReadAt(offBuf, currOff); err != nil {
			return nil, err
		}
		offset := binary.LittleEndian.Uint64(offBuf)
		currOff += 8

		index[i] = indexEntry{key: key, offset: offset}
	}

	return &SSTableReader{
		file:     file,
		index:    index,
		bloom:    bloom,
		bloomOff: bloomOff,
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

	// 3. Read the entire block into memory at once.
	blockOff := int64(sr.index[blockIdx].offset)
	var limit int64
	if blockIdx+1 < len(sr.index) {
		limit = int64(sr.index[blockIdx+1].offset)
	} else {
		limit = int64(sr.bloomOff)
	}

	blockSize := limit - blockOff
	blockBuf := make([]byte, blockSize)
	if _, err := sr.file.ReadAt(blockBuf, blockOff); err != nil {
		return nil, false, err
	}

	// Scan the RAM buffer for the key.
	currOff := 0
	for currOff < len(blockBuf) {
		if currOff+4 > len(blockBuf) {
			break
		}
		keyLen := binary.LittleEndian.Uint32(blockBuf[currOff : currOff+4])
		currOff += 4

		if currOff+int(keyLen) > len(blockBuf) {
			break
		}
		currKey := blockBuf[currOff : currOff+int(keyLen)]
		currOff += int(keyLen)

		if currOff+4 > len(blockBuf) {
			break
		}
		valLen := binary.LittleEndian.Uint32(blockBuf[currOff : currOff+4])
		currOff += 4

		cmp := bytes.Compare(currKey, key)
		if cmp == 0 {
			if currOff+int(valLen) > len(blockBuf) {
				break
			}
			val := blockBuf[currOff : currOff+int(valLen)]
			// Return a copy of the value since blockBuf is local.
			res := make([]byte, len(val))
			copy(res, val)
			return res, true, nil
		}
		
		currOff += int(valLen)
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
	return &SSTableIterator{
		reader: sr,
		off:    0,
		limit:  int64(sr.bloomOff),
	}
}

// Next advances the iterator to the next entry. Returns true if successful.
func (it *SSTableIterator) Next() bool {
	if it.off >= it.limit {
		return false
	}

	currOff := it.off
	lenBuf := make([]byte, 4)
	if _, err := it.reader.file.ReadAt(lenBuf, currOff); err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}
	keyLen := binary.LittleEndian.Uint32(lenBuf)
	currOff += 4

	key := make([]byte, keyLen)
	if _, err := it.reader.file.ReadAt(key, currOff); err != nil {
		it.err = err
		return false
	}
	currOff += int64(keyLen)

	if _, err := it.reader.file.ReadAt(lenBuf, currOff); err != nil {
		it.err = err
		return false
	}
	valLen := binary.LittleEndian.Uint32(lenBuf)
	currOff += 4

	val := make([]byte, valLen)
	if _, err := it.reader.file.ReadAt(val, currOff); err != nil {
		it.err = err
		return false
	}
	currOff += int64(valLen)

	it.curr = Entry{Key: key, Value: val}
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
