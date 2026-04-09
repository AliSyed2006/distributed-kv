package storage

import (
	"bytes"
)

// Compactor performs K-Way Merge of multiple SSTables into a single one.
type Compactor struct {
	destDir string
}

// NewCompactor creates a new Compactor.
func NewCompactor(destDir string) *Compactor {
	return &Compactor{destDir: destDir}
}

// Compact merges the provided SSTables into a new one.
// readers: slice of SSTableReaders, from newest to oldest.
func (c *Compactor) Compact(readers []*SSTableReader, outPath string) error {
	if len(readers) == 0 {
		return nil
	}

	iterators := make([]*SSTableIterator, len(readers))
	for i, r := range readers {
		iterators[i] = r.Iterator()
		iterators[i].Next() // Initialize
	}

	writer, err := NewSSTableWriter(outPath)
	if err != nil {
		return err
	}

	var mergedEntries []Entry

	for {
		// 1. Find the smallest key across all iterators.
		var minKey []byte
		foundAny := false
		for _, it := range iterators {
			if it.off < it.limit {
				if !foundAny || bytes.Compare(it.Entry().Key, minKey) < 0 {
					minKey = it.Entry().Key
					foundAny = true
				}
			}
		}

		if !foundAny {
			break // All iterators exhausted
		}

		// 2. Pick the value from the newest SSTable (lowest index in our slice).
		var winnerValue []byte
		winnerFound := false
		for _, it := range iterators {
			if it.off <= it.limit && bytes.Equal(it.Entry().Key, minKey) {
				if !winnerFound {
					winnerValue = it.Entry().Value
					winnerFound = true
				}
				// Advance all iterators that have this key.
				it.Next()
			}
		}

		// 3. Evict tombstones (nil value) and add to merged set.
		if winnerValue != nil {
			mergedEntries = append(mergedEntries, Entry{Key: minKey, Value: winnerValue})
		}
	}

	if len(mergedEntries) > 0 {
		return writer.Write(mergedEntries)
	}

	// If everything was tombstones, we still need to finalize or handle empty file.
	// For now, just close if nothing to write.
	return writer.file.Close()
}
