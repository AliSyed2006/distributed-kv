package storage

import (
	"bytes"
	"container/heap"
)

// Compactor performs K-Way Merge of multiple SSTables into a single one.
type Compactor struct {
	destDir string
}

// NewCompactor creates a new Compactor.
func NewCompactor(destDir string) *Compactor {
	return &Compactor{destDir: destDir}
}

type heapEntry struct {
	entry   Entry
	iterIdx int
}

type entryHeap []heapEntry

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].entry.Key, h[j].entry.Key)
	if cmp == 0 {
		// If keys are equal, prioritize the one from the newer SSTable (lower index)
		return h[i].iterIdx < h[j].iterIdx
	}
	return cmp < 0
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *entryHeap) Push(x interface{}) {
	*h = append(*h, x.(heapEntry))
}

func (h *entryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Compact merges the provided SSTables into a new one.
// readers: slice of SSTableReaders, from newest to oldest.
func (c *Compactor) Compact(readers []*SSTableReader, outPath string) error {
	if len(readers) == 0 {
		return nil
	}

	iterators := make([]*SSTableIterator, len(readers))
	h := &entryHeap{}
	heap.Init(h)

	for i, r := range readers {
		iterators[i] = r.Iterator()
		if iterators[i].Next() {
			heap.Push(h, heapEntry{
				entry:   iterators[i].Entry(),
				iterIdx: i,
			})
		}
	}

	// We pass 0 as estimatedEntries for now, SSTableWriter will use a default.
	writer, err := NewSSTableWriter(outPath, 0)
	if err != nil {
		return err
	}

	hasEntries := false
	for h.Len() > 0 {
		// 1. Pop the smallest key. Due to h.Less, if keys are equal,
		// the one from the newest SSTable comes first.
		min := heap.Pop(h).(heapEntry)
		minKey := min.entry.Key
		winnerValue := min.entry.Value
		
		// Push next entry from the iterator we just popped.
		if iterators[min.iterIdx].Next() {
			heap.Push(h, heapEntry{
				entry:   iterators[min.iterIdx].Entry(),
				iterIdx: min.iterIdx,
			})
		}

		// 2. Handle duplicates: pop all entries from other iterators with the same key.
		for h.Len() > 0 && bytes.Equal((*h)[0].entry.Key, minKey) {
			dup := heap.Pop(h).(heapEntry)
			if iterators[dup.iterIdx].Next() {
				heap.Push(h, heapEntry{
					entry:   iterators[dup.iterIdx].Entry(),
					iterIdx: dup.iterIdx,
				})
			}
		}

		// 3. Evict tombstones (nil value) and add to writer.
		if winnerValue != nil {
			if err := writer.Add(Entry{Key: minKey, Value: winnerValue}); err != nil {
				return err
			}
			hasEntries = true
		}
	}

	if hasEntries {
		return writer.Close()
	}

	// If everything was tombstones, we still need to finalize or handle empty file.
	return writer.Close()
}
