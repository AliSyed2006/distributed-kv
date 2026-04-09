package storage

import (
	"encoding/binary"
	"io"
	"os"
	"time"
)

type OpType byte

const (
	OpPut    OpType = 0
	OpDelete OpType = 1
)

// writeTask represents a single write request waiting to be committed.
type writeTask struct {
	op      OpType
	key     []byte
	value   []byte
	errChan chan error
}

type WAL struct {
	file     *os.File
	taskChan chan writeTask
	stopChan chan struct{}
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:     file,
		taskChan: make(chan writeTask, 1000),
		stopChan: make(chan struct{}),
	}

	go w.runBatcher()
	return w, nil
}

func (w *WAL) runBatcher() {
	var batch []writeTask
	const maxBatch = 200
	const maxWait = 10 * time.Millisecond

	timer := time.NewTimer(maxWait)
	if !timer.Stop() {
		<-timer.C
	}

	flush := func() {
		if len(batch) == 0 {
			return
		}

		var buf []byte
		for _, task := range batch {
			keyLen := uint32(len(task.key))
			valLen := uint32(len(task.value))
			// Header(5) + key + ValHeader(4) + val
			entry := make([]byte, 5+len(task.key)+4+len(task.value))
			entry[0] = byte(task.op)
			binary.LittleEndian.PutUint32(entry[1:5], keyLen)
			copy(entry[5:], task.key)
			binary.LittleEndian.PutUint32(entry[5+keyLen:9+keyLen], valLen)
			copy(entry[9+keyLen:], task.value)
			buf = append(buf, entry...)
		}

		_, err := w.file.Write(buf)
		if err == nil {
			err = w.file.Sync()
		}

		for _, task := range batch {
			task.errChan <- err
		}
		batch = batch[:0]
		timer.Stop()
	}

	for {
		select {
		case task := <-w.taskChan:
			if len(batch) == 0 {
				timer.Reset(maxWait)
			}
			batch = append(batch, task)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-timer.C:
			flush()
		case <-w.stopChan:
			flush()
			return
		}
	}
}

func (w *WAL) Append(op OpType, key []byte, value []byte) error {
	errChan := make(chan error, 1)
	w.taskChan <- writeTask{
		op:      op,
		key:     key,
		value:   value,
		errChan: errChan,
	}
	return <-errChan
}

func (w *WAL) Recovery(cb func(op OpType, key, value []byte) error) error {
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(w.file, header); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		op := OpType(header[0])
		kLen := binary.LittleEndian.Uint32(header[1:5])
		key := make([]byte, kLen)
		io.ReadFull(w.file, key)
		vLenBuf := make([]byte, 4)
		io.ReadFull(w.file, vLenBuf)
		vLen := binary.LittleEndian.Uint32(vLenBuf)
		val := make([]byte, vLen)
		io.ReadFull(w.file, val)
		if err := cb(op, key, val); err != nil {
			return err
		}
	}
	w.file.Seek(0, 2)
	return nil
}

func (w *WAL) Close() error {
	close(w.stopChan)
	return w.file.Close()
}
