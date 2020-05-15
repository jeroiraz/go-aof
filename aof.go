// Package aof implements a minimalist append-only file API with build-in fold-powered operators
//
// package main
// import (
//   "log"
//   "github.com/jeroiraz/go-aof"
// )
//
// // Note: Error handling not included
//
// func main() {
//   app, err := app.Open("test_file.aof")
//   defer app.Close()
//
//   // put some data into slice and append to file using the appender
//   var bs []byte
//   off, err := app.Append(bs)
//
//   app.ForEach(func(e *aof.Entry) (cutoff bool, err error) {
//       log.Printf("Entry at offset: %d has size: %d", e.Offset(), e.Size())
//       return false, nil
//   })
//
//   mr, err := app.Map(func(e *aof.Entry) (size interface{}, cutoff bool, err error) {
//       return e.Size(), false, nil
//   })
//   log.Printf("Sizes: %v", mr)
// }
package aof

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
	"os"
	"sync"
)

var (
	ErrUnexpectedReadError = errors.New("aof: Unexpected error reading file")
	ErrCompletingLastEntry = errors.New("aof: Error completing last Entry")
	ErrLastEntryIncomplete = errors.New("aof: Last Entry was incomplete")
	ErrInvalidArguments    = errors.New("aof: Invalid arguments")
	ErrUnexpectedWriteErr  = errors.New("aof: Unexpected error writing file")
	ErrEntryExceedsMaxSize = errors.New("aof: Entry exceeds max supported size")
	ErrAppenderClosed      = errors.New("aof: Appender closed")
)

type Appender struct {
	f            *os.File
	r            *bufio.Reader
	w            *bufio.Writer
	mux          sync.Mutex
	maxEntrySize int
	baseOffset   int64
	size         int64
	sharedMem    *sharedMem
	closed       bool
	err          error
}

type Config struct {
	MaxEntrySize int
	BaseOffset   int64
	Perm         os.FileMode
	ReadOnly     bool
}

const DefaultMaxEntrySize = 65535
const DefaultBaseOffset = 0
const DefaultPerm = 0644
const DefaultReadOnly = false

type Entry struct {
	off        int64
	size       int
	bytes      []byte
	incomplete bool
}

type FoldHandler interface {
	Fold(e *Entry) (bool, error)
	Value() interface{}
	Values() []interface{}
}

// Note: entry may be used as a shared memory and it should not leave its intended scope
type FoldFn func(e *Entry, pred interface{}) (red interface{}, cutoff bool, err error)
type ForEachFn func(e *Entry) (cutoff bool, err error)
type MapFn func(e *Entry) (r interface{}, cutoff bool, err error)
type FilterFn func(e *Entry) (include bool, cutoff bool, err error)

type sharedMem struct {
	sharedEntry    *Entry
	bufRWEntrySize []byte
	bufRWEntryFlag []byte
}

const (
	fIncompleteEntry uint8 = 1 << iota
	fCompleteEntry
)

var byteOrder = binary.LittleEndian

func Open(filename string) (app *Appender, err error) {
	defaultCfg := &Config{
		MaxEntrySize: DefaultMaxEntrySize,
		BaseOffset:   DefaultBaseOffset,
		Perm:         DefaultPerm,
		ReadOnly:     DefaultReadOnly,
	}
	return OpenWithConfig(filename, defaultCfg)
}

func OpenWithConfig(filename string, cfg *Config) (app *Appender, err error) {
	if cfg.MaxEntrySize < 1 || cfg.BaseOffset < 0 {
		return nil, ErrInvalidArguments
	}

	var flag int
	if cfg.ReadOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR | os.O_APPEND
	}

	f, err := os.OpenFile(filename, flag, cfg.Perm)
	if err != nil {
		return nil, err
	}

	sharedMem := &sharedMem{
		sharedEntry:    &Entry{size: 0, bytes: make([]byte, cfg.MaxEntrySize)},
		bufRWEntrySize: make([]byte, entrySizeLen(cfg.MaxEntrySize)),
		bufRWEntryFlag: make([]byte, 1),
	}

	app = &Appender{
		f:            f,
		r:            bufio.NewReader(f),
		w:            bufio.NewWriter(f),
		maxEntrySize: cfg.MaxEntrySize,
		baseOffset:   cfg.BaseOffset,
		size:         0,
		sharedMem:    sharedMem,
		closed:       false,
		err:          nil,
	}

	handler := &sizeFoldHandler{app: app, size: 0}
	err = app.FoldWithHandler(handler)
	app.size = handler.size

	return
}

func (app *Appender) Close() error {
	app.mux.Lock()
	defer app.mux.Unlock()

	return app.close(nil)
}

func (app *Appender) close(err error) error {
	app.closed = true
	app.err = err
	return app.f.Close()
}

func (app *Appender) seek(off int64) error {
	_, err := app.f.Seek(app.baseOffset+off, io.SeekStart)
	if err != nil {
		return ErrUnexpectedReadError
	}
	app.r.Reset(app.f)
	return nil
}

func entrySizeLen(maxEntrySize int) int {
	len := bits.Len(uint(maxEntrySize))
	if len <= 16 {
		return 2
	}
	if len <= 32 {
		return 4
	}
	panic("Unreacheable point")
}

func readInt(b []byte) int {
	switch len(b) {
	case 2:
		return int(byteOrder.Uint16(b))
	case 4:
		return int(byteOrder.Uint32(b))
	}
	panic("Unreacheable point")
}

func writeInt(b []byte, n int) {
	switch len(b) {
	case 2:
		byteOrder.PutUint16(b, uint16(n))
		return
	case 4:
		byteOrder.PutUint32(b, uint32(n))
		return
	}
	panic("Unreacheable point")
}

// read fills up entry. Number of bytes missing to complete the entry is returned
func (e *Entry) read(app *Appender) (int, error) {
	// Read entry size
	for i := range app.sharedMem.bufRWEntrySize {
		app.sharedMem.bufRWEntrySize[i] = 0
	}

	n, err := app.r.Read(app.sharedMem.bufRWEntrySize)
	if err != nil && err != io.EOF {
		return 0, ErrUnexpectedReadError
	}

	if n == 0 {
		e.size = 0
		return 0, err
	}

	e.size = readInt(app.sharedMem.bufRWEntrySize)

	if e.bytes == nil || len(e.bytes) < e.size {
		e.bytes = make([]byte, e.size)
	}

	// Read entry content if size could be fully read
	rc := 0
	if n == len(app.sharedMem.bufRWEntrySize) {
		for rc < e.size && err == nil {
			rc, err = app.r.Read(e.bytes[:e.size])
			if err != nil && err != io.EOF {
				return 0, ErrUnexpectedReadError
			}
		}
	}

	// Read entry flag
	app.sharedMem.bufRWEntryFlag[0] = 0
	if rc == e.size {
		_, err = app.r.Read(app.sharedMem.bufRWEntryFlag)
		if err != nil && err != io.EOF {
			return 0, ErrUnexpectedReadError
		}
	}

	e.incomplete = app.sharedMem.bufRWEntryFlag[0] != fCompleteEntry

	missingBytes := (len(app.sharedMem.bufRWEntrySize) - n) + (e.size - rc)
	if app.sharedMem.bufRWEntryFlag[0] == 0 {
		missingBytes++
	}

	return missingBytes, err
}

func (app *Appender) Append(bs []byte) (off int64, err error) {
	offs, err := app.AppendBulk([][]byte{bs})
	if err != nil {
		return 0, err
	}
	return offs[0], nil
}

func (app *Appender) AppendBulk(bss [][]byte) (offs []int64, err error) {
	app.mux.Lock()
	defer app.mux.Unlock()

	if app.closed {
		return nil, ErrAppenderClosed
	}

	if bss == nil || len(bss) == 0 {
		return nil, ErrInvalidArguments
	}

	offs = make([]int64, len(bss))

	var writtenBytes int64 = 0

	for i, bs := range bss {
		if bs == nil || len(bs) == 0 {
			return nil, ErrInvalidArguments
		}
		if len(bs) > app.maxEntrySize {
			return nil, ErrEntryExceedsMaxSize
		}

		// Write encoded entry size
		writeInt(app.sharedMem.bufRWEntrySize, len(bs))
		n, err := app.w.Write(app.sharedMem.bufRWEntrySize)
		if n != len(app.sharedMem.bufRWEntrySize) || err != nil {
			app.close(err)
			return nil, ErrUnexpectedWriteErr
		}

		// Write entry
		n, err = app.w.Write(bs)
		if n != len(bs) || err != nil {
			app.close(err)
			return nil, ErrUnexpectedWriteErr
		}

		// Flag as valid entry
		if err = app.w.WriteByte(fCompleteEntry); err != nil {
			app.close(err)
			return nil, ErrUnexpectedWriteErr
		}

		offs[i] = app.size + writtenBytes
		writtenBytes += int64(len(app.sharedMem.bufRWEntrySize) + len(bs) + len(app.sharedMem.bufRWEntryFlag))
	}

	if err = app.w.Flush(); err != nil {
		app.close(err)
		return nil, ErrUnexpectedWriteErr
	}

	app.size += writtenBytes

	return offs, nil
}

func (app *Appender) Read(off int64) (e *Entry, err error) {
	app.mux.Lock()
	defer app.mux.Unlock()

	if app.closed {
		return nil, ErrAppenderClosed
	}

	if off < 0 || off > app.size {
		return nil, ErrInvalidArguments
	}

	if err := app.seek(off); err != nil {
		return nil, ErrUnexpectedReadError
	}

	e = &Entry{off: off}
	_, err = e.read(app)

	return e, err
}

func (app *Appender) ForEach(f ForEachFn) error {
	return app.FoldWithHandler(&forEachHandler{f: f})
}

func (app *Appender) Map(f MapFn) (ls []interface{}, err error) {
	handler := &mapHandler{f: f, ls: nil}
	err = app.FoldWithHandler(handler)
	return handler.Values(), err
}

func (app *Appender) FilteredMap(f FilterFn, m MapFn) (ls []interface{}, err error) {
	handler := &filteredMapHandler{f: f, m: m, ls: nil}
	err = app.FoldWithHandler(handler)
	return handler.Values(), err
}

func (app *Appender) Fold(f FoldFn, v interface{}) (ret interface{}, err error) {
	handler := &gFoldHandler{f: f, v: v}
	err = app.FoldWithHandler(handler)
	return handler.Value(), err
}

func (app *Appender) FoldWithHandler(handler FoldHandler) error {
	app.mux.Lock()
	defer app.mux.Unlock()

	if app.closed {
		return ErrAppenderClosed
	}

	sharedEntry := app.sharedMem.sharedEntry

	var off int64 = 0
	err := app.seek(0)
	if err != nil {
		return err
	}

	for {
		sharedEntry.off = off
		mb, err := sharedEntry.read(app)

		// Complete last entry if less bytes has been read
		if mb > 0 {
			bs := make([]byte, mb)
			bs[mb-1] = fIncompleteEntry

			n, err := app.w.Write(bs)
			if n != mb || err != nil {
				app.close(err)
				return ErrCompletingLastEntry
			}

			if err = app.w.Flush(); err != nil {
				app.close(err)
				return ErrCompletingLastEntry
			}

			err = ErrLastEntryIncomplete
		}

		if err == io.EOF {
			return nil
		}

		cutoff, herr := handler.Fold(sharedEntry)
		if herr != nil {
			return herr
		}

		if cutoff {
			return err
		}

		off += int64(len(app.sharedMem.bufRWEntrySize) + sharedEntry.size + len(app.sharedMem.bufRWEntryFlag))
	}
}
