// Package aof implements a minimalist append-only file API with build-in map/filter/fold operators
//
// package main
// import (
//   "log"
//   "github.com/jeroiraz/go-aof"
// )
//
// func main() {
//   app, err := app.Open("test_file.aof")
//   // error handling
//   defer app.Close()
//
//   var bs []byte
//   // put some data into slice and append to file using the appender
//   off, err := app.Append(bs)
//
//   app.ForEach(func(e *aof.Entry) (cutoff bool, err error) {
//       log.Printf("Entry at offset: %d has size: %d", e.Offset(), e.Size())
//       return false, nil
//   })
//
//   fr, err := app.Filter(func(e *aof.Entry) (include bool, cutoff bool, err error) {
//       return e.Ignore(), false, nil
//   })
//   // error handling
//   log.Printf("Ignored entries %v", len(fr))
//
//   mr, err := app.Map(func(e *aof.Entry) (ignored interface{}, cutoff bool, err error) {
//       return e.Size(), false, nil
//   })
//   // error handling
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

// DefaultMaxEntrySize default max entry size
const DefaultMaxEntrySize = 65535

var (
	ErrUnexpectedReadError = errors.New("aof: Unexpected error reading file")
	ErrCompletingLastEntry = errors.New("aof: Error completing last entry")
	ErrLastEntryIgnored    = errors.New("aof: Last entry fixed and ignored")
	ErrInvalidArguments    = errors.New("aof: Invalid arguments")
	ErrUnexpectedWriteErr  = errors.New("aof: Unexpected error writing file")
	ErrEntryExceedsMaxSize = errors.New("aof: Entry exceeds max supported size")
)

type Appender struct {
	f            *os.File
	r            *bufio.Reader
	w            *bufio.Writer
	mux          sync.Mutex
	maxEntrySize int
	off0         int64
	size         int64
	sharedMem    *sharedMem
}

type Options struct {
	initialOffset int64
	maxEntrySize  int
	perm          os.FileMode
	readOnly      bool
}

type Entry struct {
	off    int64
	size   int
	bytes  []byte
	ignore bool
}

type FoldHandler interface {
	Fold(e *Entry) (bool, error)
	Value() interface{}
	Values() []interface{}
}

type FoldFn func(*Entry, interface{}) (red interface{}, cutoff bool, err error)
type ForEachFn func(*Entry) (cutoff bool, err error)
type MapFn func(*Entry) (ls interface{}, cutoff bool, err error)
type FilterFn func(*Entry) (include bool, cutoff bool, err error)

type sharedMem struct {
	bufEntrySize []byte
	bufEntryFlag []byte
}

const (
	fIgnoreEntry uint8 = 1 << iota
	fValidEntry
)

var byteOrder = binary.LittleEndian

func Open(filename string) (app *Appender, err error) {
	return OpenOptions(filename, &Options{initialOffset: 0, maxEntrySize: DefaultMaxEntrySize, perm: 0644})
}

func OpenOptions(filename string, opts *Options) (app *Appender, err error) {
	if opts.maxEntrySize < 1 || opts.initialOffset < 0 {
		return nil, ErrInvalidArguments
	}

	var flag int
	if opts.readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR | os.O_APPEND
	}

	f, err := os.OpenFile(filename, flag, opts.perm)
	if err != nil {
		return nil, err
	}

	app = &Appender{
		f:            f,
		r:            bufio.NewReader(f),
		w:            bufio.NewWriter(f),
		maxEntrySize: opts.maxEntrySize,
		off0:         opts.initialOffset,
		size:         0,
	}

	err = app.init()

	return
}

func (app *Appender) Close() error {
	return app.f.Close()
}

// Initialize appender. ErrLastRecordIgnored is returned if last entry could not be fully read
func (app *Appender) init() error {
	app.sharedMem = &sharedMem{
		bufEntrySize: make([]byte, entrySizeLen(app.maxEntrySize)),
		bufEntryFlag: make([]byte, 1),
	}

	handler := &sizeFoldHandler{app: app, size: 0}
	err := app.FoldWithHandler(handler)
	app.size = handler.size

	return err
}

func (app *Appender) seek(off int64) error {
	_, err := app.f.Seek(app.off0+off, io.SeekStart)
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
	for i := range app.sharedMem.bufEntrySize {
		app.sharedMem.bufEntrySize[i] = 0
	}

	n, err := app.r.Read(app.sharedMem.bufEntrySize)
	if err != nil && err != io.EOF {
		return 0, ErrUnexpectedReadError
	}

	if n == 0 {
		e.size = 0
		return 0, err
	}

	e.size = readInt(app.sharedMem.bufEntrySize)

	if e.bytes == nil || len(e.bytes) < e.size {
		e.bytes = make([]byte, e.size)
	}

	// Read entry content if size could be fully read
	rc := 0
	if n == len(app.sharedMem.bufEntrySize) {
		for rc < e.size && err == nil {
			rc, err = app.r.Read(e.bytes[:e.size])
			if err != nil && err != io.EOF {
				return 0, ErrUnexpectedReadError
			}
		}
	}

	// Read entry flag
	app.sharedMem.bufEntryFlag[0] = 0
	if rc == e.size {
		_, err = app.r.Read(app.sharedMem.bufEntryFlag)
		if err != nil && err != io.EOF {
			return 0, ErrUnexpectedReadError
		}
	}

	e.ignore = app.sharedMem.bufEntryFlag[0] != fValidEntry

	missingBytes := (len(app.sharedMem.bufEntrySize) - n) + (e.size - rc)
	if app.sharedMem.bufEntryFlag[0] == 0 {
		missingBytes++
	}

	return missingBytes, err
}

func (app *Appender) Append(bs []byte) (int64, error) {
	app.mux.Lock()
	defer app.mux.Unlock()

	if bs == nil || len(bs) == 0 {
		return 0, ErrInvalidArguments
	}
	if len(bs) > app.maxEntrySize {
		return 0, ErrEntryExceedsMaxSize
	}

	// Write encoded entry size
	writeInt(app.sharedMem.bufEntrySize, len(bs))
	n, err := app.w.Write(app.sharedMem.bufEntrySize)
	if n != len(app.sharedMem.bufEntrySize) || err != nil {
		return 0, ErrUnexpectedWriteErr
	}

	// Write entry
	n, err = app.w.Write(bs)
	if n != len(bs) || err != nil {
		return 0, ErrUnexpectedWriteErr
	}

	// Flag as valid entry
	err = app.w.WriteByte(fValidEntry)
	if err != nil {
		return 0, ErrUnexpectedWriteErr
	}

	err = app.w.Flush()
	if err != nil {
		return 0, ErrUnexpectedWriteErr
	}

	off := app.size
	app.size += int64(len(app.sharedMem.bufEntrySize) + len(bs) + len(app.sharedMem.bufEntryFlag))

	return off, nil
}

func (app *Appender) Read(off int64) (*Entry, error) {
	app.mux.Lock()
	defer app.mux.Unlock()

	if off < 0 || off > app.size {
		return nil, ErrInvalidArguments
	}

	err := app.seek(off)
	if err != nil {
		return nil, ErrUnexpectedReadError
	}

	e := &Entry{off: off}
	_, err = e.read(app)

	return e, err
}

func (app *Appender) ForEach(f ForEachFn) error {
	return app.FoldWithHandler(&forEachHandler{f: f})
}

func (app *Appender) Map(f MapFn) ([]interface{}, error) {
	handler := &mapHandler{f: f, ls: nil}
	err := app.FoldWithHandler(handler)
	return handler.Values(), err
}

func (app *Appender) Filter(f FilterFn) ([]interface{}, error) {
	handler := &filterHandler{f: f, ls: nil}
	err := app.FoldWithHandler(handler)
	return handler.Values(), err
}

func (app *Appender) FilteredMap(f FilterFn, m MapFn) ([]interface{}, error) {
	handler := &filteredMapHandler{f: f, m: m, ls: nil}
	err := app.FoldWithHandler(handler)
	return handler.Values(), err
}

func (app *Appender) Fold(f FoldFn, v interface{}) (interface{}, error) {
	handler := &gFoldHandler{f: f, v: v}
	err := app.FoldWithHandler(handler)
	return handler.Value(), err
}

func (app *Appender) FoldWithHandler(handler FoldHandler) error {
	app.mux.Lock()
	defer app.mux.Unlock()

	sharedEntry := &Entry{size: 0, bytes: make([]byte, app.maxEntrySize)}

	var off int64 = 0
	err := app.seek(0)
	if err != nil {
		return err
	}

	for {
		sharedEntry.off = off
		mb, err := sharedEntry.read(app)

		// Complete & Ignore last entry if less bytes has been read
		if mb > 0 {
			bs := make([]byte, mb)
			bs[mb-1] = fIgnoreEntry

			n, err := app.w.Write(bs)
			if n != mb || err != nil {
				return ErrCompletingLastEntry
			}

			if err = app.w.Flush(); err != nil {
				return ErrCompletingLastEntry
			}

			err = ErrLastEntryIgnored
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

		off += int64(len(app.sharedMem.bufEntrySize) + sharedEntry.size + len(app.sharedMem.bufEntryFlag))
	}
}
