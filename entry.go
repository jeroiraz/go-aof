package aof

import "fmt"

func (e *Entry) Offset() int64 {
	return e.off
}

func (e *Entry) Size() int {
	return e.size
}

func (e *Entry) Bytes() []byte {
	return e.bytes[:e.size]
}

func (e *Entry) Incomplete() bool {
	return e.incomplete
}

func (e *Entry) String() string {
	return fmt.Sprintf("{offset: %v, bytes: %v, incomplete: %v}", e.Offset(), e.Bytes(), e.Incomplete())
}
