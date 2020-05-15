# go-aof
Minimalist Append Only File API

### Installation

```go
go get github.com/jeroiraz/go-aof
```


### Example

```go
package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/jeroiraz/go-aof"
)

func randomBytes(size int) []byte {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, size)
	rand.Read(b)
	for i, v := range b {
		if v == 0 {
			b[i]++
		}
	}
	return b
}

func main() {
	app, err := aof.Open("file.aof")
	if err != nil {
		panic(err)
	}
	defer app.Close()

	for i := 1; i <= 10; i++ {
		b := randomBytes(i)
		_, err := app.Append(b)
		if err != nil {
			panic(err)
		}
	}

	err = app.ForEach(func(e *aof.Entry) (cutoff bool, err error) {
		log.Printf("Entry at offset: %d has size: %d", e.Offset(), e.Size())
		return false, nil
	})
	if err != nil {
		panic(err)
	}

	fr, err := app.Filter(func(e *aof.Entry) (include bool, cutoff bool, err error) {
		return e.Incomplete(), false, nil
	})
	if err != nil {
		panic(err)
	}
	log.Printf("Incomplete entries %v", len(fr))

	mr, err := app.Map(func(e *aof.Entry) (size interface{}, cutoff bool, err error) {
		return e.Size(), false, nil
	})
	if err != nil {
		panic(err)
	}

	log.Printf("Sizes: %v", mr)
}
```
