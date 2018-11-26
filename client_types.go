package ninjato

import (
	"errors"
	"math"

	"github.com/ClickHouse-Ninja/ninjato/src/point"
)

type ClientConfig struct {
	Address     string
	PayloadSize int
	BacklogSize int
	Concurrency int
	Debug       bool
}

type Point = point.Point

type buffer struct {
	len     int
	payload [math.MaxInt16]byte
}

var ErrBufferIsFull = errors.New("buffer is full")

func (buf *buffer) Write(b []byte) (int, error) {
	if buf.len+len(b) > math.MaxInt16 {
		return 0, ErrBufferIsFull
	}
	buf.len += len(b)
	return copy(buf.payload[buf.len-len(b):], b), nil
}

func (buf *buffer) bytes() []byte {
	return buf.payload[:buf.len]
}

func (buf *buffer) reset() {
	buf.len = 0
}
