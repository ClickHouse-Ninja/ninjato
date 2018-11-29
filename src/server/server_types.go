package server

import (
	"io"

	"github.com/ClickHouse-Ninja/ninjato/src/point"
)

type ServerConfig struct {
	DSN             string
	Address         string
	PprofAddr       string
	BacklogSize     int
	Concurrency     int
	MaxBlockInQueue int
	Logger          func(string, ...interface{})
}

type packet struct {
	service string
	points  []point.Point
}

type buffer struct {
	idx     int
	len     int
	payload [16 * 1024]byte
}

func (buf *buffer) Read(b []byte) (int, error) {
	if buf.idx >= buf.len {
		return 0, io.EOF
	}
	from, to := buf.idx, buf.idx+len(b)
	if to > buf.len {
		to = buf.len
	}
	buf.idx += copy(b, buf.payload[from:to])
	return buf.idx - from, nil
}
