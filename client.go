package ninjato

import (
	"errors"
	"log"
	"net"
	"runtime"
	"time"

	"github.com/pierrec/lz4"

	"github.com/ClickHouse-Ninja/ninjato/src/binary"
	"github.com/ClickHouse-Ninja/ninjato/src/point"
)

type Client interface {
	Push(*Point) error
}

const (
	PayloadSize = 16 * 1024
	BacklogSize = 10000
)

func NewClient(service string, config ClientConfig) (_ Client, err error) {
	var (
		backlog     = BacklogSize
		concurrency = runtime.NumCPU()
	)
	if config.BacklogSize > 0 {
		backlog = config.BacklogSize
	}
	if config.Concurrency > 0 {
		concurrency = config.Concurrency
	}
	client := client{
		debug:   config.Debug,
		logger:  log.Printf,
		service: service,
		backlog: make(chan *Point, backlog),
	}
	if client.address, err = net.ResolveUDPAddr("udp", config.Address); err != nil {
		return nil, err
	}
	for i := 0; i < concurrency; i++ {
		if err = client.background(i); err != nil {
			return nil, err
		}
	}
	return &client, nil
}

type client struct {
	service string
	address *net.UDPAddr
	backlog chan *Point
	debug   bool
	logger  func(string, ...interface{})
}

var ErrBacklogIsFull = errors.New("backlog is full")

func (cl *client) Push(p *Point) error {
	select {
	case cl.backlog <- p:
		return nil
	default:
		return ErrBacklogIsFull
	}
}

func (cl *client) background(num int) error {
	var (
		flush        = time.NewTicker(10 * time.Second)
		compressed   = &buffer{}
		uncompressed = &buffer{}
		encoder      = binary.Encoder{Output: uncompressed}
		compressor   = lz4.NewWriter(compressed)
		conn, err    = net.DialUDP("udp", nil, cl.address)
	)
	if err != nil {
		return err
	}
	go func() {
		for {
			var count uint16
			encoder.String(cl.service)
		collect:
			for {
				select {
				case p := <-cl.backlog:
					if err := point.Marshal(&encoder, p); err != nil {
						break collect
					}
					if count++; uncompressed.len >= PayloadSize {
						break collect
					}
				case <-flush.C:
					break collect
				}
			}
			if count > 0 {
				if _, err := compressor.Write(uncompressed.bytes()); err != nil {
					cl.logger("compressor %v", err)
				}
				if err := compressor.Flush(); err != nil {
					cl.logger("LZ4 flush: %v", err)
				}
				if _, err := conn.Write(compressed.bytes()); err != nil {
					cl.logger("conn write: %v", err)
				}
				if cl.debug {
					cl.logger("goroutine=%d, count=%d, buffer=%d compressed=%d", num, count, uncompressed.len, compressed.len)
				}
				compressor.Reset(compressed)
				uncompressed.reset()
			}
			compressed.reset()
		}
	}()
	return nil
}

var _ Client = (*client)(nil)
