package server

import (
	"log"
	"net"
	"runtime"
	"time"

	"github.com/ClickHouse-Ninja/ninjato/src/binary"
	"github.com/ClickHouse-Ninja/ninjato/src/point"
	"github.com/kshvakov/clickhouse"
	"github.com/kshvakov/clickhouse/lib/data"
	"github.com/pierrec/lz4"
)

const (
	BacklogSize     = 5000
	MaxBlockInQueue = 20
)

func NewServer(config ServerConfig) (*Server, error) {
	connect, err := clickhouse.OpenDirect(config.DSN)
	if err != nil {
		return nil, err
	}
	defer connect.Close()
	connect.Begin()
	if _, err = connect.Prepare(insertPointsQuery); err != nil {
		return nil, err
	}
	block, err := connect.Block()
	if err != nil {
		return nil, err
	}
	var (
		concurrency     = runtime.NumCPU()
		backlogSize     = BacklogSize
		maxBlockInQueue = MaxBlockInQueue
		logger          = log.Printf
	)
	if config.Logger != nil {
		logger = config.Logger
	}
	if config.BacklogSize != 0 {
		backlogSize = config.BacklogSize
	}
	if config.Concurrency != 0 {
		concurrency = config.Concurrency
	}
	if config.MaxBlockInQueue != 0 {
		maxBlockInQueue = config.MaxBlockInQueue
	}
	return &Server{
		dsn:         config.DSN,
		block:       block,
		blocks:      make(chan *data.Block, maxBlockInQueue),
		fields:      make(chan []string, 100),
		backlog:     make(chan packet, backlogSize),
		idleConn:    make(chan clickhouse.Clickhouse, concurrency),
		address:     config.Address,
		concurrency: concurrency,
		logger:      logger,
	}, nil
}

type Server struct {
	dsn         string
	block       *data.Block
	blocks      chan *data.Block
	fields      chan []string
	backlog     chan packet
	idleConn    chan clickhouse.Clickhouse
	address     string
	concurrency int
	logger      func(string, ...interface{})
}

func (srv *Server) Listen() error {
	conn, err := net.ListenPacket("udp", srv.address)
	if err != nil {
		return err
	}
	for i := 0; i < srv.concurrency; i++ {
		go srv.backgroundMakeBlock()
		go srv.backgroundWriteBlock()
		go srv.listen(conn)
	}
	srv.backgroundWriteFields()
	return nil
}

func (srv *Server) listen(conn net.PacketConn) {
	var (
		buffer  buffer
		reader  = lz4.NewReader(&buffer)
		decoder = binary.Decoder{
			Input: reader,
		}
		pointsLen = 10
	)
	for {
		if ln, _, err := conn.ReadFrom(buffer.payload[:]); err == nil {
			buffer.idx = 0
			buffer.len = ln
			if service, err := decoder.String(); err == nil {
				packet := packet{
					service: service,
					points:  make([]point.Point, 0, pointsLen),
				}
				for {
					var p point.Point
					if err := point.Unmarshal(&decoder, &p); err != nil {
						break
					}
					if p.IsValid() {
						packet.points = append(packet.points, p)
					}
				}
				if ln := len(packet.points); ln != 0 {
					if ln > pointsLen {
						pointsLen = ln
					}
					srv.backlog <- packet
				}
			}
			reader.Reset(&buffer)
		}
	}
}

func (srv *Server) backgroundMakeBlock() {
	for flush := time.Tick(2 * time.Second); ; {
		block := srv.block.Copy()
		block.Reserve()
	collect:
		for {
			select {
			case packet := <-srv.backlog:
				for _, point := range packet.points {
					block.NumRows++
					block.WriteInt32(0, point.Timestamp())
					block.WriteString(1, packet.service)
					block.WriteString(2, point.Label)
					block.WriteFloat64(3, point.Value)
					block.WriteArray(4, clickhouse.Array(point.Tags().Keys()))
					block.WriteArray(5, clickhouse.Array(point.Tags().Values()))
					block.WriteArray(6, clickhouse.Array(point.Fields().Keys()))
					block.WriteArray(7, clickhouse.Array(point.Fields().Values()))
					select {
					case srv.fields <- point.Fields().Keys():
					default:
					}
				}
			case <-flush:
				break collect
			}
		}
		if block.NumRows != 0 {
			srv.blocks <- block
		}
	}
}

func (srv *Server) backgroundWriteBlock() {
	for {
		block := <-srv.blocks
		if conn, err := srv.conn(); err == nil {
			conn.Begin()
			conn.Prepare(insertPointsQuery)
			if err := conn.WriteBlock(block); err == nil {
				srv.releaseConn(conn, conn.Commit())
			} else {
				conn.Close()
			}
		}
	}
}

func (srv *Server) backgroundWriteFields() {
	var (
		tmp  = make(map[string]struct{}, 100)
		keys = make([]string, 0, 100)
	)
	for flush := time.Tick(10 * time.Second); ; {
		select {
		case fields := <-srv.fields:
			for _, key := range fields {
				if _, ok := tmp[key]; !ok {
					keys = append(keys, key)
					{
						tmp[key] = struct{}{}
					}
				}
			}
		case t := <-flush:
			if len(keys) != 0 && !t.IsZero() {
				if conn, err := srv.conn(); err == nil {
					conn.Begin()
					conn.Prepare(insertFieldsQuery)
					if block, err := conn.Block(); err == nil {
						block.Reserve()
						for _, key := range keys {
							block.NumRows++
							block.WriteString(0, key)
						}
						conn.WriteBlock(block)
					}
					srv.releaseConn(conn, conn.Commit())
				}
				keys = keys[:0]
			}
		}
	}
}

func (srv *Server) conn() (clickhouse.Clickhouse, error) {
	select {
	case conn := <-srv.idleConn:
		return conn, nil
	default:
		return clickhouse.OpenDirect(srv.dsn)
	}
}

func (srv *Server) releaseConn(conn clickhouse.Clickhouse, err error) {
	if err != nil || len(srv.idleConn) > srv.concurrency {
		if err != nil {
			srv.logger("release connect: ", err)
		}
		conn.Close()
		return
	}
	srv.idleConn <- conn
}

const (
	insertPointsQuery = `
	INSERT INTO ninjato.series (
		DateTime
		, Service
		, Label
		, Value
		, Tags.Key
		, Tags.Value
		, Fields.Key
		, Fields.Value
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?
	)
	`
	insertFieldsQuery = `INSERT INTO ninjato.fields (Key) VALUES (?)`
)
