package main

import (
	"flag"
	"log"

	"github.com/ClickHouse-Ninja/ninjato/src/server"
)

var config server.ServerConfig

func init() {
	flag.StringVar(&config.DSN, "dsn", "tcp://127.0.0.1:9000?database=ninjato", "ClickHouse DSN")
	flag.StringVar(&config.Address, "address", ":1053", "UDP")
	flag.IntVar(&config.Concurrency, "concurrency", 0, "number of the parralel write workers (default auto)")
	flag.IntVar(&config.BacklogSize, "backlog-size", 5000, "number of incoming packets in the backlog")
	flag.IntVar(&config.MaxBlockInQueue, "max-block-in-queue", 20, "number of prepared blocks in the write queue")
	flag.StringVar(&config.PprofAddr, "pprof-addr", "", "")
}
func main() {
	flag.Parse()
	server, err := server.NewServer(config)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(server.Listen())
}
