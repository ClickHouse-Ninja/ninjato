package main

import (
	"flag"
	"log"

	"github.com/ClickHouse-Ninja/ninjato/src/server"
)

var config server.ServerConfig

func init() {
	flag.StringVar(&config.DSN, "dsn", "tcp://127.0.0.1?database=ninjato", "ClickHouse DSN")
	flag.StringVar(&config.Address, "address", ":1053", "UDP")
	flag.IntVar(&config.Concurrency, "concurrency", 4, "number of the parralel write workers")
}
func main() {
	flag.Parse()
	server, err := server.NewServer(config)
	if err != nil {
		log.Fatal(err)
	}
	server.Listen()
}
