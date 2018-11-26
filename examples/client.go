package main

import (
	"log"
	"time"

	"github.com/ClickHouse-Ninja/ninjato"
	"github.com/ClickHouse-Ninja/ninjato/src/point"
)

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	client, err := ninjato.NewClient("test_service", ninjato.ClientConfig{
		Debug:       false,
		Address:     "127.0.0.1:1053",
		BacklogSize: 25000,
	})
	checkError(err)
	for {
		var i float64
		for i = 0; i < 15000; i++ {
			err := client.Push(point.New("count", i).WithTags(map[string]string{
				"datacenter": "EU",
				"country":    "RU",
			}).WithFields(map[string]float64{
				"cpu":    123 * i,
				"memory": 42 * i,
				"calls":  56 * i,
			}))
			checkError(err)
		}
		time.Sleep(time.Millisecond * 10)
	}
}
