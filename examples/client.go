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
	var (
		datacenters = []string{"EU", "US", "RU"}
		countries   = []string{"UK", "US", "RU", "UA"}
	)
	for {
		for i := 0; i < 15000; i++ {
			err := client.Push(point.New("total_requests", 0.001*float64(i%10)).WithTags(map[string]string{
				"datacenter": datacenters[i%len(datacenters)],
				"country":    countries[i%len(countries)],
			}).WithFields(map[string]float64{
				"cpu":    123 * float64(i),
				"memory": 42 * float64(i),
			}))
			checkError(err)
		}
		time.Sleep(time.Millisecond * 10)
	}
}
