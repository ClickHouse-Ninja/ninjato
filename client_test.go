package ninjato

import (
	"testing"

	"github.com/ClickHouse-Ninja/ninjato/src/point"
	"github.com/stretchr/testify/assert"
)

func TestClientPush(t *testing.T) {
	client := client{
		backlog: make(chan *Point, 10),
	}
	for i := 0; i < 10; i++ {
		if err := client.Push(&Point{}); !assert.NoError(t, err) {
			return
		}
	}
	if err := client.Push(&Point{}); assert.Error(t, err) {
		assert.Equal(t, ErrBacklogIsFull, err)
	}
	<-client.backlog
	{
		assert.NoError(t, client.Push(&Point{}))
	}
}

func BenchmarkClientPush(b *testing.B) {
	client := client{
		backlog: make(chan *Point, BacklogSize),
	}
	go func() {
		for {
			<-client.backlog
		}
	}()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			client.Push(point.New("counter", 42).
				WithTags(point.Tags{"country": "RU", "datacenter": "US"}).
				WithFields(point.Fields{"cpu": 1, "memory": 2, "exec": 42}))
		}
	})
}
