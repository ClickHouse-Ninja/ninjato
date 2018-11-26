package atime_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse-Ninja/ninjato/src/atime"
)

func BenchmarkStd(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		time.Now()
	}
}

func BenchmarkAtime(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		atime.Now()
	}
}

func TestNow(t *testing.T) {
	var (
		end  = time.NewTimer(10 * time.Second)
		tick = time.Tick(time.Second)
	)
	for {
		select {
		case <-tick:
			assert.True(t, time.Now().Sub(atime.Now()) < 2*time.Second)
		case <-end.C:
			return
		}
	}
}
