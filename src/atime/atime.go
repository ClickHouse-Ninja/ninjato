package atime

import (
	"sync/atomic"
	"time"
)

var timestamp int64

func init() {
	atomic.StoreInt64(&timestamp, time.Now().Unix())
	go func() {
		for tick := time.Tick(time.Second); ; {
			atomic.StoreInt64(&timestamp, (<-tick).Unix())
		}
	}()
}

// Now returns the current time rounded to the seconds.
func Now() time.Time {
	return time.Unix(atomic.LoadInt64(&timestamp), 0)
}
