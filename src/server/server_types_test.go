package server

import "testing"

func TestBuffer(t *testing.T) {
	var (
		raw = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
		buf = buffer{
			len: 9,
		}
	)
	copy(buf.payload[:], raw)
	for i := 0; i < 3; i++ {
		b := make([]byte, 5)
		t.Log(buf.Read(b))
		t.Log(b)
	}
}
