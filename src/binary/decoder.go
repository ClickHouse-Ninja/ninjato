package binary

import (
	"encoding/binary"
	"io"
	"math"
)

type Decoder struct {
	Input   io.Reader
	scratch [binary.MaxVarintLen64]byte
}

func (decoder *Decoder) String() (string, error) {
	strlen, err := decoder.UInt8()
	if err != nil {
		return "", err
	}
	str, err := decoder.fixed(int(strlen))
	if err != nil {
		return "", err
	}
	return string(str), nil
}

func (decoder *Decoder) fixed(ln int) ([]byte, error) {
	buf := make([]byte, ln)
	if _, err := decoder.read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (decoder *Decoder) UInt8() (uint8, error) {
	if _, err := decoder.read(decoder.scratch[:1]); err != nil {
		return 0, err
	}
	return uint8(decoder.scratch[0]), nil
}

func (decoder *Decoder) Int32() (int32, error) {
	v, err := decoder.uint32()
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func (decoder *Decoder) uint64() (uint64, error) {
	if _, err := decoder.read(decoder.scratch[:8]); err != nil {
		return 0, err
	}
	return uint64(decoder.scratch[0]) |
		uint64(decoder.scratch[1])<<8 |
		uint64(decoder.scratch[2])<<16 |
		uint64(decoder.scratch[3])<<24 |
		uint64(decoder.scratch[4])<<32 |
		uint64(decoder.scratch[5])<<40 |
		uint64(decoder.scratch[6])<<48 |
		uint64(decoder.scratch[7])<<56, nil
}

func (decoder *Decoder) uint32() (uint32, error) {
	if _, err := decoder.read(decoder.scratch[:4]); err != nil {
		return 0, err
	}
	return uint32(decoder.scratch[0]) |
		uint32(decoder.scratch[1])<<8 |
		uint32(decoder.scratch[2])<<16 |
		uint32(decoder.scratch[3])<<24, nil
}

func (decoder *Decoder) Float64() (float64, error) {
	v, err := decoder.uint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

func (decoder *Decoder) read(p []byte) (n int, err error) {
	return io.ReadFull(decoder.Input, p)
}
