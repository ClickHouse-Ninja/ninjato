package point_test

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/ClickHouse-Ninja/ninjato/src/binary"
	"github.com/ClickHouse-Ninja/ninjato/src/point"
	"github.com/pierrec/lz4"
	"github.com/stretchr/testify/assert"
)

func BenchmarkNewPoint(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		point.New("some_label", 42).WithTags(point.Tags{
			"country_code": "RU",
			"datacenter":   "EU",
		}).WithFields(point.Fields{
			"cpu":    0.56,
			"memory": 1313,
			"disk":   56,
		})
	}
}

func BenchmarkNewPointSmall(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		point.New("some_label", 42)
	}
}

func BenchmarkMarshalPoint(b *testing.B) {
	var (
		p = point.New("some_label", 42).WithTags(point.Tags{
			"country_code": "RU",
			"datacenter":   "EU",
		}).WithFields(point.Fields{
			"cpu":    0.56,
			"memory": 1313,
			"disk":   56,
		})
		enc = binary.Encoder{
			Output: ioutil.Discard,
		}
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := point.Marshal(&enc, p); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkMarshalPointSmall(b *testing.B) {
	var (
		p   = point.New("some_label", 42)
		enc = binary.Encoder{
			Output: ioutil.Discard,
		}
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := point.Marshal(&enc, p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalPointCompressionLZ4(b *testing.B) {
	var (
		p = point.New("some_label", 42).WithTags(point.Tags{
			"country_code": "RU",
			"datacenter":   "EU",
		}).WithFields(point.Fields{
			"cpu":    0.56,
			"memory": 1313,
			"disk":   56,
		})
		enc = binary.Encoder{
			Output: lz4.NewWriter(ioutil.Discard),
		}
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := point.Marshal(&enc, p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalPointCompressionLZ4Small(b *testing.B) {
	var (
		p   = point.New("some_label", 42)
		enc = binary.Encoder{
			Output: lz4.NewWriter(ioutil.Discard),
		}
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := point.Marshal(&enc, p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalPoint(b *testing.B) {
	var (
		p = point.New("some_label", 42).WithTags(point.Tags{
			"country_code": "RU",
			"datacenter":   "EU",
		}).WithFields(point.Fields{
			"cpu":    0.56,
			"memory": 1313,
			"disk":   56,
		})
		buf = bytes.Buffer{}
		enc = binary.Encoder{
			Output: &buf,
		}
	)
	if err := point.Marshal(&enc, p); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var (
			p2  point.Point
			dec = binary.Decoder{
				Input: bytes.NewBuffer(buf.Bytes()),
			}
		)
		if err := point.Unmarshal(&dec, &p2); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalPointSmall(b *testing.B) {
	var (
		p   = point.New("some_label", 42)
		buf = bytes.Buffer{}
		enc = binary.Encoder{
			Output: &buf,
		}
	)
	if err := point.Marshal(&enc, p); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var (
			p2  point.Point
			dec = binary.Decoder{
				Input: bytes.NewBuffer(buf.Bytes()),
			}
		)
		if err := point.Unmarshal(&dec, &p2); err != nil {
			b.Fatal(err)
		}
	}
}
func TestSerialize(t *testing.T) {
	var (
		p = point.New("some_label", 42).WithTags(point.Tags{
			"country_code": "RU",
			"datacenter":   "EU",
		}).WithFields(point.Fields{
			"cpu":    0.42,
			"disk":   256,
			"memory": 1024,
		})
		buf = bytes.Buffer{}
		enc = binary.Encoder{
			Output: &buf,
		}
		dec = binary.Decoder{
			Input: &buf,
		}
	)
	if err := point.Marshal(&enc, p); assert.NoError(t, err) {
		var p2 point.Point
		if err := point.Unmarshal(&dec, &p2); assert.NoError(t, err) {
			if assert.Equal(t, p.Label, p2.Label) && assert.Equal(t, p.Value, p2.Value) {
				tags := p2.Tags()
				{
					assert.Equal(t, []string{"country_code", "datacenter"}, tags.Keys())
					assert.Equal(t, []string{"RU", "EU"}, tags.Values())
				}
				fields := p2.Fields()
				{
					assert.Equal(t, []string{"cpu", "disk", "memory"}, fields.Keys())
					assert.Equal(t, []float64{0.42, 256, 1024}, fields.Values())
				}
			}
		}
	}
}

func TestSerializeLZ4(t *testing.T) {
	var (
		p = point.New("some_label", 42).WithTags(point.Tags{
			"country_code": "RU",
			"datacenter":   "EU",
		}).WithFields(point.Fields{
			"cpu":    0.42,
			"disk":   256,
			"memory": 1024,
		})
		buf = bytes.Buffer{}
		enc = binary.Encoder{
			Output: lz4.NewWriter(&buf),
		}
		dec = binary.Decoder{
			Input: lz4.NewReader(&buf),
		}
	)
	if err := point.Marshal(&enc, p); assert.NoError(t, err) {
		enc.Output.(*lz4.Writer).Flush()
		var p2 point.Point
		if err := point.Unmarshal(&dec, &p2); assert.NoError(t, err) {
			if assert.Equal(t, p.Label, p2.Label) && assert.Equal(t, p.Value, p2.Value) {
				tags := p2.Tags()
				{
					assert.Equal(t, []string{"country_code", "datacenter"}, tags.Keys())
					assert.Equal(t, []string{"RU", "EU"}, tags.Values())
				}
				fields := p2.Fields()
				{
					assert.Equal(t, []string{"cpu", "disk", "memory"}, fields.Keys())
					assert.Equal(t, []float64{0.42, 256, 1024}, fields.Values())
				}
			}
		}
	}
}

func TestSerializeSize(t *testing.T) {
	var (
		compressed   = bytes.Buffer{}
		uncompressed = bytes.Buffer{}
		encoder      = binary.Encoder{
			Output: &uncompressed,
		}
		lz4Writer  = lz4.NewWriter(&compressed)
		encoderLZ4 = binary.Encoder{
			Output: lz4Writer,
		}
	)
	var i float64
	for i = 0; i < 20; i++ {
		p := point.New("some_label", 42).WithTags(point.Tags{
			"country_code": "RU",
			"datacenter":   "EU",
		}).WithFields(point.Fields{
			"cpu":    0.42 * i,
			"disk":   256 * i,
			"memory": 1024 * i,
		})
		if err := point.Marshal(&encoder, p); !assert.NoError(t, err) {
			return
		}
		if err := point.Marshal(&encoderLZ4, p); !assert.NoError(t, err) {
			return
		}
	}
	if err := lz4Writer.Flush(); !assert.NoError(t, err) {
		return
	}
	t.Logf("uncompressed=%d, compressed=%d, rate=%.2f",
		uncompressed.Len(),
		compressed.Len(),
		float64(uncompressed.Len())/float64(compressed.Len()),
	)
}
