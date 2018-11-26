package point

import (
	"sort"

	"github.com/ClickHouse-Ninja/ninjato/src/binary"
)

// Marshal point.
func Marshal(encoder *binary.Encoder, point *Point) (err error) {
	if err = encoder.String(point.Label); err != nil {
		return err
	}
	if err = encoder.Float64(point.Value); err != nil {
		return err
	}
	if err = encoder.Int32(point.timestamp); err != nil {
		return err
	}
	if err = encoder.Uint8(uint8(len(point.tags.keys))); err != nil {
		return err
	}
	if len(point.tags.keys) > 1 {
		sort.Sort(point.tags)
	}
	if len(point.fields.keys) > 1 {
		sort.Sort(point.fields)
	}
	for _, key := range point.tags.keys {
		if err = encoder.String(key); err != nil {
			return err
		}
	}
	for _, value := range point.tags.values {
		if err = encoder.String(value); err != nil {
			return err
		}
	}
	if err = encoder.Uint8(uint8(len(point.fields.keys))); err != nil {
		return err
	}
	for _, key := range point.fields.keys {
		if err = encoder.String(key); err != nil {
			return err
		}
	}
	for _, value := range point.fields.values {
		if err = encoder.Float64(value); err != nil {
			return err
		}
	}
	if err = encoder.Uint8(point.magicNumber); err != nil {
		return err
	}
	return nil
}

// Unmarshal point.
func Unmarshal(decoder *binary.Decoder, point *Point) (err error) {
	if point.Label, err = decoder.String(); err != nil {
		return err
	}
	if point.Value, err = decoder.Float64(); err != nil {
		return err
	}
	if point.timestamp, err = decoder.Int32(); err != nil {
		return err
	}
	var ln uint8
	if ln, err = decoder.UInt8(); err != nil {
		return err
	}
	if ln != 0 {
		point.tags.keys = make([]string, 0, ln)
		point.tags.values = make([]string, 0, ln)
		for i := 0; i < int(ln); i++ {
			var key string
			if key, err = decoder.String(); err != nil {
				return err
			}
			point.tags.keys = append(point.tags.keys, key)
		}
		for i := 0; i < int(ln); i++ {
			var value string
			if value, err = decoder.String(); err != nil {
				return err
			}
			point.tags.values = append(point.tags.values, value)
		}
		sort.Sort(point.tags)
	}
	if ln, err = decoder.UInt8(); err != nil {
		return err
	}
	if ln != 0 {
		point.fields.keys = make([]string, 0, ln)
		point.fields.values = make([]float64, 0, ln)
		for i := 0; i < int(ln); i++ {
			var key string
			if key, err = decoder.String(); err != nil {
				return err
			}
			point.fields.keys = append(point.fields.keys, key)
		}
		for i := 0; i < int(ln); i++ {
			var value float64
			if value, err = decoder.Float64(); err != nil {
				return err
			}
			point.fields.values = append(point.fields.values, value)
		}
		sort.Sort(point.fields)
	}
	if point.magicNumber, err = decoder.UInt8(); err != nil {
		return err
	}
	return nil
}
