package point

import (
	"github.com/ClickHouse-Ninja/ninjato/src/atime"
)

const magicNumber = 146

func New(label string, value float64) *Point {
	return &Point{
		Label:       label,
		Value:       value,
		timestamp:   int32(atime.Now().Unix()),
		magicNumber: magicNumber,
	}
}

type (
	Tags   = map[string]string
	Fields = map[string]float64
)

type Point struct {
	Label       string
	Value       float64
	tags        TagsPair
	fields      FieldsPair
	timestamp   int32
	magicNumber uint8
}

func (p *Point) WithTags(tags Tags) *Point {
	if cap(p.tags.keys) == 0 {
		p.tags.keys = make([]string, 0, len(tags))
		p.tags.values = make([]string, 0, len(tags))
	}
	for k, v := range tags {
		p.tags.keys = append(p.tags.keys, k)
		p.tags.values = append(p.tags.values, v)
	}
	return p
}

func (p *Point) WithFields(fields Fields) *Point {
	if cap(p.fields.keys) == 0 {
		p.fields.keys = make([]string, 0, len(fields))
		p.fields.values = make([]float64, 0, len(fields))
	}
	for k, v := range fields {
		p.fields.keys = append(p.fields.keys, k)
		p.fields.values = append(p.fields.values, v)
	}
	return p
}

func (p *Point) Timestamp() int32 {
	return p.timestamp
}

func (p *Point) IsValid() bool {
	return p.magicNumber == magicNumber && p.timestamp != 0
}

func (p *Point) Tags() *TagsPair {
	return &p.tags
}

func (p *Point) Fields() *FieldsPair {
	return &p.fields
}

type TagsPair struct {
	keys   []string
	values []string
}

func (t TagsPair) Len() int           { return len(t.keys) }
func (t TagsPair) Less(i, j int) bool { return t.keys[i] < t.keys[j] }
func (t TagsPair) Swap(i, j int) {
	t.keys[i], t.keys[j] = t.keys[j], t.keys[i]
	t.values[i], t.values[j] = t.values[j], t.values[i]
}

func (t *TagsPair) Keys() []string {
	return t.keys
}

func (t *TagsPair) Values() []string {
	return t.values
}

type FieldsPair struct {
	keys   []string
	values []float64
}

func (f FieldsPair) Len() int           { return len(f.keys) }
func (f FieldsPair) Less(i, j int) bool { return f.keys[i] < f.keys[j] }
func (f FieldsPair) Swap(i, j int) {
	f.keys[i], f.keys[j] = f.keys[j], f.keys[i]
	f.values[i], f.values[j] = f.values[j], f.values[i]
}

func (f *FieldsPair) Keys() []string {
	return f.keys
}

func (f *FieldsPair) Values() []float64 {
	return f.values
}
