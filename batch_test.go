package chdistr

import (
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/goccy/go-reflect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestFieldsToSlice(t *testing.T) {
	foo := struct {
		F1 int
		f2 string
		F3 bool
		F4 float32 `ch:"-"`
	}{
		F1: 11,
		f2: "f2",
		F3: true,
		F4: 1.01,
	}

	v := reflect.ValueOf(foo)

	res := fieldsToSlice(v, getStructInfo(v))
	assert.Equal(t, []any{11, true}, res)
}

type testFoo struct {
	UI   uint
	UI8  uint8
	UI16 uint16
	UI32 uint32
	UI64 uint64
	U128 proto.UInt128
	U256 proto.UInt256

	I    int
	I8   int8
	I16  int16
	I32  int32
	I64  int64
	I128 proto.Int128
	I256 proto.Int256

	B   bool
	F32 float32
	F64 float64
	S   string

	D32  proto.Decimal32
	D64  proto.Decimal64
	D128 proto.Decimal128
	D256 proto.Decimal256

	IT  proto.Interval
	IP4 proto.IPv4
	IP6 proto.IPv6
	N   proto.Nothing
	P   proto.Point

	D    proto.Date
	DT32 proto.Date32
	DT   proto.DateTime
	DT64 proto.DateTime64

	ID uuid.UUID
	T  time.Time
}

func TestBatch(t *testing.T) {
	b, err := newBatch[testFoo]()
	assert.Nil(t, err)

	b.append(testFoo{})
	for _, inp := range b.input {
		assert.Equal(t, 1, inp.Data.Rows())
	}
}

func BenchmarkBatchAppend(b *testing.B) {
	bt, err := newBatch[testFoo]()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bt.append(testFoo{})
	}
}
