package chdistr

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/goccy/go-reflect"
	"github.com/google/uuid"
)

type appender func(v any, input proto.Input)

func newAppender[T any](idx int) appender {
	return func(v any, input proto.Input) {
		input[idx].Data.(proto.ColumnOf[T]).Append(v.(T))
	}
}

func newUIntAppender[T uint32 | uint64](idx int) appender {
	return func(v any, input proto.Input) {
		input[idx].Data.(proto.ColumnOf[T]).Append(T(v.(uint)))
	}
}

func newIntAppender[T int32 | int64](idx int) appender {
	return func(v any, input proto.Input) {
		input[idx].Data.(proto.ColumnOf[T]).Append(T(v.(int)))
	}
}

type chDates interface {
	proto.Date | proto.Date32 | proto.DateTime | proto.DateTime64
}

func newDateAppender[T chDates](idx int, converter func(t T) time.Time) appender {
	return func(v any, input proto.Input) {
		input[idx].Data.(proto.ColumnOf[time.Time]).Append(converter(v.(T)))
	}
}

func dateToTime[T interface{ Time() time.Time }](t T) time.Time {
	return t.Time()
}

func precisionDateToTime[T interface {
	Time(p proto.Precision) time.Time
}](t T) time.Time {
	return t.Time(proto.PrecisionMax)
}

func getColAndAppenderFromField(name string, idx int, field reflect.StructField) (col proto.InputColumn, fn appender, err error) {
	var data proto.ColInput
	typ := field.Type
	switch k := typ.Kind(); k {
	case reflect.Uint8:
		data = &proto.ColUInt8{}
		fn = newAppender[uint8](idx)
	case reflect.Uint16:
		data = &proto.ColUInt16{}
		fn = newAppender[uint16](idx)
	case reflect.Uint32:
		data = &proto.ColUInt32{}
		fn = newAppender[uint32](idx)
	case reflect.Uint64:
		data = &proto.ColUInt64{}
		fn = newAppender[uint64](idx)
	case reflect.Uint:
		switch s := strconv.IntSize; s {
		case 64:
			data = &proto.ColUInt64{}
			fn = newUIntAppender[uint64](idx)
		case 32:
			data = &proto.ColUInt32{}
			fn = newUIntAppender[uint32](idx)
		default:
			return col, nil, fmt.Errorf("unknown uint (has %d-bit)", s)
		}

	case reflect.Int8:
		data = &proto.ColInt8{}
		fn = newAppender[int8](idx)
	case reflect.Int16:
		data = &proto.ColInt16{}
		fn = newAppender[int16](idx)
	case reflect.Int32:
		data = &proto.ColInt32{}
		fn = newAppender[int32](idx)
	case reflect.Int64:
		data = &proto.ColInt64{}
		fn = newAppender[int64](idx)
	case reflect.Int:
		switch s := strconv.IntSize; s {
		case 64:
			data = &proto.ColInt64{}
			fn = newIntAppender[int64](idx)
		case 32:
			data = &proto.ColInt32{}
			fn = newIntAppender[int32](idx)
		default:
			return col, nil, fmt.Errorf("unknown int (has %d-bit)", s)
		}

	case reflect.Bool:
		data = &proto.ColBool{}
		fn = newAppender[bool](idx)
	case reflect.Float32:
		data = &proto.ColFloat32{}
		fn = newAppender[float32](idx)
	case reflect.Float64:
		data = &proto.ColFloat64{}
		fn = newAppender[float64](idx)
	case reflect.String:
		data = &proto.ColStr{}
		fn = newAppender[string](idx)
	}

	switch typ.PkgPath() {
	case "github.com/ClickHouse/ch-go/proto":
		switch name := typ.Name(); name {
		case "Decimal32":
			data = &proto.ColDecimal32{}
			fn = newAppender[proto.Decimal32](idx)
		case "Decimal64":
			data = &proto.ColDecimal64{}
			fn = newAppender[proto.Decimal64](idx)
		case "Decimal128":
			data = &proto.ColDecimal128{}
			fn = newAppender[proto.Decimal128](idx)
		case "Decimal256":
			data = &proto.ColDecimal256{}
			fn = newAppender[proto.Decimal256](idx)
		case "UInt128":
			data = &proto.ColUInt128{}
			fn = newAppender[proto.UInt128](idx)
		case "UInt256":
			data = &proto.ColUInt256{}
			fn = newAppender[proto.UInt256](idx)
		case "Int128":
			data = &proto.ColInt128{}
			fn = newAppender[proto.Int128](idx)
		case "Int256":
			data = &proto.ColInt256{}
			fn = newAppender[proto.Int256](idx)
		case "Interval":
			data = &proto.ColInterval{}
			fn = newAppender[proto.Interval](idx)
		case "IPv4":
			data = &proto.ColIPv4{}
			fn = newAppender[proto.IPv4](idx)
		case "IPv6":
			data = &proto.ColIPv6{}
			fn = newAppender[proto.IPv6](idx)
		case "Nothing":
			data = new(proto.ColNothing)
			fn = newAppender[proto.Nothing](idx)
		case "Point":
			data = &proto.ColPoint{}
			fn = newAppender[proto.Point](idx)
		case "Date":
			data = &proto.ColDate{}
			fn = newDateAppender(idx, dateToTime[proto.Date])
		case "Date32":
			data = &proto.ColDate32{}
			fn = newDateAppender(idx, dateToTime[proto.Date32])
		case "DateTime":
			data = &proto.ColDateTime{}
			fn = newDateAppender(idx, dateToTime[proto.DateTime])
		case "DateTime64":
			data = &proto.ColDateTime64{
				Precision:    proto.PrecisionMax,
				PrecisionSet: true,
			}
			fn = newDateAppender(idx, precisionDateToTime[proto.DateTime64])

		default:
			return col, nil, fmt.Errorf("field %s: ch type %s is not supported", field.Name, name)
		}
	case "github.com/google/uuid":
		if name := typ.Name(); name != "UUID" {
			return col, nil, fmt.Errorf("field %s: uuid type %s is not supported", field.Name, name)
		}

		data = &proto.ColUUID{}
		fn = newAppender[uuid.UUID](idx)
	case "time":
		//TODO: add tag settings
		if name := typ.Name(); name != "Time" {
			return col, nil, fmt.Errorf("field %s: time type %s is not supported", field.Name, name)
		}

		data = &proto.ColDateTime{}
		fn = newAppender[time.Time](idx)
	}

	if data == nil || fn == nil {
		return col, nil, fmt.Errorf("field %s: unknown type %s.%s (kind of %s)",
			field.Name, typ.PkgPath(), typ.Name(), typ.Kind())
	}

	return proto.InputColumn{
		Name: name,
		Data: data,
	}, fn, nil
}
