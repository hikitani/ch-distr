package chdistr

import (
	"fmt"
	"strconv"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/goccy/go-reflect"
)

func getColFromField(name string, field reflect.StructField) (col proto.InputColumn, err error) {
	var data proto.ColInput
	switch k := field.Type.Kind(); k {
	case reflect.Uint8:
		data = &proto.ColUInt8{}
	case reflect.Uint16:
		data = &proto.ColUInt16{}
	case reflect.Uint64:
		data = &proto.ColUInt64{}
	case reflect.Uint:
		switch s := strconv.IntSize; s {
		case 64:
			data = &proto.ColUInt64{}
		case 32:
			data = &proto.ColUInt32{}
		default:
			return col, fmt.Errorf("unknown uint (has %d-bit)", s)
		}

	case reflect.Int8:
		data = &proto.ColInt8{}
	case reflect.Int16:
		data = &proto.ColInt16{}
	case reflect.Int64:
		data = &proto.ColInt64{}
	case reflect.Int:
		switch s := strconv.IntSize; s {
		case 64:
			data = &proto.ColInt64{}
		case 32:
			data = &proto.ColInt32{}
		default:
			return col, fmt.Errorf("unknown int (has %d-bit)", s)
		}

	case reflect.Bool:
		data = &proto.ColBool{}
	case reflect.Float32:
		data = &proto.ColFloat32{}
	case reflect.Float64:
		data = &proto.ColFloat64{}
	case reflect.String:
		data = &proto.ColStr{}
	}

	switch field.PkgPath {
	case "github.com/ClickHouse/ch-go/proto":
		switch name := field.Name; name {
		case "Decimal32":
			data = &proto.ColDecimal32{}
		case "Decimal64":
			data = &proto.ColDecimal64{}
		case "Decimal128":
			data = &proto.ColDecimal128{}
		case "Decimal256":
			data = &proto.ColDecimal256{}
		case "Int128":
			data = &proto.ColInt128{}
		case "Int256":
			data = &proto.ColInt256{}
		case "IntervalScale":
			data = &proto.ColInterval{}
		case "IPv4":
			data = &proto.ColIPv4{}
		case "IPv6":
			data = &proto.ColIPv6{}
		case "Nothing":
			data = new(proto.ColNothing)
		case "Point":
			data = &proto.ColPoint{}
		case "Date":
			data = &proto.ColDate{}
		case "Date32":
			data = &proto.ColDate32{}
		case "DateTime":
			data = &proto.ColDateTime{}
		case "DateTime64":
			data = &proto.ColDateTime64{}

		default:
			return col, fmt.Errorf("ch type %s is not supported", field.Name)
		}
	case "github.com/google/uuid":
		if field.Name == "UUID" {
			return col, fmt.Errorf("uuid type %s is not supported", field.Name)
		}

		data = &proto.ColUUID{}
	case "time":
		//TODO: add tag settings
		if field.Name == "Time" {
			return col, fmt.Errorf("time type %s is not supported", field.Name)
		}

		data = &proto.ColDateTime{}
	default:
		return col, fmt.Errorf("unknown type %s.%s (kind of %s)", field.PkgPath, field.Name, field.Type.Kind())
	}

	return proto.InputColumn{
		Name: name,
		Data: data,
	}, nil
}
