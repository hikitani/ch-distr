package chdistr

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/goccy/go-reflect"

	"github.com/ClickHouse/ch-go/proto"
)

var (
	ErrInvalidType      = errors.New("got invalid type")
	ErrGotNotStructType = errors.New("expected type struct")
)

type batch[T any] struct {
	input      proto.Input
	appenders  []func(v any, input proto.Input)
	structInfo []reflect.StructField
}

func (b *batch[T]) append(v T) {
	for i, field := range fieldsToSlice(reflect.ValueOf(v), b.structInfo) {
		b.appenders[i](field, b.input)
	}
}

func newBatch[T any]() (*batch[T], error) {
	var v T
	refVal := reflect.Indirect(reflect.ValueOf(v))

	var (
		appenders  []func(v any, input proto.Input)
		input      proto.Input
		structInfo []reflect.StructField
	)
	switch refVal.Kind() {
	case reflect.Struct:
		structInfo = getStructInfo(refVal)
		for i, field := range structInfo {
			if fieldIsPrivate(field) {
				continue
			}

			var name string
			if tagVal, ok := field.Tag.Lookup("ch"); ok && tagVal == "-" {
				continue
			} else if !ok {
				name = toUnderScore(field.Name)
			} else {
				name = tagVal
			}

			typKind := field.Type.Kind()

			col, err := getColFromTypeInfo(name, typKind)
			if err != nil {
				return nil, fmt.Errorf("get input column: %s", err)
			}
			input = append(input, col)

			appender, err := getAppenderToColFromTypeInfo(i, field)
			if err != nil {
				return nil, fmt.Errorf("get appender: %s", err)
			}
			appenders = append(appenders, appender)
		}
	case reflect.Invalid:
		return nil, ErrInvalidType
	default:
		return nil, ErrGotNotStructType
	}

	return &batch[T]{
		input:     input,
		appenders: appenders,
	}, nil
}

func getColFromTypeInfo(name string, kind reflect.Kind) (col proto.InputColumn, _ error) {
	switch kind {
	case reflect.Uint8:
		col = proto.InputColumn{
			Name: name,
			Data: &proto.ColUInt8{},
		}
	}

	return col, fmt.Errorf("")
}

func getAppenderToColFromTypeInfo(idx int, field reflect.StructField) (fn func(v any, input proto.Input), _ error) {
	switch field.Type.Kind() {
	case reflect.Uint8:
		fn = func(v any, input proto.Input) {
			input[idx].Data.(proto.ColumnOf[uint8]).Append(v.(uint8))
		}
	}

	return nil, fmt.Errorf("")
}

func getStructInfo(v reflect.Value) []reflect.StructField {
	info := make([]reflect.StructField, 0, v.NumField())
	typeInfo := v.Type()
	for i := 0; i < v.NumField(); i++ {
		info = append(info, typeInfo.Field(i))
	}
	return info
}

func fieldsToSlice(v reflect.Value, structInfo []reflect.StructField) []any {
	sample := make([]any, 0, len(structInfo))

	for i := 0; i < len(structInfo); i++ {
		field := structInfo[i]
		if fieldIsPrivate(field) {
			continue
		}

		if tagVal, ok := field.Tag.Lookup("ch"); ok && tagVal == "-" {
			continue
		}

		sample = append(sample, v.Field(i).Interface())
	}

	return sample
}

func fieldIsPrivate(field reflect.StructField) bool {
	return field.Name[0] >= 'a' && field.Name[0] <= 'z'
}

// https://gist.github.com/zxh/cee082053aa9674812e8cd4387088301
// Camelcase to underscore style.
func toUnderScore(name string) string {
	l := len(name)
	ss := strings.Split(name, "")

	// we just care about the key of idx map,
	// the value of map is meaningless
	idx := make(map[int]int, 1)

	var rs []rune
	for _, s := range name {
		rs = append(rs, []rune(string(s))...)
	}

	for i := l - 1; i >= 0; {
		if unicode.IsUpper(rs[i]) {
			var start, end int
			end = i
			j := i - 1
			for ; j >= 0; j-- {
				if unicode.IsLower(rs[j]) {
					start = j + 1
					break
				}
			}
			// handle the case: "BBC" or "AaBBB" case
			if end == l-1 {
				idx[start] = 1
			} else {
				if start == end {
					// value=1 is meaningless
					idx[start] = 1
				} else {
					idx[start] = 1
					idx[end] = 1
				}
			}
			i = j - 1
		} else {
			i--
		}
	}

	for i := l - 1; i >= 0; i-- {
		ss[i] = strings.ToLower(ss[i])
		if _, ok := idx[i]; ok && i != 0 {
			ss = append(ss[0:i], append([]string{"_"}, ss[i:]...)...)
		}
	}

	return strings.Join(ss, "")
}
