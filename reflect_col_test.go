package chdistr

import (
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
)

func TestDateConverter(t *testing.T) {
	tdate := time.Date(2022, time.October, 12, 0, 0, 0, 0, time.UTC)

	date := proto.ToDate(tdate)
	assert.Equal(t, tdate, dateToTime(date))
}

func TestDate32Converter(t *testing.T) {
	tdate := time.Date(2022, time.October, 12, 0, 0, 0, 0, time.UTC)

	date := proto.ToDate32(tdate)
	assert.Equal(t, tdate, dateToTime(date))
}

func TestDateTimeConverter(t *testing.T) {
	tdate := time.Date(2022, time.October, 12, 6, 30, 45, 0, time.Local)

	date := proto.ToDateTime(tdate)
	assert.Equal(t, tdate, dateToTime(date))
}

func TestDateTime64Converter(t *testing.T) {
	tdate := time.Date(2022, time.October, 12, 6, 30, 45, 123456789, time.Local)

	date := proto.ToDateTime64(tdate, proto.PrecisionMax)
	assert.Equal(t, tdate, precisionDateToTime(date))
}
