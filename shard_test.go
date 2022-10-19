package chdistr

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

type testStruct struct {
	Ts   time.Time
	Ts6  proto.DateTime64
	Foo  string
	Bar  uint8
	Long proto.UInt256
}

func getTestConn(ctx context.Context, t testing.TB) *ch.Client {
	const (
		ddlCreate = `CREATE TABLE IF NOT EXISTS default.table_insert
	(
		ts   DateTime,
		ts6  DateTime64(6),
		foo  String,
		bar  UInt8,
		long UInt256
	)
	ENGINE = MergeTree
	ORDER BY ts`
		ddlDrop = "DROP TABLE IF EXISTS default.table_insert"
	)

	conn, err := ch.Dial(ctx, ch.Options{
		Address: "127.0.0.1:9000",
	})
	if err != nil {
		t.Fatal("ch dial: ", err)
	}
	t.Cleanup(func() { conn.Close() })

	err = conn.Do(ctx, ch.Query{
		Body: ddlCreate,
	})
	if err != nil {
		t.Fatal("create table: ", err)
	}
	t.Cleanup(func() {
		conn.Do(ctx, ch.Query{
			Body: ddlDrop,
		})
	})

	return conn
}

func TestShard(t *testing.T) {
	ctx := context.Background()
	conn := getTestConn(ctx, t)

	sh, err := newShard[testStruct](ctx, NewHostInfo("localhost"), ch.Options{
		Address: "127.0.0.1:9000",
	})
	if err != nil {
		t.Fatal("create shard: ", err)
	}
	t.Cleanup(func() { sh.close() })

	ctx, cancel := context.WithCancel(ctx)
	datach := make(chan testStruct)
	sharedch := make(chan *batch[testStruct])
	stch := make(chan<- Node, 1)

	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		flushInterval := 100 * time.Millisecond
		return sh.start(ctx, flushInterval, "table_insert", datach, sharedch, stch)
	})

	go func() {
		for i := 0; i < 1000; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				datach <- testStruct{
					Ts:   time.Now(),
					Ts6:  proto.ToDateTime64(time.Now(), proto.PrecisionMicro),
					Foo:  randstr(10),
					Bar:  uint8(rand.Uint32()),
					Long: proto.UInt256FromUInt64(rand.Uint64()),
				}
			}
		}
	}()

	time.AfterFunc(200*time.Millisecond, func() {
		cancel()
	})

	if err := errg.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			t.Fatal("shard executing err: ", err)
		}
	}

	ctx = context.Background()
	var data proto.ColDateTime
	err = conn.Do(ctx, ch.Query{
		Body: "SELECT ts FROM default.table_insert",
		Result: proto.Results{
			{Name: "ts", Data: &data},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			t.Log(block.Rows)
			return nil
		},
	})
	if err != nil {
		t.Fatal("select from table_insert: ", err)
	}

	assert.Equal(t, 1000, data.Rows())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randstr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
