package chdistr

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestErrg(t *testing.T) {
	errg, ctx := errgroup.WithContext(context.Background())

	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second))
	t.Cleanup(cancel)
	errg.Go(func() error {
		<-ctx.Done()

		return ctx.Err()
	})

	errg.Wait()
}

func TestInserter(t *testing.T) {
	// ins, err := NewInserter[testStruct, HostInfo](ClusterOptions[HostInfo]{
	// 	Hosts: []Options[HostInfo]{
	// 		{
	// 			Host: NewHostInfo("127.0.0.1:9000", "default"),
	// 		},
	// 		{
	// 			Host: NewHostInfo("127.0.0.1:9000", "test1"),
	// 		},
	// 	},
	// }, RoundRobinSelector())
	ins, err := NewInserter[testStruct, WeightHostInfo](ClusterOptions[WeightHostInfo]{
		Hosts: []Options[WeightHostInfo]{
			{
				Host: NewWeightHostInfo("127.0.0.1:9000", "default", 4),
			},
			{
				Host: NewWeightHostInfo("127.0.0.1:9000", "test1", 1),
			},
		},
	}, WeightRoundRobinSelector())
	if err != nil {
		t.Fatalf("new inserter: %s", err)
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()

	errg, ctx := errgroup.WithContext(ctx)
	conn := getTestConn(ctx, t)

	errg.Go(func() error {
		if err := ins.Start(ctx, "table_insert"); err != nil {
			return fmt.Errorf("inserter start: %w", err)
		}

		return nil
	})

	errg.Go(func() error {
		time.Sleep(3 * time.Second)
		for i := 0; i < 1000; i++ {
			err := ins.Push(ctx, testStruct{
				Ts:   time.Now(),
				Ts6:  proto.ToDateTime64(time.Now(), proto.PrecisionMicro),
				Foo:  randstr(10),
				Bar:  uint8(rand.Uint32()),
				Long: proto.UInt256FromUInt64(rand.Uint64()),
			})
			if err != nil {
				return fmt.Errorf("push: %w", err)
			}
		}

		return nil
	})

	if err := errg.Wait(); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

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
