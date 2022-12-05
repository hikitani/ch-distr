package chdistr

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"go.uber.org/multierr"
)

type shard[T any] struct {
	client *chpool.Pool
	host   Host
	pool   batchPool[T]
}

func (s *shard[T]) start(
	ctx context.Context,
	flushInterval time.Duration,
	table string,
	data <-chan T,
	sharedBatches chan *batch[T],
	stch chan<- Host,
) (err error) {
	if flushInterval == 0 {
		return errors.New("flush interval must be greater than zero")
	}

	if cap(stch) == 0 {
		return errors.New("stch channel must be buffered")
	}

	errs := make(chan error)

	t := time.NewTicker(flushInterval)
	defer t.Stop()

	select {
	case stch <- s.host.SetState(HostUp):
	case <-ctx.Done():
		err = multierr.Append(err, ctx.Err())
		return
	}

	defer func() {
		if errors.Is(err, context.Canceled) {
			return
		}

		select {
		case stch <- s.host.SetState(HostDown):
		case <-ctx.Done():
			err = multierr.Append(err, ctx.Err())
		}
	}()

	b, err := s.pool.get()
	if err != nil {
		return fmt.Errorf("get batch from pool: %w", err)
	}

	var wg int32
	execQuery := func(b *batch[T]) {
		atomic.AddInt32(&wg, 1)
		go func() {
			defer atomic.AddInt32(&wg, -1)
			defer s.pool.put(b)

			err := s.client.Do(ctx, ch.Query{
				Body:  b.input.Into(table),
				Input: b.input,
			})

			if err != nil {
				if errors.Is(err, context.Canceled) {
					err = nil
				}

				select {
				case sharedBatches <- b:
				case <-time.After(flushInterval / 2):
				}

				errs <- err
				return
			}
		}()
	}

loop:
	for {
		select {
		case v := <-data:
			b.append(v)
		case sharedBatch := <-sharedBatches:
			execQuery(sharedBatch)
		case <-t.C:
			execQuery(b)

			b, err = s.pool.get()
			if err != nil {
				err = fmt.Errorf("get batch from pool: %w", err)
				break loop
			}
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		case err = <-errs:
			break loop
		}
	}

	for atomic.LoadInt32(&wg) != 0 {
		err = multierr.Append(err, <-errs)
	}

	return err
}

func (s *shard[T]) close() error {
	s.client.Close()

	return nil
}

func newShard[T any](ctx context.Context, host Host, opt ch.Options) (*shard[T], error) {
	client, err := chpool.Dial(ctx, chpool.Options{
		ClientOptions: opt,
		MinConns:      1,
		MaxConns:      4,
	})
	if err != nil {
		return nil, fmt.Errorf("ch dial: %w", err)
	}

	// Generic checking
	if _, err := newBatch[T](); err != nil {
		return nil, fmt.Errorf("batch init: %w", err)
	}

	return &shard[T]{
		pool:   make(batchPool[T], 4),
		client: client,
		host:   host,
	}, nil
}

type batchPool[T any] chan *batch[T]

func (p batchPool[T]) get() (*batch[T], error) {
	select {
	case b := <-p:
		return b, nil
	default:
		return newBatch[T]()
	}
}

func (p batchPool[T]) put(b *batch[T]) {
	select {
	case p <- b:
		return
	default:
		return
	}
}
