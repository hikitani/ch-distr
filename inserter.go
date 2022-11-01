package chdistr

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/alphadose/haxmap"
	"golang.org/x/sync/errgroup"
)

type Options[H Host] struct {
	Host        H              // 127.0.0.1:9000
	User        string         // "default"
	Password    string         // blank string by default
	QuotaKey    string         // blank string by default
	Compression ch.Compression // disabled by default
	Settings    []ch.Setting   // none by default

	Dialer      ch.Dialer     // defaults to net.Dialer
	DialTimeout time.Duration // defaults to 1s
	TLS         *tls.Config   // no TLS is used by default
}

type GlobalOptions struct {
	Database    string         // "default"
	User        string         // "default"
	Password    string         // blank string by default
	QuotaKey    string         // blank string by default
	Compression ch.Compression // disabled by default
	Settings    []ch.Setting   // none by default

	Dialer      ch.Dialer     // defaults to net.Dialer
	DialTimeout time.Duration // defaults to 1s
	TLS         *tls.Config   // no TLS is used by default
}

type ClusterOptions[H Host] struct {
	Hosts  []Options[H]
	Global GlobalOptions
}

type shardWithChan[T any] struct {
	shard *shard[T]
	data  chan T
}

type DistrInserter[T any, H Host] struct {
	cluster  ClusterOptions[H]
	shards   *haxmap.Map[string, shardWithChan[T]]
	selector HostSelector[H]

	flushInterval    time.Duration
	reconnectTimeout time.Duration
	pushTimeout      time.Duration
	maxPushAttempts  int
	ShardErrHandler  func(err error)
}

func makeCHOpts[H Host](global GlobalOptions, options Options[H]) ch.Options {
	chOpts := ch.Options{
		Database:    global.Database,
		Password:    global.Password,
		User:        global.User,
		QuotaKey:    global.QuotaKey,
		Compression: global.Compression,
		Settings:    global.Settings,
		Dialer:      global.Dialer,
		DialTimeout: global.DialTimeout,
		TLS:         global.TLS,
	}

	hinfo := options.Host.Info()
	chOpts.Address = hinfo.Address

	if hinfo.Database != "" {
		chOpts.Database = hinfo.Database
	}

	if options.Password != "" {
		chOpts.Password = options.Password
	}

	if options.User != "" {
		chOpts.User = options.User
	}

	if options.QuotaKey != "" {
		chOpts.QuotaKey = options.QuotaKey
	}

	if options.Compression != 0 {
		chOpts.Compression = options.Compression
	}

	if len(options.Settings) != 0 {
		chOpts.Settings = options.Settings
	}

	if options.Dialer != nil {
		chOpts.Dialer = options.Dialer
	}

	if options.DialTimeout != 0 {
		chOpts.DialTimeout = options.DialTimeout
	}

	if options.TLS != nil {
		chOpts.TLS = options.TLS
	}

	return chOpts
}

func (ins *DistrInserter[T, H]) Start(ctx context.Context, table string) error {
	stch := make(chan Host, len(ins.cluster.Hosts))
	defer close(stch)

	sharedBatches := make(chan *batch[T])
	defer close(sharedBatches)

	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return ListenStates[H](ctx, ins.selector, stch)
	})

	for _, nodeOpt := range ins.cluster.Hosts {
		host := nodeOpt.Host
		sh, err := newShard[T](ctx, host, makeCHOpts(ins.cluster.Global, nodeOpt))
		if err != nil {
			return fmt.Errorf("create shard for host %s: %w", host.Info(), err)
		}
		defer sh.close()

		data := make(chan T, 1)

		ins.shards.Set(host.Info().ID(), struct {
			shard *shard[T]
			data  chan T
		}{
			shard: sh,
			data:  data,
		})

		errg.Go(func() error {
			for {
				err := sh.start(ctx, ins.flushInterval, table, data, sharedBatches, stch)
				if errors.Is(err, context.Canceled) {
					return err
				}

				if errors.Is(err, context.DeadlineExceeded) {
					return err
				}

				if ins.ShardErrHandler != nil {
					ins.ShardErrHandler(err)
				}

				time.Sleep(ins.reconnectTimeout)
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
		})
	}

	return errg.Wait()
}

func (ins *DistrInserter[T, H]) Push(ctx context.Context, v T) error {
	for {
		h := ins.selector.Pick()
		shinfo, ok := ins.shards.Get(h.ID())
		if !ok {
			continue
		}

		select {
		case shinfo.data <- v:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			select {
			case shinfo.data <- v:
				return nil
			case <-time.After(ins.pushTimeout):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func NewInserter[T any, H Host](cluster ClusterOptions[H], selector HostSelector[H]) (*DistrInserter[T, H], error) {
	if len(cluster.Hosts) == 0 {
		return nil, errors.New("add options of hosts")
	}

	for _, h := range cluster.Hosts {
		if err := selector.AddHost(h.Host); err != nil {
			return nil, fmt.Errorf("add host %s: %w", h.Host.Info(), err)
		}
	}

	return &DistrInserter[T, H]{
		cluster:          cluster,
		selector:         selector,
		shards:           haxmap.New[string, shardWithChan[T]](),
		flushInterval:    5 * time.Second,
		reconnectTimeout: 2 * time.Second,
		pushTimeout:      5 * time.Millisecond,
		maxPushAttempts:  5,
	}, nil
}
