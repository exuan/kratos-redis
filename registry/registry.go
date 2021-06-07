package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
)

var (
	_ registry.Registrar = (*Registry)(nil)
	_ registry.Discovery = (*Registry)(nil)
)

const (
	keyFormat     = "%s/%s/%s"
	watcherFormat = "%s/%s"
	defaultScan   = 20
	defaultTTL    = time.Minute
)

type (
	Option func(o *options)

	options struct {
		ctx        context.Context
		namespace  string
		ttl        time.Duration
		watcherTtl time.Duration
	}

	Registry struct {
		opts   *options
		client *redis.Client
		ticker *time.Ticker
		cancel context.CancelFunc
		ctx    context.Context
	}
)

func Context(ctx context.Context) Option {
	return func(o *options) { o.ctx = ctx }
}

func Namespace(ns string) Option {
	return func(o *options) { o.namespace = ns }
}

func TTL(ttl time.Duration) Option {
	return func(o *options) { o.ttl = ttl }
}
func WatcherTTL(ttl time.Duration) Option {
	return func(o *options) { o.watcherTtl = ttl }
}

func New(client *redis.Client, opts ...Option) *Registry {
	options := &options{
		ctx:        context.Background(),
		namespace:  "/microservices",
		ttl:        defaultTTL,
		watcherTtl: defaultTTL,
	}
	for _, o := range opts {
		o(options)
	}
	r := &Registry{
		client: client,
		opts:   options,
		ticker: time.NewTicker(options.ttl),
	}

	r.ctx, r.cancel = context.WithCancel(options.ctx)
	return r
}

func (r *Registry) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	return services(ctx, r.client, serviceName)
}

func (r *Registry) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	key := fmt.Sprintf(watcherFormat, r.opts.namespace, serviceName)
	return newWatcher(ctx, key, r.client, r.opts.watcherTtl), nil
}

func (r *Registry) Register(ctx context.Context, service *registry.ServiceInstance) error {
	key := fmt.Sprintf(keyFormat, r.opts.namespace, service.Name, service.ID)
	value, err := jsoniter.MarshalToString(service)
	if err != nil {
		return err
	}

	if err := r.register(ctx, key, value, r.opts.ttl); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			case _, ok := <-r.ticker.C:
				if !ok {
					return
				}
				r.register(ctx, key, value, r.opts.ttl)
			}
		}
	}()

	return nil
}

func (r *Registry) register(ctx context.Context, key string, value string, ttl time.Duration) error {
	res, err := r.client.TTL(ctx, key).Result()
	if err != nil {
		return err
	}

	ttl = ttl + 2*time.Second
	if res > 1 {
		err := r.client.Expire(ctx, key, ttl).Err()
		return err
	}
	return r.client.Set(ctx, key, value, ttl).Err()
}

func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	r.ticker.Stop()
	r.cancel()
	key := fmt.Sprintf(keyFormat, r.opts.namespace, service.Name, service.ID)
	return r.client.Del(ctx, key).Err()
}

func services(ctx context.Context, client *redis.Client, key string) ([]*registry.ServiceInstance, error) {
	key = key + "*"
	var cursor uint64
	items := make([]*registry.ServiceInstance, 0)

	for {
		var keys []string
		var err error
		keys, cursor, err = client.Scan(ctx, cursor, key, defaultScan).Result()
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			break
		}
		res, err := client.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, err
		}

		for _, v := range res {
			switch str := v.(type) {
			case string:
				si := new(registry.ServiceInstance)
				if err := jsoniter.UnmarshalFromString(str, si); err != nil {
					return nil, err
				}
				items = append(items, si)
			}
		}
		if cursor == 0 {
			break
		}
	}

	return items, nil
}
