package registry

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-redis/redis/v8"
)

var (
	_ registry.Watcher = (*watcher)(nil)
)

type watcher struct {
	key    string
	ticker *time.Ticker
	ctx    context.Context
	cancel context.CancelFunc
	client *redis.Client
}

func newWatcher(ctx context.Context, key string, client *redis.Client, ttl time.Duration) *watcher {
	w := &watcher{
		key:    key,
		ticker: time.NewTicker(ttl),
		client: client,
	}
	w.ctx, w.cancel = context.WithCancel(ctx)
	return w
}

func (w *watcher) Next() ([]*registry.ServiceInstance, error) {
	for {
		select {
		case <-w.ctx.Done():
			return nil, w.ctx.Err()
		case <-w.ticker.C:
		}
		return services(w.ctx, w.client, w.key)
	}
}

func (w *watcher) Stop() error {
	w.ticker.Stop()
	w.cancel()

	return nil
}
