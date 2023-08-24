package redis

import (
	"context"
	"errors"
	"time"

	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/redis/go-redis/v9"
)

type Cache struct {
	client redis.Cmdable
}

func NewCache(client redis.Cmdable) *Cache {
	return &Cache{client: client}
}

func (c *Cache) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	return c.client.Set(ctx, key, val, expiration).Err()
}

func (c *Cache) Get(ctx context.Context, key string) (val ecache.Value) {
	val.AnyValue.Val, val.AnyValue.Err = c.client.Get(ctx, key).Result()
	if val.AnyValue.Err != nil && errors.Is(val.AnyValue.Err, redis.Nil) {
		val.AnyValue.Err = errs.ErrKeyNotExist
	}
	return
}
