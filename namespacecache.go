package ecache

import (
	"context"
	"github.com/ecodeclub/ecache/memory/lru"
	"time"
)

type NamespaceCache struct {
	c         Cache
	namespace string
}

func NewNamespaceCacheForLru(c *lru.Cache, namespace string) *NamespaceCache {
	return &NamespaceCache{
		c:         c,
		namespace: namespace,
	}
}

func NewNamespaceCacheForRedis(c Cache, namespace string) *NamespaceCache {
	return &NamespaceCache{
		c:         c,
		namespace: namespace,
	}
}

func (c *NamespaceCache) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	return c.c.Set(ctx, c.namespace+key, val, expiration)
}

func (c *NamespaceCache) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	return c.c.SetNX(ctx, c.namespace+key, val, expiration)
}

func (c *NamespaceCache) GetSet(ctx context.Context, key string, val string) Value {
	return c.c.GetSet(ctx, c.namespace+key, val)
}

func (c *NamespaceCache) Delete(ctx context.Context, key ...string) (int64, error) {
	newkey := make([]string, len(key))
	for i, v := range key {
		newkey[i] = c.namespace + v
	}
	return c.c.Delete(ctx, newkey...)
}

func (c *NamespaceCache) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	return c.c.LPush(ctx, c.namespace+key, val...)
}

func (c *NamespaceCache) LPop(ctx context.Context, key string) Value {
	return c.c.LPop(ctx, c.namespace+key)
}

func (c *NamespaceCache) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	return c.c.SAdd(ctx, c.namespace+key, members...)
}

func (c *NamespaceCache) SRem(ctx context.Context, key string, members ...any) (int64, error) {
	return c.c.SRem(ctx, c.namespace+key, members...)
}

func (c *NamespaceCache) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.c.IncrBy(ctx, c.namespace+key, value)
}

func (c *NamespaceCache) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.c.DecrBy(ctx, c.namespace+key, value)
}

func (c *NamespaceCache) IncrByFloat(ctx context.Context, key string, value float64) (float64, error) {
	return c.c.IncrByFloat(ctx, c.namespace+key, value)
}

func (c *NamespaceCache) Get(ctx context.Context, key string) Value {
	return c.c.Get(ctx, c.namespace+key)
}
