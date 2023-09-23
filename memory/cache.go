// Copyright 2023 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"context"
	"github.com/ecodeclub/ecache"
	"time"
)

var _ ecache.Cache = (*Cache)(nil)

type Cache struct {
	client ecache.Cache
}

func NewCache(client ecache.Cache) *Cache {
	return &Cache{client: client}
}

func (c *Cache) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	return c.client.Set(ctx, key, val, expiration)
}

func (c *Cache) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, val, expiration)
}

func (c *Cache) Get(ctx context.Context, key string) ecache.Value {
	return c.client.Get(ctx, key)
}

func (c *Cache) GetSet(ctx context.Context, key string, val string) ecache.Value {
	return c.client.GetSet(ctx, key, val)
}

func (c *Cache) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	return c.client.LPush(ctx, key, val...)
}

func (c *Cache) LPop(ctx context.Context, key string) ecache.Value {
	return c.client.LPop(ctx, key)
}

func (c *Cache) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	return c.client.SAdd(ctx, key, members...)
}

func (c *Cache) SRem(ctx context.Context, key string, members ...any) ecache.Value {
	return c.client.SRem(ctx, key, members...)
}

func (c *Cache) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.client.IncrBy(ctx, key, value)
}

func (c *Cache) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.client.DecrBy(ctx, key, value)
}
