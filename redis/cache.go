// Copyright 2021 ecodeclub
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

package redis

import (
	"context"
	"errors"
	"time"

	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/redis/go-redis/v9"
)

var _ ecache.Cache = (*Cache)(nil)

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
	val.Val, val.Err = c.client.Get(ctx, key).Result()
	if val.Err != nil && errors.Is(val.Err, redis.Nil) {
		val.Err = errs.ErrKeyNotExist
	}
	return
}
