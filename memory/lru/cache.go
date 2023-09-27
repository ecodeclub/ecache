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

package lru

import (
	"context"
	"sync"
	"time"

	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/hashicorp/golang-lru/v2/simplelru"
)

type Cache struct {
	lock   sync.RWMutex
	client simplelru.LRUCache[string, any]
}

func NewCache(client simplelru.LRUCache[string, any]) *Cache {
	return &Cache{
		lock:   sync.RWMutex{},
		client: client,
	}
}

// Set expiration 无效 由lru 统一控制过期时间
func (c *Cache) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.client.Add(key, val)
	return nil
}

// SetNX expiration 无效 由lru 统一控制过期时间
func (c *Cache) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.client.Contains(key) {
		return false, nil
	}

	c.client.Add(key, val)

	return true, nil
}

func (c *Cache) Get(ctx context.Context, key string) (val ecache.Value) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var ok bool
	val.Val, ok = c.client.Get(key)
	if !ok {
		val.Err = errs.ErrKeyNotExist
	}

	return
}

func (c *Cache) GetSet(ctx context.Context, key string, val string) (result ecache.Value) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var ok bool
	result.Val, ok = c.client.Get(key)
	if !ok {
		result.Err = errs.ErrKeyNotExist
	}

	c.client.Add(key, val)

	return
}

func (c *Cache) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	// TODO
	return 0, nil
}

func (c *Cache) LPop(ctx context.Context, key string) (val ecache.Value) {
	// TODO
	return
}

func (c *Cache) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	// TODO
	return 0, nil
}

func (c *Cache) SRem(ctx context.Context, key string, members ...any) (val ecache.Value) {
	// TODO
	return
}

func (c *Cache) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	// TODO
	return 0, nil
}

func (c *Cache) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	// TODO
	return 0, nil
}

func (c *Cache) IncrByFloat(ctx context.Context, key string, value float64) (float64, error) {
	// TODO
	return 0, nil
}
