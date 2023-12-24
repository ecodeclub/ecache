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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ecodeclub/ekit/set"

	"github.com/ecodeclub/ekit/list"

	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
)

var (
	_ ecache.Cache = (*Cache)(nil)
)

type Cache struct {
	lock sync.RWMutex
	lru  *LRU[string, any]
}

func NewCache(lru *LRU[string, any]) *Cache {
	return &Cache{
		lock: sync.RWMutex{},
		lru:  lru,
	}
}

func (c *Cache) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.lru.AddTTL(key, val, expiration)
	return nil
}

// SetNX 由 strategy 统一控制过期时间
func (c *Cache) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.lru.Contains(key) {
		return false, nil
	}

	c.lru.AddTTL(key, val, expiration)

	return true, nil
}

func (c *Cache) Get(ctx context.Context, key string) (val ecache.Value) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var ok bool
	val.Val, ok = c.lru.Get(key)
	if !ok {
		val.Err = errs.ErrKeyNotExist
	}

	return
}

func (c *Cache) GetSet(ctx context.Context, key string, val string) (result ecache.Value) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var ok bool
	result.Val, ok = c.lru.Get(key)
	if !ok {
		result.Err = errs.ErrKeyNotExist
	}

	c.lru.Add(key, val)

	return
}

func (c *Cache) Delete(ctx context.Context, key ...string) (int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	n := int64(0)
	for _, k := range key {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		_, ok := c.lru.Get(k)
		if !ok {
			continue
		}
		if c.lru.Remove(k) {
			n++
		} else {
			return n, fmt.Errorf("%w: key = %s", errs.ErrDeleteKeyFailed, k)
		}
	}
	return n, nil
}

// anySliceToValueSlice 公共转换
func (c *Cache) anySliceToValueSlice(data ...any) []ecache.Value {
	newVal := make([]ecache.Value, len(data), cap(data))
	for key, value := range data {
		anyVal := ecache.Value{}
		anyVal.Val = value
		newVal[key] = anyVal
	}
	return newVal
}

func (c *Cache) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		ok     bool
		result = ecache.Value{}
	)
	result.Val, ok = c.lru.Get(key)
	if !ok {
		l := &list.ConcurrentList[ecache.Value]{
			List: list.NewLinkedListOf[ecache.Value](c.anySliceToValueSlice(val...)),
		}
		c.lru.Add(key, l)
		return int64(l.Len()), nil
	}

	data, ok := result.Val.(list.List[ecache.Value])
	if !ok {
		return 0, errors.New("当前key不是list类型")
	}

	err := data.Append(c.anySliceToValueSlice(val)...)
	if err != nil {
		return 0, err
	}

	c.lru.Add(key, data)
	return int64(data.Len()), nil
}

func (c *Cache) LPop(ctx context.Context, key string) (val ecache.Value) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		ok bool
	)
	val.Val, ok = c.lru.Get(key)
	if !ok {
		val.Err = errs.ErrKeyNotExist
		return
	}

	data, ok := val.Val.(list.List[ecache.Value])
	if !ok {
		val.Err = errors.New("当前key不是list类型")
		return
	}

	value, err := data.Delete(0)
	if err != nil {
		val.Err = err
		return
	}

	val = value
	return
}

func (c *Cache) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		ok     bool
		result = ecache.Value{}
	)
	result.Val, ok = c.lru.Get(key)
	if !ok {
		result.Val = set.NewMapSet[any](8)
	}

	s, ok := result.Val.(set.Set[any])
	if !ok {
		return 0, errors.New("当前key已存在不是set类型")
	}

	for _, value := range members {
		s.Add(value)
	}
	c.lru.Add(key, s)

	return int64(len(s.Keys())), nil
}

func (c *Cache) SRem(ctx context.Context, key string, members ...any) (int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	result, ok := c.lru.Get(key)
	if !ok {
		return 0, errs.ErrKeyNotExist
	}

	s, ok := result.(set.Set[any])
	if !ok {
		return 0, errors.New("当前key已存在不是set类型")
	}

	var rems int64
	for _, member := range members {
		if s.Exist(member) {
			s.Delete(member)
			rems++
		}
	}
	return rems, nil
}

func (c *Cache) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		ok     bool
		result = ecache.Value{}
	)
	result.Val, ok = c.lru.Get(key)
	if !ok {
		c.lru.Add(key, value)
		return value, nil
	}

	incr, err := result.Int64()
	if err != nil {
		return 0, errors.New("当前key不是int64类型")
	}

	newVal := incr + value
	c.lru.Add(key, newVal)

	return newVal, nil
}

func (c *Cache) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		ok     bool
		result = ecache.Value{}
	)
	result.Val, ok = c.lru.Get(key)
	if !ok {
		c.lru.Add(key, -value)
		return -value, nil
	}

	decr, err := result.Int64()
	if err != nil {
		return 0, errors.New("当前key不是int64类型")
	}

	newVal := decr - value
	c.lru.Add(key, newVal)

	return newVal, nil
}

func (c *Cache) IncrByFloat(ctx context.Context, key string, value float64) (float64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		ok     bool
		result = ecache.Value{}
	)
	result.Val, ok = c.lru.Get(key)
	if !ok {
		c.lru.Add(key, value)
		return value, nil
	}

	val, err := result.Float64()
	if err != nil {
		return 0, errors.New("当前key不是float64类型")
	}

	newVal := val + value
	c.lru.Add(key, newVal)

	return newVal, nil
}
