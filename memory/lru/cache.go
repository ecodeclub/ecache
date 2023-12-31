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

type entry struct {
	key       string
	value     any
	expiresAt time.Time
}

func (e entry) isExpired() bool {
	return !e.expiresAt.IsZero() && e.expiresAt.Before(time.Now())
}

type EvictCallback func(key string, value any)

type Option func(l *Cache)

func WithEvictCallback(callback func(k string, v any)) Option {
	return func(l *Cache) {
		l.callback = callback
	}
}

type Cache struct {
	lock     sync.RWMutex
	capacity int
	list     *linkedList[entry]
	data     map[string]*element[entry]
	callback EvictCallback
}

func NewCache(capacity int, options ...Option) *Cache {
	res := &Cache{
		list:     newLinkedList[entry](),
		data:     make(map[string]*element[entry], capacity),
		capacity: capacity,
	}
	for _, opt := range options {
		opt(res)
	}
	return res
}

func (c *Cache) pushEntry(key string, ent entry) bool {
	if c.len() > c.capacity {
		c.removeOldest()
	}
	if elem, ok := c.data[key]; ok {
		elem.Value = ent
		c.list.moveToFront(elem)
		return false
	}
	elem := c.list.pushFront(ent)
	c.data[key] = elem
	return true
}

func (c *Cache) addTTL(key string, value any, expiration time.Duration) bool {
	ent := entry{key: key, value: value,
		expiresAt: time.Now().Add(expiration)}
	return c.pushEntry(key, ent)
}

func (c *Cache) add(key string, value any) bool {
	ent := entry{key: key, value: value}
	return c.pushEntry(key, ent)
}

func (c *Cache) get(key string) (value any, ok bool) {
	if elem, exist := c.data[key]; exist {
		ent := elem.Value
		if ent.isExpired() {
			c.removeElement(elem)
			return
		}
		c.list.moveToFront(elem)
		return ent.value, true
	}
	return
}

func (c *Cache) removeOldest() {
	if elem := c.list.back(); elem != nil {
		c.removeElement(elem)
	}
}

func (c *Cache) removeElement(elem *element[entry]) {
	c.list.removeElem(elem)
	ent := elem.Value
	c.delete(ent.key)
	if c.callback != nil {
		c.callback(ent.key, ent.value)
	}
}

func (c *Cache) remove(key string) (present bool) {
	if elem, ok := c.data[key]; ok {
		c.removeElement(elem)
		if elem.Value.isExpired() {
			return false
		}
		return true
	}
	return false
}

func (c *Cache) contains(key string) (ok bool) {
	elem, ok := c.data[key]
	if ok {
		if elem.Value.isExpired() {
			c.removeElement(elem)
			return false
		}
	}
	return ok
}

func (c *Cache) delete(key string) {
	delete(c.data, key)
}

func (c *Cache) len() int {
	var length int
	for elem, i := c.list.back(), 0; i < c.list.len(); i++ {
		if elem.Value.isExpired() {
			c.removeElement(elem)
			continue
		}
		elem = elem.prev
		length++
	}
	return length
}

func (c *Cache) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.addTTL(key, val, expiration)
	return nil
}

func (c *Cache) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.contains(key) {
		return false, nil
	}

	c.addTTL(key, val, expiration)

	return true, nil
}

func (c *Cache) Get(ctx context.Context, key string) (val ecache.Value) {
	c.lock.Lock()
	defer c.lock.Unlock()
	var ok bool
	val.Val, ok = c.get(key)
	if !ok {
		val.Err = errs.ErrKeyNotExist
	}

	return
}

func (c *Cache) GetSet(ctx context.Context, key string, val string) (result ecache.Value) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var ok bool
	result.Val, ok = c.get(key)
	if !ok {
		result.Err = errs.ErrKeyNotExist
	}

	c.add(key, val)

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
		_, ok := c.get(k)
		if !ok {
			continue
		}
		if c.remove(k) {
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
	result.Val, ok = c.get(key)
	if !ok {
		l := &list.ConcurrentList[ecache.Value]{
			List: list.NewLinkedListOf[ecache.Value](c.anySliceToValueSlice(val...)),
		}
		c.add(key, l)
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

	c.add(key, data)
	return int64(data.Len()), nil
}

func (c *Cache) LPop(ctx context.Context, key string) (val ecache.Value) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		ok bool
	)
	val.Val, ok = c.get(key)
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
	result.Val, ok = c.get(key)
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
	c.add(key, s)

	return int64(len(s.Keys())), nil
}

func (c *Cache) SRem(ctx context.Context, key string, members ...any) (int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	result, ok := c.get(key)
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
	result.Val, ok = c.get(key)
	if !ok {
		c.add(key, value)
		return value, nil
	}

	incr, err := result.Int64()
	if err != nil {
		return 0, errors.New("当前key不是int64类型")
	}

	newVal := incr + value
	c.add(key, newVal)

	return newVal, nil
}

func (c *Cache) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		ok     bool
		result = ecache.Value{}
	)
	result.Val, ok = c.get(key)
	if !ok {
		c.add(key, -value)
		return -value, nil
	}

	decr, err := result.Int64()
	if err != nil {
		return 0, errors.New("当前key不是int64类型")
	}

	newVal := decr - value
	c.add(key, newVal)

	return newVal, nil
}

func (c *Cache) IncrByFloat(ctx context.Context, key string, value float64) (float64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		ok     bool
		result = ecache.Value{}
	)
	result.Val, ok = c.get(key)
	if !ok {
		c.add(key, value)
		return value, nil
	}

	val, err := result.Float64()
	if err != nil {
		return 0, errors.New("当前key不是float64类型")
	}

	newVal := val + value
	c.add(key, newVal)

	return newVal, nil
}
