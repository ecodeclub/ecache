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

package priority

import (
	"context"
	"sync"
	"time"

	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit"
	"github.com/ecodeclub/ekit/queue"
)

type Option func(c *Cache)

func WithCapacity(cap int) Option {
	return func(c *Cache) {
		c.cap = cap
	}
}

func WithComparator(comparator ekit.Comparator[*Node]) Option {
	return func(c *Cache) {
		c.comparator = comparator
	}
}

func WithCleanInterval(interval time.Duration) Option {
	return func(c *Cache) {
		c.cleanInterval = interval
	}
}

func NewCache(opts ...Option) ecache.Cache {
	defaultCap := 1024
	defaultCleanInterval := 10 * time.Second
	defaultScanCount := 1000
	defaultExpiration := 30 * time.Second

	// defaultComparator 默认比较器 按节点的过期时间进行比较
	defaultComparator := func(src, dest *Node) int {
		if src.Dl.Before(dest.Dl) {
			return -1
		}

		if src.Dl.After(dest.Dl) {
			return 1
		}

		return 0
	}

	cache := &Cache{
		index:             make(map[string]*Node),
		comparator:        defaultComparator,
		cap:               defaultCap,
		cleanInterval:     defaultCleanInterval,
		scanCount:         defaultScanCount,
		defaultExpiration: defaultExpiration,
	}

	for _, opt := range opts {
		opt(cache)
	}

	cache.pq = queue.NewPriorityQueue[*Node](defaultCap, cache.comparator)

	go cache.clean()

	return cache
}

type Cache struct {
	index             map[string]*Node            // 用于存储数据的索引，方便快速查找
	pq                *queue.PriorityQueue[*Node] // 优先级队列，用于存储数据
	comparator        ekit.Comparator[*Node]      // 比较器
	mu                sync.RWMutex                // 读写锁
	cap               int                         // 容量
	len               int                         // 当前队列长度
	cleanInterval     time.Duration               // 清理过期数据的时间间隔
	scanCount         int                         // 扫描次数
	closeC            chan struct{}               // 关闭信号
	defaultExpiration time.Duration
}

func (c *Cache) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 如果存在，则更新
	if node, ok := c.index[key]; ok {
		node.Val = val
		node.Dl = time.Now().Add(expiration) // 更新过期时间
		return nil
	}
	// 如果不存在，则插入
	// 插入之前校验容量是否已满，如果已满，需要淘汰优先级最低的数据
	c.add(ctx, key, val, expiration)

	return nil
}

func (c *Cache) add(ctx context.Context, key string, val any, expiration time.Duration) {
	c.checkCapacityAndDisuse()

	node := &Node{
		Key: key,
		Val: val,
		Dl:  time.Now().Add(expiration),
	}

	_ = c.pq.Enqueue(node)

	c.index[key] = node
	c.len++
}

func (c *Cache) checkCapacityAndDisuse() {
	if c.len >= c.cap {
		// 先淘汰堆顶元素，保证有足够的空间插入新数据
		c.disuse()

		// 看下堆顶元素是否是否被标记删除，如果是，则删除
		for top, _ := c.pq.Peek(); top.isDel; top, _ = c.pq.Peek() {
			c.disuse()
		}

	}
}

func (c *Cache) disuse() {
	// 淘汰优先级最低的数据
	node, _ := c.pq.Dequeue()
	// 删除索引
	delete(c.index, node.Key)
	c.len--
}

func (c *Cache) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, ok := c.index[key]; ok {
		node.Dl = time.Now().Add(expiration) // 更新过期时间
		return false, nil
	}

	c.add(ctx, key, val, expiration)

	return true, nil
}

func (c *Cache) Get(ctx context.Context, key string) ecache.Value {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.index[key]

	if ok && node.Dl.After(time.Now()) {
		return ecache.Value{
			AnyValue: ekit.AnyValue{
				Val: node.Val,
			},
		}
	}

	// 过期删除
	if ok {
		c.delete(node)
		c.len--
	}

	return ecache.Value{
		AnyValue: ekit.AnyValue{
			Err: errs.ErrKeyNotExist,
		},
	}
}

func (c *Cache) GetSet(ctx context.Context, key string, val string) ecache.Value {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.index[key]

	if ok && node.Dl.After(time.Now()) {
		old := node.Val
		node.Val = val
		return ecache.Value{
			AnyValue: ekit.AnyValue{
				Val: old,
			},
		}
	}

	if ok {
		node.Val = val
		node.Dl = time.Now().Add(c.defaultExpiration)
	} else {
		c.add(ctx, key, val, c.defaultExpiration)
	}

	return ecache.Value{
		AnyValue: ekit.AnyValue{
			Err: errs.ErrKeyNotExist,
		},
	}

}

func (c *Cache) Delete(ctx context.Context, key ...string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var count int64

	for _, k := range key {
		// 这里其实还要考虑过期的情况，如果过期了，是否要计入删除的数量
		// 这里暂时不考虑过期的情况
		if node, ok := c.index[k]; ok {
			c.delete(node)
			c.len--
			count++
		}
	}

	return count, nil
}

func (c *Cache) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Cache) LPop(ctx context.Context, key string) ecache.Value {
	//TODO implement me
	panic("implement me")
}

func (c *Cache) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Cache) SRem(ctx context.Context, key string, members ...any) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Cache) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Cache) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Cache) IncrByFloat(ctx context.Context, key string, value float64) (float64, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Cache) Close() error {
	close(c.closeC)
	return nil
}

func (c *Cache) clean() {

	ticker := time.NewTicker(c.cleanInterval)

	for {
		select {
		case <-ticker.C:
			c.scan()
		case <-c.closeC:
			return
		}
	}
}

func (c *Cache) scan() {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, v := range c.index {
		if v.Dl.Before(time.Now()) {
			c.delete(v)
			c.len--
		}
		count++
		if count >= c.scanCount {
			break
		}
	}
}

func (c *Cache) delete(n *Node) {
	// 标记删除
	n.isDel = true
	delete(c.index, n.Key)
}

type Node struct {
	Key   string
	Val   any
	Dl    time.Time // 过期时间
	isDel bool
}
