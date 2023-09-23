package priority

import (
	"context"
	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit"
	"sync"
	"time"
)

type Option func(c *Cache)

func WithCapacity(cap int) Option {
	return func(c *Cache) {
		c.cap = cap
	}
}

func WithComparator(comparator Comparator[Node]) Option {
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

	cache := &Cache{
		index:             make(map[string]*Node),
		comparator:        defaultComparator{},
		cap:               defaultCap,
		cleanInterval:     defaultCleanInterval,
		scanCount:         defaultScanCount,
		defaultExpiration: defaultExpiration,
	}

	for _, opt := range opts {
		opt(cache)
	}

	cache.pq = NewQueueWithHeap[Node](cache.comparator)

	go cache.clean()

	return cache
}

// defaultComparator 默认比较器 按节点的过期时间进行比较
type defaultComparator struct {
}

func (d defaultComparator) Compare(src, dest *Node) int {
	if src.Dl.Before(dest.Dl) {
		return -1
	}

	if src.Dl.After(dest.Dl) {
		return 1
	}

	return 0
}

type Cache struct {
	index             map[string]*Node // 用于存储数据的索引，方便快速查找
	pq                Queue[Node]      // 优先级队列，用于存储数据
	comparator        Comparator[Node] // 比较器
	mu                sync.RWMutex     // 读写锁
	cap               int              // 容量
	len               int              // 当前队列长度
	cleanInterval     time.Duration    // 清理过期数据的时间间隔
	scanCount         int              // 扫描次数
	closeC            chan struct{}    // 关闭信号
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
	c.checkCapacityAndDisuse(ctx)

	node := &Node{
		Key: key,
		Val: val,
		Dl:  time.Now().Add(expiration),
	}

	_ = c.pq.Push(ctx, node)

	c.index[key] = node
	c.len++
}

func (c *Cache) checkCapacityAndDisuse(ctx context.Context) {
	if c.len >= c.cap {
		// 淘汰优先级最低的数据
		node, _ := c.pq.Pop(ctx)
		// 删除索引
		delete(c.index, node.Key)
		c.len--
	}
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

func (c *Cache) SRem(ctx context.Context, key string, members ...any) ecache.Value {
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
	_ = c.pq.Remove(context.Background(), n)
	delete(c.index, n.Key)
}

type Node struct {
	Key string
	Val any
	Dl  time.Time // 过期时间
	idx int
}

func (n Node) Index() int {
	return n.idx
}

func (n Node) SetIndex(idx int) {
	n.idx = idx
}
