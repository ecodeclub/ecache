//go:build goexperiment.arenas

package arena

import (
	arenapkg "arena"
	"context"
	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit"
	"sync"
	"time"
	"unsafe"
)

type Option[T any] func(*Cache[T])

// WithExpiration 设置过期时间
func WithExpiration[T any](exp time.Duration) Option[T] {
	return func(c *Cache[T]) {
		c.defaultExp = exp
	}
}

// WithThreshold 设置阈值
func WithThreshold[T any](threshold uintptr) Option[T] {
	return func(c *Cache[T]) {
		c.threshold = threshold
	}
}

// WithScanCount 设置扫描次数
func WithScanCount[T any](count int64) Option[T] {
	return func(c *Cache[T]) {
		c.scanCount = count
	}
}

// WithScanInterval 设置扫描间隔
func WithScanInterval[T any](interval time.Duration) Option[T] {
	return func(c *Cache[T]) {
		c.scanInterval = interval
	}
}

func NewCache[V any](opts ...Option[V]) *Cache[V] {
	// 默认过期时间为30s
	defaultExp := 30 * time.Second
	// 默认阈值为100M
	defaultThreshold := 100 * 1024 * 1024
	// 默认扫描次数为1000
	defaultScanCount := 1000
	// 默认扫描间隔为10s
	defaultScanInterval := 10 * time.Second

	cache := &Cache[V]{
		index:        make(map[string]*data[V]),
		defaultExp:   defaultExp,
		threshold:    uintptr(defaultThreshold),
		scanCount:    int64(defaultScanCount),
		scanInterval: defaultScanInterval,
	}

	for _, opt := range opts {
		opt(cache)
	}

	cache.chain = newArenas[V](defaultExp, cache.threshold)

	// 将arena的锁指向cache的锁，这样就可以保证arena的锁和cache的锁是同一个
	cache.chain.mu = &cache.mu

	// 启动一个goroutine，定时清理过期的arena和索引数据
	go cache.clean()

	return cache
}

// Cache 是一个基于arena的缓存
// 由于目前arena提供的api，在开辟内存的时候，需要泛型的支持，所以这里设计为泛型
// 而且目前arena提供的api，无法单独释放某个元素的内存，只能释放整个arena
// 所以目前cache的设计受到了一定的限制，比如何时应该释放arena，如何定位arena中的元素等
// 当前的设计思路如下：
//  1. cache的数据结构为一组arena构成的有序双向链表，按照过期时间排序，用于存储值数据
//  2. 在arena链的基础上，维护一个哈希索引，用于快速定位数据
//  3. 当往cache中设置一个键值对时，逻辑如下：
//     3.1 先查看索引中是否存在，如果存在，则执行以下逻辑：
//     3.1.1 如果设置当前值的过期时间早于原来元素所对应arena的过期时间，说明可以复用原来的arena，则直接更新值
//     3.1.2 如果设置当前值的过期时间晚于原来元素所对应arena的过期时间，则说明沿着arena链向后查找，找到一个合适的arena，
//     重新开辟一块内存，将值插入到该arena中(如何定义合适的arena，后边会进行说明)，然后更新当前值所对应的arena
//     3.2 如果索引中不存在，则执行以下逻辑找到一个合适的arena，将值插入：
//     3.2.1 如果当前arena链为空，则创建一个新的arena，将值插入到该arena中
//     3.2.2 如果当前arena链不为空，则沿着arena链向后查找，找到第一个arena过期时间大于当前值过期时间的arena，执行以下逻辑：
//     3.2.2.1 如果当前arena的过期时间和当前元素的过期时间在一定范围内，则直接使用该arena，将值插入到该arena中
//     3.2.2.2 如果当前arena的过期时间超过当前元素过期时间一定范围，为了防止该元素原本应该早就过期了，
//     但是一直要等待arena过期才被释放，这段时间有可能时很长一段时间，所以这里应该重新创建一个arena， 此arena的创建逻辑如下：
//     3.2.2.2.1 如果当前arena链中此arena没有上一个arena，或者当前设置值的过期时间超过此arena链中上一个arena一定的范围，
//     则以当前元素的过期时间作为过期时间，创建一个新的arena，将值插入到该arena中
//     3.2.2.2.2 如果当前设置值的过期时间与arena链中上一个arena的过期时间在一定的范围内，
//     则以上一个arena的过期时间加上此范围作为过期时间创建一个新的arena，将值插入到该arena中
//     (此处不用当前元素的过期时间作为arena的过期时间，是防止过期时间离上一个arena过期时间太近，可能会频繁创建arena)
//     3.3.3 如果当前arena链中没有合适的arena，则执行3.2.2.2.1和3.2.2.2.2的逻辑创建一个新的arena
//     (以上每次创建新的arena，都会将新的arena插入到arena链中，同时更新索引)
//
// 过期的索引数据和过期的arena，都会在后台goroutine中进行清理
//  1. 清理过期索引数据逻辑如下： 每次清理的时候，都会遍历索引找到过期的数据，然后删除，
//     但是为了防止一直占用锁，所以清理采用定时扫描加惰性删除的方式
//  2. 清理过期arena逻辑如下： 每次会获取arena链的头节点，然后阻塞等待arena过期，过期后，会将arena从链中删除，
//     但是为了防止从头部插入新的arena，则每次会给清理的goroutine一个信号，看是否需要重置阻塞时间
type Cache[V any] struct {
	chain        *arenas[V]          // 按照过期时间排序的arena
	index        map[string]*data[V] // 索引arena中的数据
	defaultExp   time.Duration       // 默认过期时间
	threshold    uintptr             // 超过此阈值时，如果当前arena中没有活跃数据，可以释放arena
	mu           sync.RWMutex        // 保护index
	scanCount    int64               // 清除索引数据的时候，为了避免一直占有锁，限制扫描次数
	scanInterval time.Duration       // 多久扫描一次索引数据
	closeC       chan struct{}       // 关闭信号
}

func (c *Cache[V]) clean() {

	// 启动一个goroutine，定时清理过期的arena
	go c.chain.clean()

	ticker := time.NewTicker(c.scanInterval)

	for {
		select {
		case <-ticker.C:
			c.cleanIndex()
		case <-c.closeC:
			return
		}
	}
}

func (c *Cache[V]) cleanIndex() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 限制扫描次数
	var count int64

	for k, d := range c.index {
		if count >= c.scanCount {
			break
		}
		// 如果过期时间早于当前时间，则删除
		if d.dl.Before(time.Now()) {
			d.a.deCount()
			delete(c.index, k)
		}
		count++
	}
}

func (c *Cache[V]) Set(ctx context.Context, key string, val V, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 先查看是否存在
	d, ok := c.index[key]

	dl := time.Now().Add(expiration)

	// 不存在或者当前arena已经过期
	if !ok || d.a.dl.Before(time.Now()) {
		// 找到一个合适的arena
		a := c.chain.find(dl)

		c.index[key] = a.newData(key, val, dl)
		return nil
	}

	d.dl = dl
	// 如果新的过期时间早于arena的过期时间，则无需重新分配内存
	if d.dl.Before(d.a.dl) {
		*d.val = val
		return nil
	}

	// 如果新的过期时间晚于arena的过期时间，则重新分配内存，找到下一个合适的arena
	d.a.deCount()
	a := c.chain.findFrom(d.a, dl)
	d.a = a
	d.val = a.New()
	*d.val = val
	return nil
}

func (c *Cache[V]) SetNX(ctx context.Context, key string, val V, expiration time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	d, ok := c.index[key]

	dl := time.Now().Add(expiration)

	// 如果存在并且没有过期，则更新过期时间
	if ok && d.dl.After(time.Now()) {
		// 存在，更新
		d.dl = dl
		// 如果新的过期时间早于arena的过期时间，则无需重新分配内存
		if d.dl.Before(d.a.dl) {
			return false, nil
		}

		// 如果新的过期时间晚于arena的过期时间，则重新分配内存，找到下一个合适的arena
		d.a.deCount()
		a := c.chain.findFrom(d.a, dl)

		v := d.val

		d.a = a
		d.val = a.New()
		*d.val = *v
		return false, nil
	}

	// 走到这里说明不存在或者已经过期，这时候应该set
	// 存在，但是过期了，如果arena还没有过期，则更新
	if ok && d.a.dl.After(time.Now()) {
		d.dl = dl

		a := c.chain.findFrom(d.a, dl)
		// 说明可以复用，更新时间与值即可
		if a == d.a {
			*d.val = val
			return true, nil
		}

		// 说明不能复用，需要重新分配内存
		d.a.deCount()
		d.a = a
		d.val = a.New()
		*d.val = val
		return true, nil
	}

	// 找到一个合适的arena
	a := c.chain.find(dl)

	c.index[key] = a.newData(key, val, dl)
	return true, nil
}

func (c *Cache[V]) Get(ctx context.Context, key string) ecache.Value {
	c.mu.Lock()
	defer c.mu.Unlock()

	d, ok := c.index[key]

	if ok {
		// 如果没有过期，则返回
		if d.dl.After(time.Now()) {
			return ecache.Value{
				AnyValue: ekit.AnyValue{
					Val: d.val,
				},
			}
		}
		// 如果过期了，则删除
		d.a.deCount()
		delete(c.index, key)
	}

	return ecache.Value{
		AnyValue: ekit.AnyValue{
			Err: errs.ErrKeyNotExist,
		},
	}
}

func (c *Cache[V]) GetSet(ctx context.Context, key string, val V) ecache.Value {
	c.mu.Lock()
	defer c.mu.Unlock()

	dl := time.Now().Add(c.defaultExp)

	d, ok := c.index[key]

	if ok {
		// 如果没有过期或者arena没有过期，则直接替换值
		if now := time.Now(); d.dl.After(now) || d.a.dl.After(now) {
			v := *d.val

			*d.val = val

			if d.dl.After(now) {
				return ecache.Value{
					AnyValue: ekit.AnyValue{
						Val: v,
					},
				}
			}

			d.dl = dl

			return ecache.Value{
				AnyValue: ekit.AnyValue{
					Val: v,
					Err: errs.ErrKeyNotExist,
				},
			}
		}

		// 数据过期，arena也过期了，删除
		d.a.deCount()
		delete(c.index, key)
	}

	// 找到一个合适的arena
	a := c.chain.find(dl)
	c.index[key] = a.newData(key, val, dl)

	return ecache.Value{
		AnyValue: ekit.AnyValue{
			Val: val,
			Err: errs.ErrKeyNotExist,
		},
	}
}

func (c *Cache[V]) Close() error {
	_ = c.chain.Close()
	close(c.closeC)
	return nil
}

func newArenas[V any](defaultExp time.Duration, threshold uintptr) *arenas[V] {
	return &arenas[V]{
		defaultExp: defaultExp,
		threshold:  threshold,
	}
}

type arenas[T any] struct {
	head       *arena[T]
	tail       *arena[T]
	size       int // arena的数量
	defaultExp time.Duration
	threshold  uintptr
	c          chan time.Time
	mu         *sync.RWMutex
}

func (ac *arenas[T]) Close() error {
	close(ac.c)
	return nil
}

func (ac *arenas[T]) Free(a *arena[T]) {
	ac.size--
	a.Free()
}

// clean 清理过期的arena
func (ac *arenas[T]) clean() {
	var earlyC <-chan time.Time

	if ac.head != nil {
		earlyC = time.After(ac.head.dl.Sub(time.Now()))
	}

	for {
	SEL:
		select {
		case <-earlyC:
			ac.mu.Lock()
			// 说明有arena过期了
			cur := ac.head
			for cur != nil {
				// 如果过期了，则删除
				if cur.dl.Before(time.Now()) {
					ac.head = cur.next
					ac.Free(cur)

					cur = ac.head
					continue
				}

				// 如果没有过期，但是已经超过阈值了，可以删除了
				// 目前先不删除，因为加上这段逻辑，在其他地方还得考虑很多的问题
				/*if cur.len*ac.threshold > ac.threshold && cur.activeCount == 0 {
					// 说明没有过期，但是已经超过阈值了，可以删除了
					ac.head = cur.next
					ac.Free(cur)

					cur = ac.head
					continue
				}*/

				// 如果没有过期，则说明后面的都没有过期
				earlyC = time.After(cur.dl.Sub(time.Now()))
				ac.mu.Unlock()
				break SEL
			}
			earlyC = nil
			ac.mu.Unlock()
		case t, ok := <-ac.c:
			if !ok {
				return
			}
			// 如果过期时间比头节点早，则要更新等待时间
			if ac.head != nil && ac.head.dl.After(t) {
				earlyC = time.After(t.Sub(time.Now()))
			}
		}
	}

}

func (ac *arenas[T]) add(a *arena[T]) {
	if ac.head == nil {
		ac.head = a
		ac.tail = a
		return
	}

	ac.tail.next = a
	a.prev = ac.tail
	ac.tail = a
}

// find 找到一个合适的arena
func (ac *arenas[T]) find(dl time.Time) *arena[T] {
	// 懒初始化
	if ac.head == nil {
		// 新建一个arena
		// 防止arena过期时间太短，导致频繁创建arena
		mdl := time.Now().Add(ac.defaultExp)

		if mdl.Before(dl) {
			mdl = dl
		}

		a := ac.newArena(mdl)
		ac.add(a)
		return a
	}

	return ac.findFrom(ac.head, dl)
}

// findFrom 从指定的arena开始找, 如果a为nil，则从头开始找
func (ac *arenas[T]) findFrom(a *arena[T], dl time.Time) *arena[T] {
	ms := a
	cur := ms

	// 查找合适的arena分配内存
	for ms != nil {
		// 跳过过期时间早于当前元素过期时间的arena
		if ms.dl.Before(dl) {
			cur = ms
			ms = ms.next
			continue
		}

		// 找到了合适的arena
		// 如果当前元素过期时间早于当前arena过期时间减去默认过期时间(也就是过期时间不会相差太久)，则将当前元素插入到当前arena中
		if dl.Add(ac.defaultExp).Before(ms.dl) {
			// 如果当前元素时间处于上个arena和上个arena加上默认过期时间之间，
			// 则以上个arena加上默认过期时间作为过期时间创建一个新的arena
			// 然后将元素在此arena中分配内存
			mdl := dl

			if ms.prev != nil && ms.prev.dl.Add(ac.defaultExp).After(dl) {
				mdl = ms.prev.dl.Add(ac.defaultExp)
			}

			// 如果当前元素是头节点，且当前元素过期时间早于当前时间加上默认过期时间，则以当前时间加上默认过期时间作为过期时间创建一个新的arena
			if t := time.Now().Add(ac.defaultExp); ms.prev == nil && dl.Before(t) {
				mdl = t
			}

			// 以当前元素过期时间作为过期时间创建一个新的arena
			tmp := ac.newArena(mdl)
			tmp.next = ms
			tmp.prev = ms.prev
			if ms.prev != nil {
				ms.prev.next = tmp
			} else {
				// 说明当前元素是头节点
				ac.head = tmp
			}
			ms.prev = tmp
			ms = tmp
		}

		return ms
	}

	// 没有找到合适的arena，创建一个新的arena
	// 防止arena过期时间太短，导致频繁创建arena
	mdl := cur.dl.Add(ac.defaultExp)

	if mdl.Before(dl) {
		mdl = dl
	}

	ms = ac.newArena(mdl)
	ms.prev = cur
	cur.next = ms
	return ms
}

func (ac *arenas[T]) newArena(dl time.Time) *arena[T] {

	a := newArena[T](dl, ac.threshold)

	ac.size++

	// 通知清除goroutine有新的arena加入
	select {
	case ac.c <- dl:
	default:
	}

	return a
}

func newArena[V any](dl time.Time, threshold uintptr) *arena[V] {
	size := unsafe.Sizeof(*new(V))
	return &arena[V]{
		m:         arenapkg.NewArena(),
		dl:        dl,
		threshold: threshold,
		elemSize:  size,
	}
}

type arena[T any] struct {
	m           *arenapkg.Arena
	len         uintptr   // 表示arena当前容量
	dl          time.Time // 记录当前arena过期时间
	threshold   uintptr   // 超过此阈值时，如果当前arena中没有活跃数据，可以释放arena
	activeCount uintptr   // 表示当前arena中活跃的数据数量，当活跃数为0时，可以释放arena
	next        *arena[T] // 下一个arena
	prev        *arena[T] // 上一个arena
	// ac          *arenas[T] // 所属的arenas
	elemSize uintptr // 元素大小
}

func (a *arena[T]) New() *T {
	a.activeCount++
	a.len += a.elemSize
	return arenapkg.New[T](a.m)
}

func (a *arena[T]) deCount() {
	a.activeCount--
}

func (a *arena[T]) Free() {
	a.m.Free()
	if a.prev != nil {
		a.prev.next = a.next
	}
	if a.next != nil {
		a.next.prev = a.prev
	}
	a.next = nil
	a.prev = nil
}

func (a *arena[T]) newData(key string, val T, dl time.Time) *data[T] {
	m := a.New()
	*m = val

	return &data[T]{
		key: key,
		val: m,
		dl:  dl,
		a:   a,
	}
}

type data[T any] struct {
	key string
	val *T
	dl  time.Time
	a   *arena[T]
}
