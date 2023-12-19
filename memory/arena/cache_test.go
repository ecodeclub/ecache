//go:build goexperiment.arenas

package arena

import (
	"context"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCache_Set(t *testing.T) {

	ctx := context.TODO()

	testCases := []struct {
		name string

		cache *Cache[user]

		key        string
		val        user
		expiration time.Duration

		beforeFunc func(*Cache[user])

		wantIndex       map[string]*data[user]
		wantIndArenaExp map[string]time.Time // 元素所对应arena的过期时间，验证是否正确设置
		wantChain       *arenas[user]
	}{
		{
			// 测试过期时间在默认的过期时间之内
			name:       "set a new key with expiration < default expiration",
			cache:      NewCache[user](),
			key:        "k1",
			val:        user{name: "k1", age: 18, score: 99.9},
			expiration: 10 * time.Second,
			beforeFunc: func(c *Cache[user]) {

			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 99.9},
					dl:  time.Now().Add(10 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(30 * time.Second),
					},
				},
			},
			wantChain: &arenas[user]{
				size: 1,
				head: &arena[user]{
					dl: time.Now().Add(30 * time.Second),
				},
				tail: &arena[user]{
					dl: time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试过期时间超过默认的过期时间
			name:       "set a new key with expiration > default expiration",
			cache:      NewCache[user](),
			key:        "k1",
			val:        user{name: "k1", age: 18, score: 99.9},
			expiration: 40 * time.Second,
			beforeFunc: func(c *Cache[user]) {

			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 99.9},
					dl:  time.Now().Add(40 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(40 * time.Second),
					},
				},
			},
			wantChain: &arenas[user]{
				size: 1,
				head: &arena[user]{
					dl: time.Now().Add(40 * time.Second),
				},
				tail: &arena[user]{
					dl: time.Now().Add(40 * time.Second),
				},
			},
		},
		{
			// 测试已经存在一个arena，并且复用这个arena
			name:       "set a new key with has one arena and reuse this arena",
			cache:      NewCache[user](),
			key:        "k2",
			val:        user{name: "k2", age: 20, score: 100.9},
			expiration: 80 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 19, score: 98.9}, 100*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 19, score: 98.9},
					dl:  time.Now().Add(100 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(100 * time.Second),
					},
				},
				"k2": {
					key: "k2",
					val: &user{name: "k2", age: 20, score: 100.9},
					dl:  time.Now().Add(80 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(100 * time.Second),
					},
				},
			},
			wantChain: &arenas[user]{
				size: 1,
				head: &arena[user]{
					dl: time.Now().Add(100 * time.Second),
				},
				tail: &arena[user]{
					dl: time.Now().Add(100 * time.Second),
				},
			},
		},
		{
			// 测试已经存在一个arena，并在前边创建一个以元素过期时间为准的arena
			name:       "set a new key with has one arena and create a new arena with element expiration",
			cache:      NewCache[user](),
			key:        "k2",
			val:        user{name: "k2", age: 20, score: 100.9},
			expiration: 50 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 19, score: 98.9}, 100*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 19, score: 98.9},
					dl:  time.Now().Add(100 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(100 * time.Second),
					},
				},
				"k2": {
					key: "k2",
					val: &user{name: "k2", age: 20, score: 100.9},
					dl:  time.Now().Add(50 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(50 * time.Second),
					},
				},
			},
			wantChain: func() *arenas[user] {
				tail := &arena[user]{
					dl: time.Now().Add(100 * time.Second),
				}

				head := &arena[user]{
					dl:   time.Now().Add(50 * time.Second),
					next: tail,
				}

				tail.prev = head

				return &arenas[user]{
					size: 2,
					head: head,
					tail: tail,
				}
			}(),
		},
		{
			// 测试已经存在一个arena，并在前边创建一个以当前时间加上默认过期时间为准的arena
			name:       "set a new key with has one arena and create a new arena with current time add default expiration",
			cache:      NewCache[user](),
			key:        "k2",
			val:        user{name: "k2", age: 20, score: 100.9},
			expiration: 20 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 19, score: 98.9}, 100*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 19, score: 98.9},
					dl:  time.Now().Add(100 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(100 * time.Second),
					},
				},
				"k2": {
					key: "k2",
					val: &user{name: "k2", age: 20, score: 100.9},
					dl:  time.Now().Add(20 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(30 * time.Second),
					},
				},
			},
			wantChain: func() *arenas[user] {
				tail := &arena[user]{
					dl: time.Now().Add(100 * time.Second),
				}

				head := &arena[user]{
					dl:   time.Now().Add(30 * time.Second),
					next: tail,
				}

				tail.prev = head

				return &arenas[user]{
					size: 2,
					head: head,
					tail: tail,
				}
			}(),
		},
		{
			// 测试已经存在两个arena，并插入到第二个arena的中
			name:       "set a new key with has two arenas and insert to the second arena",
			cache:      NewCache[user](),
			key:        "k3",
			val:        user{name: "k3", age: 20, score: 100.9},
			expiration: 50 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 99.9}, 20*time.Second)
				_ = c.Set(ctx, "k2", user{name: "k2", age: 19, score: 98.9}, 70*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 99.9},
					dl:  time.Now().Add(20 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(30 * time.Second),
					},
				},
				"k2": {
					key: "k2",
					val: &user{name: "k2", age: 19, score: 98.9},
					dl:  time.Now().Add(70 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(70 * time.Second),
					},
				},
				"k3": {
					key: "k3",
					val: &user{name: "k3", age: 20, score: 100.9},
					dl:  time.Now().Add(50 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(70 * time.Second),
					},
				},
			},
			wantChain: func() *arenas[user] {
				tail := &arena[user]{
					dl: time.Now().Add(70 * time.Second),
				}

				head := &arena[user]{
					dl:   time.Now().Add(30 * time.Second),
					next: tail,
				}
				tail.prev = head

				return &arenas[user]{
					size: 2,
					head: head,
					tail: tail,
				}
			}(),
		},
		{
			// 测试已经存在两个arena，并在中间创建一个以元素过期时间为准的arena
			name:       "set a new key with has two arenas and create a new arena with expiration",
			cache:      NewCache[user](),
			key:        "k3",
			val:        user{name: "k3", age: 20, score: 100.9},
			expiration: 65 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 99.9}, 20*time.Second)
				_ = c.Set(ctx, "k2", user{name: "k2", age: 19, score: 98.9}, 100*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 99.9},
					dl:  time.Now().Add(20 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(30 * time.Second),
					},
				},
				"k2": {
					key: "k2",
					val: &user{name: "k2", age: 19, score: 98.9},
					dl:  time.Now().Add(100 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(100 * time.Second),
					},
				},
				"k3": {
					key: "k3",
					val: &user{name: "k3", age: 20, score: 100.9},
					dl:  time.Now().Add(65 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(65 * time.Second),
					},
				},
			},
			wantChain: func() *arenas[user] {
				tail := &arena[user]{
					dl: time.Now().Add(100 * time.Second),
				}

				mid := &arena[user]{
					dl:   time.Now().Add(65 * time.Second),
					next: tail,
				}

				head := &arena[user]{
					dl:   time.Now().Add(30 * time.Second),
					next: mid,
				}
				tail.prev = mid
				mid.prev = head

				return &arenas[user]{
					size: 3,
					head: head,
					tail: tail,
				}
			}(),
		},
		{
			// 测试已经存在两个arena，并在中间创建一个以上一个arena过期时间加上默认过期时间为准的arena
			name:       "set a new key with has two arenas and create a new arena with last arena expiration plus default expiration",
			cache:      NewCache[user](),
			key:        "k3",
			val:        user{name: "k3", age: 20, score: 100.9},
			expiration: 55 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 99.9}, 20*time.Second)
				_ = c.Set(ctx, "k2", user{name: "k2", age: 19, score: 98.9}, 100*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 99.9},
					dl:  time.Now().Add(20 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(30 * time.Second),
					},
				},
				"k2": {
					key: "k2",
					val: &user{name: "k2", age: 19, score: 98.9},
					dl:  time.Now().Add(100 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(100 * time.Second),
					},
				},
				"k3": {
					key: "k3",
					val: &user{name: "k3", age: 20, score: 100.9},
					dl:  time.Now().Add(55 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(60 * time.Second),
					},
				},
			},
			wantChain: func() *arenas[user] {
				tail := &arena[user]{
					dl: time.Now().Add(100 * time.Second),
				}

				mid := &arena[user]{
					dl:   time.Now().Add(60 * time.Second),
					next: tail,
				}

				head := &arena[user]{
					dl:   time.Now().Add(30 * time.Second),
					next: mid,
				}
				tail.prev = mid
				mid.prev = head

				return &arenas[user]{
					size: 3,
					head: head,
					tail: tail,
				}
			}(),
		},
		{
			// 测试key存在并且复用了arena
			name:       "set a exist key and reuse arena",
			cache:      NewCache[user](),
			key:        "k1",
			val:        user{name: "k1", age: 18, score: 100.00},
			expiration: 25 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 99.9}, 10*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 100.00},
					dl:  time.Now().Add(25 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(30 * time.Second),
					},
				},
			},
			wantChain: &arenas[user]{
				size: 1,
				head: &arena[user]{
					dl: time.Now().Add(30 * time.Second),
				},
				tail: &arena[user]{
					dl: time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试key存在，但是没有复用arena，而且用元素过期时间作为新的arena的过期时间
			name:       "set a exist key and not reuse arena and use element expiration as new arena expiration",
			cache:      NewCache[user](),
			key:        "k1",
			val:        user{name: "k1", age: 18, score: 100.00},
			expiration: 70 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 99.9}, 20*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 100.00},
					dl:  time.Now().Add(70 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(70 * time.Second),
					},
				},
			},
			wantChain: func() *arenas[user] {
				tail := &arena[user]{
					dl: time.Now().Add(70 * time.Second),
				}

				head := &arena[user]{
					dl:   time.Now().Add(30 * time.Second),
					next: tail,
				}
				tail.prev = head

				return &arenas[user]{
					size: 2,
					head: head,
					tail: tail,
				}
			}(),
		},
		{
			// 测试key存在，但是没有复用arena，而且用上一个arena加上默认过期时间作为新的arena的过期时间
			name:       "set a exist key and not reuse arena and use last arena expiration add default expiration as new arena expiration",
			cache:      NewCache[user](),
			key:        "k1",
			val:        user{name: "k1", age: 18, score: 100.00},
			expiration: 50 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 99.9}, 20*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 100.00},
					dl:  time.Now().Add(50 * time.Second),
					a: &arena[user]{
						dl: time.Now().Add(60 * time.Second),
					},
				},
			},
			wantChain: func() *arenas[user] {
				tail := &arena[user]{
					dl: time.Now().Add(60 * time.Second),
				}

				head := &arena[user]{
					dl:   time.Now().Add(30 * time.Second),
					next: tail,
				}
				tail.prev = head

				return &arenas[user]{
					size: 2,
					head: head,
					tail: tail,
				}
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.beforeFunc(tc.cache)

			_ = tc.cache.Set(ctx, tc.key, tc.val, tc.expiration)

			assert.Equal(t, len(tc.wantIndex), len(tc.cache.index))

			for k, v := range tc.wantIndex {
				assert.Equal(t, v.val, tc.cache.index[k].val)

				assert.InDelta(t, v.dl.Unix(), tc.cache.index[k].dl.Unix(), 1)

				// 验证元素所对于的arena是否正确
				// 由于时间会有误差，所以这里应该近似比较，而不是精确比较
				assert.InDelta(t, v.a.dl.Unix(), tc.cache.index[k].a.dl.Unix(), 1)
			}

			// 比较arena的数量
			assert.Equal(t, tc.wantChain.size, tc.cache.chain.size)

			// 比较链表中arena的过期时间
			if tc.wantChain != nil {
				wh := tc.wantChain.head
				ch := tc.cache.chain.head

				for wh != nil {
					// 由于时间会有误差，所以这里应该近似比较，而不是精确比较
					// 误差在1s以内，就认为是相等的
					assert.InDelta(t, wh.dl.Unix(), ch.dl.Unix(), 1)

					wh = wh.next
					ch = ch.next
				}
			}

		})
	}
}

func TestCache_Get(t *testing.T) {

	ctx := context.TODO()

	testCases := []struct {
		name string

		cache *Cache[user]

		key string

		beforeFunc func(*Cache[user])

		wantBeforeCount int // 调用Get方法前，索引的元素数量，验证清理过期元素是否正确
		wantVal         *user
		wantErr         error
		wantIndex       map[string]*data[user]
	}{
		{
			name:  "get a not exist key",
			cache: NewCache[user](),
			key:   "k1",
			beforeFunc: func(c *Cache[user]) {

			},
			wantBeforeCount: 0,
			wantErr:         errs.ErrKeyNotExist,
		},
		{
			name:  "get a exist key",
			cache: NewCache[user](),
			key:   "k1",
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 100.00}, 10*time.Second)
			},
			wantBeforeCount: 1,
			wantVal:         &user{name: "k1", age: 18, score: 100.00},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 100.00},
					dl:  time.Now().Add(10 * time.Second),
				},
			},
		},
		{
			name:  "get a exist key but expired with lazy clean",
			cache: NewCache[user](),
			key:   "k1",
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 100.00}, 1*time.Second)
				time.Sleep(2 * time.Second)
			},
			wantBeforeCount: 1,
			wantErr:         errs.ErrKeyNotExist,
		},
		{
			name:  "get a exist key but expired and clean with scan goroutine",
			cache: NewCache[user](WithScanInterval[user](2 * time.Second)),
			key:   "k1",
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 100.00}, 1*time.Second)
				time.Sleep(3 * time.Second)
			},
			wantBeforeCount: 0,
			wantErr:         errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.beforeFunc(tc.cache)

			assert.Equal(t, tc.wantBeforeCount, len(tc.cache.index))

			res := tc.cache.Get(ctx, tc.key)

			assert.Equal(t, len(tc.wantIndex), len(tc.cache.index))

			assert.Equal(t, tc.wantErr, res.Err)

			if res.Err != nil {
				return
			}

			assert.Equal(t, tc.wantVal, res.Val)

			for k, v := range tc.wantIndex {
				assert.Equal(t, v.val, tc.cache.index[k].val)

				assert.InDelta(t, v.dl.Unix(), tc.cache.index[k].dl.Unix(), 1)
			}

		})
	}
}

func TestCache_SetNX(t *testing.T) {

	ctx := context.TODO()

	testCases := []struct {
		name string

		cache *Cache[user]

		key        string
		val        user
		expiration time.Duration

		beforeFunc func(*Cache[user])
		wantIndex  map[string]*data[user]
		wantRes    bool
		wantErr    error
	}{
		{
			name:       "set a not exist key",
			cache:      NewCache[user](),
			key:        "k1",
			val:        user{name: "k1", age: 18, score: 100.00},
			expiration: 10 * time.Second,
			beforeFunc: func(c *Cache[user]) {

			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 100.00},
					dl:  time.Now().Add(10 * time.Second),
				},
			},
			wantRes: true,
		},
		{
			name:       "set a exist key and not expired",
			cache:      NewCache[user](),
			key:        "k1",
			val:        user{name: "k1", age: 20, score: 98.99},
			expiration: 10 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 100.00}, 5*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 100.00},
					dl:  time.Now().Add(10 * time.Second),
				},
			},
			wantRes: false,
		},
		{
			name:       "set a exist key and expired",
			cache:      NewCache[user](),
			key:        "k1",
			val:        user{name: "k1", age: 20, score: 98.99},
			expiration: 10 * time.Second,
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 18, score: 100.00}, 1*time.Second)
				time.Sleep(2 * time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 20, score: 98.99},
					dl:  time.Now().Add(12 * time.Second),
				},
			},
			wantRes: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.beforeFunc(tc.cache)

			res, err := tc.cache.SetNX(ctx, tc.key, tc.val, tc.expiration)

			assert.Equal(t, len(tc.wantIndex), len(tc.cache.index))

			assert.Equal(t, tc.wantErr, err)

			if err != nil {
				return
			}

			assert.Equal(t, tc.wantRes, res)

			for k, v := range tc.wantIndex {
				assert.Equal(t, v.val, tc.cache.index[k].val)

				assert.InDelta(t, v.dl.Unix(), tc.cache.index[k].dl.Unix(), 1)
			}

		})
	}
}

func TestCache_GetSet(t *testing.T) {

	ctx := context.TODO()

	testCases := []struct {
		name string

		cache *Cache[user]

		key string
		val user

		beforeFunc func(*Cache[user])
		wantIndex  map[string]*data[user]
		wantVal    user
		wantErr    error
	}{
		{
			name:  "get set a not exist key",
			cache: NewCache[user](),
			key:   "k1",
			val:   user{name: "k1", age: 18, score: 100.00},
			beforeFunc: func(c *Cache[user]) {

			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 100.00},
					dl:  time.Now().Add(30 * time.Second),
				},
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name:  "get set a exist key",
			cache: NewCache[user](),
			key:   "k1",
			val:   user{name: "k1", age: 18, score: 100.00},
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 20, score: 98.99}, 10*time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 100.00},
					dl:  time.Now().Add(10 * time.Second),
				},
			},
			wantVal: user{name: "k1", age: 20, score: 98.99},
		},
		{
			name:  "get set a exist key and expired",
			cache: NewCache[user](),
			key:   "k1",
			val:   user{name: "k1", age: 18, score: 100.00},
			beforeFunc: func(c *Cache[user]) {
				_ = c.Set(ctx, "k1", user{name: "k1", age: 20, score: 98.99}, 1*time.Second)
				time.Sleep(2 * time.Second)
			},
			wantIndex: map[string]*data[user]{
				"k1": {
					key: "k1",
					val: &user{name: "k1", age: 18, score: 100.00},
					dl:  time.Now().Add(32 * time.Second),
				},
			},
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.beforeFunc(tc.cache)

			res := tc.cache.GetSet(ctx, tc.key, tc.val)

			assert.Equal(t, len(tc.wantIndex), len(tc.cache.index))

			for k, v := range tc.wantIndex {
				assert.Equal(t, v.val, tc.cache.index[k].val)

				assert.InDelta(t, v.dl.Unix(), tc.cache.index[k].dl.Unix(), 1)
			}

			assert.Equal(t, tc.wantErr, res.Err)

			if res.Err != nil {
				return
			}

			assert.Equal(t, tc.wantVal, res.Val)
		})
	}
}

type user struct {
	name  string
	age   int8
	score float64
}
