package priority

import (
	"context"
	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCache_Set(t *testing.T) {
	ctx := context.TODO()

	testCases := []struct {
		name string

		cache ecache.Cache

		key        string
		val        any
		expiration time.Duration

		before func(cache ecache.Cache)

		wantIndex map[string]*Node
	}{
		{
			// 测试正常情况
			name:       "test normal set",
			cache:      NewCache(),
			key:        "k1",
			val:        "v1",
			expiration: 30 * time.Second,
			before: func(cache ecache.Cache) {

			},
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v1",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试key已存在的情况
			name:       "test key exists set",
			cache:      NewCache(),
			key:        "k1",
			val:        "v1",
			expiration: 30 * time.Second,
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k1", "v2", 10*time.Second)
			},
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v1",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试淘汰策略
			name:       "test eviction set",
			cache:      NewCache(WithCapacity(3)),
			key:        "k1",
			val:        "v1",
			expiration: 30 * time.Second,
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k2", "v2", 10*time.Second)
				_ = cache.Set(ctx, "k3", "v3", 3*time.Second)
				_ = cache.Set(ctx, "k4", "v4", 5*time.Second)
			},
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v1",
					Dl:  time.Now().Add(30 * time.Second),
				},
				"k2": {
					Key: "k2",
					Val: "v2",
					Dl:  time.Now().Add(10 * time.Second),
				},
				"k4": {
					Key: "k4",
					Val: "v4",
					Dl:  time.Now().Add(5 * time.Second),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(tc.cache)

			_ = tc.cache.Set(ctx, tc.key, tc.val, tc.expiration)

			assert.Equal(t, len(tc.wantIndex), len(tc.cache.(*Cache).index))

			for k, v := range tc.wantIndex {
				assert.Equal(t, v.Val, tc.cache.(*Cache).index[k].Val)

				assert.InDelta(t, v.Dl.Unix(), tc.cache.(*Cache).index[k].Dl.Unix(), 1)
			}
		})
	}
}

func TestCache_SetNX(t *testing.T) {
	ctx := context.TODO()

	testCases := []struct {
		name string

		cache ecache.Cache

		key        string
		val        any
		expiration time.Duration

		before func(cache ecache.Cache)

		wantIndex map[string]*Node
		wantRes   bool
	}{
		{
			// 测试正常情况
			name:       "test normal setnx",
			cache:      NewCache(),
			key:        "k1",
			val:        "v1",
			expiration: 30 * time.Second,
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k2", "v2", 10*time.Second)
			},
			wantRes: true,
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v1",
					Dl:  time.Now().Add(30 * time.Second),
				},
				"k2": {
					Key: "k2",
					Val: "v2",
					Dl:  time.Now().Add(10 * time.Second),
				},
			},
		},
		{
			// 测试key已存在的情况
			name:       "test key exists setnx",
			cache:      NewCache(),
			key:        "k1",
			val:        "v1",
			expiration: 30 * time.Second,
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k1", "v2", 10*time.Second)
			},
			wantRes: false,
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v2",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试淘汰策略
			name:       "test eviction set",
			cache:      NewCache(WithCapacity(3)),
			key:        "k1",
			val:        "v1",
			expiration: 30 * time.Second,
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k2", "v2", 10*time.Second)
				_ = cache.Set(ctx, "k3", "v3", 3*time.Second)
				_ = cache.Set(ctx, "k4", "v4", 5*time.Second)
			},
			wantRes: true,
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v1",
					Dl:  time.Now().Add(30 * time.Second),
				},
				"k2": {
					Key: "k2",
					Val: "v2",
					Dl:  time.Now().Add(10 * time.Second),
				},
				"k4": {
					Key: "k4",
					Val: "v4",
					Dl:  time.Now().Add(5 * time.Second),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(tc.cache)

			ok, _ := tc.cache.SetNX(ctx, tc.key, tc.val, tc.expiration)

			assert.Equal(t, tc.wantRes, ok)

			assert.Equal(t, len(tc.wantIndex), len(tc.cache.(*Cache).index))

			for k, v := range tc.wantIndex {
				assert.Equal(t, v.Val, tc.cache.(*Cache).index[k].Val)

				assert.InDelta(t, v.Dl.Unix(), tc.cache.(*Cache).index[k].Dl.Unix(), 1)
			}
		})
	}
}

func TestCache_Get(t *testing.T) {
	ctx := context.TODO()

	testCases := []struct {
		name string

		cache ecache.Cache

		key string

		before func(cache ecache.Cache)

		wantVal        any
		wantErr        error
		beforeGetIndex map[string]*Node
		wantIndex      map[string]*Node
	}{
		{
			// 测试正常情况
			name:  "test normal get",
			cache: NewCache(),
			key:   "k1",
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k1", "v1", 30*time.Second)
			},
			wantVal: "v1",
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v1",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试key不存在的情况
			name:  "test key not exists get",
			cache: NewCache(),
			key:   "k1",
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k2", "v1", 30*time.Second)
			},
			wantErr: errs.ErrKeyNotExist,
			wantIndex: map[string]*Node{
				"k2": {
					Key: "k2",
					Val: "v1",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试key已存在的情况, 但是key已经过期，并且惰性删除
			name:  "test key exists but expired get and lazy delete",
			cache: NewCache(),
			key:   "k1",
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k1", "v2", 1*time.Second)
				_ = cache.Set(ctx, "k2", "v2", 30*time.Second)
				time.Sleep(2 * time.Second)
			},
			wantErr: errs.ErrKeyNotExist,
			beforeGetIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v2",
					Dl:  time.Now().Add(1 * time.Second),
				},
				"k2": {
					Key: "k2",
					Val: "v2",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
			wantIndex: map[string]*Node{
				"k2": {
					Key: "k2",
					Val: "v2",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试key已存在的情况, 但是key已经过期，并且被扫描删除
			name:  "test key exists but expired get and scan delete",
			cache: NewCache(WithCleanInterval(2 * time.Second)),
			key:   "k1",
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k1", "v2", 1*time.Second)
				_ = cache.Set(ctx, "k2", "v2", 30*time.Second)
				time.Sleep(3 * time.Second)
			},
			wantErr: errs.ErrKeyNotExist,
			beforeGetIndex: map[string]*Node{
				"k2": {
					Key: "k2",
					Val: "v2",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
			wantIndex: map[string]*Node{
				"k2": {
					Key: "k2",
					Val: "v2",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(tc.cache)

			for k, v := range tc.beforeGetIndex {
				assert.Equal(t, v.Val, tc.cache.(*Cache).index[k].Val)

				assert.InDelta(t, v.Dl.Unix(), tc.cache.(*Cache).index[k].Dl.Unix(), 2)
			}

			res := tc.cache.Get(ctx, tc.key)

			assert.Equal(t, len(tc.wantIndex), len(tc.cache.(*Cache).index))

			for k, v := range tc.wantIndex {
				assert.Equal(t, v.Val, tc.cache.(*Cache).index[k].Val)

				assert.InDelta(t, v.Dl.Unix(), tc.cache.(*Cache).index[k].Dl.Unix(), 2)
			}

			assert.Equal(t, tc.wantErr, res.Err)

			if res.Err != nil {
				return
			}

			assert.Equal(t, tc.wantVal, res.Val)
		})
	}
}

func TestCache_GetSet(t *testing.T) {
	ctx := context.TODO()

	testCases := []struct {
		name string

		cache ecache.Cache

		key string
		val string

		before func(cache ecache.Cache)

		wantVal   any
		wantErr   error
		wantIndex map[string]*Node
	}{
		{
			// 测试正常情况
			name:  "test normal getset",
			cache: NewCache(),
			key:   "k1",
			val:   "v2",
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k1", "v1", 30*time.Second)
			},
			wantVal: "v1",
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v2",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试key不存在的情况
			name:  "test key not exists getset",
			cache: NewCache(),
			key:   "k1",
			val:   "v2",
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k2", "v1", 30*time.Second)
			},
			wantErr: errs.ErrKeyNotExist,
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v2",
					Dl:  time.Now().Add(30 * time.Second),
				},
				"k2": {
					Key: "k2",
					Val: "v1",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
		{
			// 测试key已存在的情况, 但是key已经过期
			name:  "test key exists but expired getset",
			cache: NewCache(),
			key:   "k1",
			val:   "v3",
			before: func(cache ecache.Cache) {
				_ = cache.Set(ctx, "k1", "v2", 1*time.Second)
				_ = cache.Set(ctx, "k2", "v2", 30*time.Second)
				time.Sleep(2 * time.Second)
			},
			wantErr: errs.ErrKeyNotExist,
			wantIndex: map[string]*Node{
				"k1": {
					Key: "k1",
					Val: "v3",
					Dl:  time.Now().Add(32 * time.Second),
				},
				"k2": {
					Key: "k2",
					Val: "v2",
					Dl:  time.Now().Add(30 * time.Second),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(tc.cache)

			res := tc.cache.GetSet(ctx, tc.key, tc.val)

			assert.Equal(t, len(tc.wantIndex), len(tc.cache.(*Cache).index))

			for k, v := range tc.wantIndex {
				assert.Equal(t, v.Val, tc.cache.(*Cache).index[k].Val)

				assert.InDelta(t, v.Dl.Unix(), tc.cache.(*Cache).index[k].Dl.Unix(), 1)
			}

			assert.Equal(t, tc.wantErr, res.Err)

			if res.Err != nil {
				return
			}

			assert.Equal(t, tc.wantVal, res.Val)
		})
	}
}
