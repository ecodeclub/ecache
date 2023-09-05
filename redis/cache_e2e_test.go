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

//go:build e2e

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_e2e_Set(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCases := []struct {
		name  string
		after func(ctx context.Context, t *testing.T)

		key        string
		val        string
		expiration time.Duration

		wantErr error
	}{
		{
			name: "set e2e value",
			after: func(ctx context.Context, t *testing.T) {
				result, err := rdb.Get(ctx, "name").Result()
				require.NoError(t, err)
				assert.Equal(t, "大明", result)

				_, err = rdb.Del(ctx, "name").Result()
				require.NoError(t, err)
			},
			key:        "name",
			val:        "大明",
			expiration: time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)

			err := c.Set(ctx, "name", "大明", time.Minute)
			assert.NoError(t, err)
			tc.after(ctx, t)
		})
	}
}

func TestCache_e2e_Get(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCases := []struct {
		name   string
		before func(ctx context.Context, t *testing.T)
		after  func(ctx context.Context, t *testing.T)

		key string

		wantVal string
		wantErr error
	}{
		{
			name: "get e2e value",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Set(ctx, "name", "大明", time.Minute).Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Del(ctx, "name").Err())
			},
			key: "name",

			wantVal: "大明",
		},
		{
			name:    "get e2e error",
			key:     "name",
			before:  func(ctx context.Context, t *testing.T) {},
			after:   func(ctx context.Context, t *testing.T) {},
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)

			tc.before(ctx, t)
			val := c.Get(ctx, tc.key)
			assert.Equal(t, tc.wantErr, val.Err)
			if val.Err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val.Val.(string))
			tc.after(ctx, t)
		})
	}
}

func TestCache_e2e_SetNX(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCase := []struct {
		name   string
		before func(ctx context.Context, t *testing.T)
		after  func(ctx context.Context, t *testing.T)

		key     string
		val     string
		expire  time.Duration
		wantVal bool
	}{
		{
			name:   "setnx e2e value",
			before: func(ctx context.Context, t *testing.T) {},
			after: func(ctx context.Context, t *testing.T) {
				assert.NoError(t, rdb.Del(context.Background(), "testnx").Err())
			},
			key:     "testnx",
			val:     "test0001",
			wantVal: true,
		},
		{
			name: "setnx e2e fail",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.SetNX(ctx, "testnx", "hello ecache", time.Minute).Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Del(ctx, "testnx").Err())
			},
			key:     "testnx",
			val:     "hello go",
			wantVal: false,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)
			tc.before(ctx, t)
			result, err := c.SetNX(ctx, tc.key, tc.val, tc.expire)
			assert.NoError(t, err)
			assert.Equal(t, result, tc.wantVal)
			tc.after(ctx, t)
		})
	}
}

func TestCache_e2e_GetSet(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCase := []struct {
		name   string
		before func(ctx context.Context, t *testing.T)
		after  func(ctx context.Context, t *testing.T)

		key     string
		val     string
		expire  time.Duration
		wantVal string
		wantErr error
	}{
		{
			name: "getset e2e value",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Set(context.Background(), "test_get_set", "hello ecache", time.Second*10).Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, "hello go", rdb.Get(context.Background(), "test_get_set").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_get_set").Err())
			},
			key:     "test_get_set",
			val:     "hello go",
			expire:  time.Second * 10,
			wantVal: "hello ecache",
		},
		{
			name:   "getset e2e err",
			before: func(ctx context.Context, t *testing.T) {},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, "hello key notfound", rdb.Get(context.Background(), "test_get_set").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_get_set").Err())
			},
			key:     "test_get_set",
			val:     "hello key notfound",
			expire:  time.Second * 10,
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)
			tc.before(ctx, t)
			val := c.GetSet(ctx, tc.key, tc.val)
			assert.Equal(t, val.Val, tc.wantVal)
			assert.Equal(t, val.Err, tc.wantErr)
			tc.after(ctx, t)
		})
	}
}

func TestCache_e2e_LPush(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCase := []struct {
		name    string
		before  func(ctx context.Context, t *testing.T)
		after   func(ctx context.Context, t *testing.T)
		key     string
		val     []any
		wantVal int64
	}{
		{
			name:   "lpush e2e value",
			before: func(ctx context.Context, t *testing.T) {},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(2), rdb.LLen(context.Background(), "test_cache_lpush").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_cache_lpush").Err())
			},
			key:     "test_cache_lpush",
			val:     []any{"1", "2"},
			wantVal: 2,
		},
		{
			name: "lpush e2e want value",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.LPush(context.Background(), "test_cache_lpush", "hello ecache", "hello go").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(4), rdb.LLen(context.Background(), "test_cache_lpush").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_cache_lpush").Err())
			},
			key:     "test_cache_lpush",
			val:     []any{"123", "saaa"},
			wantVal: 4,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)
			tc.before(ctx, t)
			val, err := c.LPush(ctx, tc.key, tc.val...)
			require.NoError(t, err)
			assert.Equal(t, val, tc.wantVal)
			tc.after(ctx, t)
		})
	}
}

func TestCache_e2e_LPop(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCase := []struct {
		name    string
		before  func(ctx context.Context, t *testing.T)
		after   func(ctx context.Context, t *testing.T)
		key     string
		wantVal any
		wantErr error
	}{
		{
			name: "lpop e2e value",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.LPush(context.Background(), "test_cache_pop", "1", "2", "3", "4").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(3), rdb.LLen(context.Background(), "test_cache_pop").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_cache_pop").Err())
			},
			key:     "test_cache_pop",
			wantVal: "4",
		},
		{
			name: "lpop e2e one value",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.LPush(context.Background(), "test_cache_pop", "1").Err())
				require.NoError(t, rdb.LPop(context.Background(), "test_cache_pop").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Del(context.Background(), "test_cache_pop").Err())
			},
			key:     "test_cache_pop",
			wantVal: "",
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name:    "lpop e2e err",
			before:  func(ctx context.Context, t *testing.T) {},
			after:   func(ctx context.Context, t *testing.T) {},
			key:     "test_cache_pop",
			wantVal: "",
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)
			tc.before(ctx, t)
			val := c.LPop(ctx, tc.key)
			assert.Equal(t, val.Val, tc.wantVal)
			assert.Equal(t, val.Err, tc.wantErr)
			tc.after(ctx, t)
		})
	}
}

func TestCache_e2e_SAdd(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCase := []struct {
		name    string
		before  func(ctx context.Context, t *testing.T)
		after   func(ctx context.Context, t *testing.T)
		key     string
		val     []any
		wantVal int64
		wantErr error
	}{
		{
			name:   "sadd e2e value",
			before: func(ctx context.Context, t *testing.T) {},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(2), rdb.SCard(context.Background(), "test_e2e_sadd").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_sadd").Err())
			},
			key:     "test_e2e_sadd",
			val:     []any{"hello ecache", "hello go"},
			wantVal: 2,
		},
		{
			name: "sadd e2e ignore",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.SAdd(context.Background(), "test_e2e_sadd", "hello").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(1), rdb.SCard(context.Background(), "test_e2e_sadd").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_sadd").Err())
			},
			key:     "test_e2e_sadd",
			val:     []any{"hello"},
			wantVal: 0,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)
			tc.before(ctx, t)
			val, err := c.SAdd(ctx, tc.key, tc.val...)
			assert.Equal(t, val, tc.wantVal)
			assert.Equal(t, err, tc.wantErr)
			tc.after(ctx, t)
		})
	}
}

func TestCache_e2e_SRem(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCase := []struct {
		name    string
		before  func(ctx context.Context, t *testing.T)
		after   func(ctx context.Context, t *testing.T)
		key     string
		val     []any
		wantVal int64
		wantErr error
	}{
		{
			name: "srem e2e value",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.SAdd(context.Background(), "test_e2e_srem", "hello", "ecache").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(1), rdb.SCard(context.Background(), "test_e2e_srem").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_srem").Err())
			},
			key:     "test_e2e_srem",
			val:     []any{"hello"},
			wantVal: 1,
		},
		{
			name: "srem e2e nil",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.SAdd(context.Background(), "test_e2e_srem", "hello", "ecache").Err())
				require.NoError(t, rdb.SRem(context.Background(), "test_e2e_srem", "hello", "ecache").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(0), rdb.SCard(context.Background(), "test_e2e_srem").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_srem").Err())
			},
			key: "test_e2e_srem",
			val: []any{"hello"},
		},
		{
			name: "srem e2e ignore",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.SAdd(context.Background(), "test_e2e_srem", "hello", "ecache").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(2), rdb.SCard(context.Background(), "test_e2e_srem").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_srem").Err())
			},
			key:     "test_e2e_srem",
			val:     []any{"go"},
			wantVal: 0,
			wantErr: nil,
		},
		{
			name:    "srem e2e key nil",
			before:  func(ctx context.Context, t *testing.T) {},
			after:   func(ctx context.Context, t *testing.T) {},
			key:     "test_e2e_srem",
			val:     []any{"ecache"},
			wantVal: 0,
			wantErr: nil,
		},
		{
			name: "srem e2e section ignore",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.SAdd(context.Background(), "test_e2e_srem", "hello", "ecache").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(1), rdb.SCard(context.Background(), "test_e2e_srem").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_srem").Err())
			},
			key:     "test_e2e_srem",
			val:     []any{"hello", "go"},
			wantVal: 1,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)
			tc.before(ctx, t)
			val := c.SRem(ctx, tc.key, tc.val...)
			assert.Equal(t, val.Val, tc.wantVal)
			assert.Equal(t, val.Err, tc.wantErr)
			tc.after(ctx, t)
		})
	}
}

func TestCache_e2e_IncrBy(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCase := []struct {
		name    string
		before  func(ctx context.Context, t *testing.T)
		after   func(ctx context.Context, t *testing.T)
		key     string
		val     int64
		wantVal int64
		wantErr error
	}{
		{
			name: "cache e2e incrby",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Set(context.Background(), "test_e2e_incr", 1, time.Second*10).Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, "2", rdb.Get(context.Background(), "test_e2e_incr").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_incr").Err())
			},
			key:     "test_e2e_incr",
			val:     1,
			wantVal: 2,
		},
		{
			name: "cache e2e incrby not exists",
			before: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(0), rdb.Exists(context.Background(), "test_e2e_incr").Val())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, "1", rdb.Get(context.Background(), "test_e2e_incr").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_incr").Err())
			},
			key:     "test_e2e_incr",
			val:     1,
			wantVal: 1,
		},
		{
			name: "cache e2e incrby set value",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Set(context.Background(), "test_e2e_incr", 10, time.Second*10).Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, "12", rdb.Get(context.Background(), "test_e2e_incr").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_incr").Err())
			},
			key:     "test_e2e_incr",
			val:     2,
			wantVal: 12,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)
			tc.before(ctx, t)
			val, err := c.IncrBy(ctx, tc.key, tc.val)
			assert.Equal(t, val, tc.wantVal)
			assert.Equal(t, err, tc.wantErr)
			tc.after(ctx, t)
		})
	}
}

func TestCache_e2e_DecrBy(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	require.NoError(t, rdb.Ping(context.Background()).Err())

	testCase := []struct {
		name    string
		before  func(ctx context.Context, t *testing.T)
		after   func(ctx context.Context, t *testing.T)
		key     string
		val     int64
		wantVal int64
		wantErr error
	}{
		{
			name: "cache e2e decrby",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Set(context.Background(), "test_e2e_decr", 1, time.Second*10).Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, "0", rdb.Get(context.Background(), "test_e2e_decr").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_decr").Err())
			},
			key:     "test_e2e_decr",
			val:     1,
			wantVal: 0,
		},
		{
			name: "cache e2e decrby not exists",
			before: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, int64(0), rdb.Exists(context.Background(), "test_e2e_decr").Val())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, "-1", rdb.Get(context.Background(), "test_e2e_decr").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_decr").Err())
			},
			key:     "test_e2e_decr",
			val:     1,
			wantVal: -1,
		},
		{
			name: "cache e2e decrby set value",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Set(context.Background(), "test_e2e_decr", 10, time.Second*10).Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				assert.Equal(t, "8", rdb.Get(context.Background(), "test_e2e_decr").Val())
				require.NoError(t, rdb.Del(context.Background(), "test_e2e_decr").Err())
			},
			key:     "test_e2e_decr",
			val:     2,
			wantVal: 8,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(rdb)
			tc.before(ctx, t)
			val, err := c.DecrBy(ctx, tc.key, tc.val)
			assert.Equal(t, val, tc.wantVal)
			assert.Equal(t, err, tc.wantErr)
			tc.after(ctx, t)
		})
	}
}
