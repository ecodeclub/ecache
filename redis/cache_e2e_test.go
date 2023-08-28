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
			name: "set value",
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
			name: "get value",
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
			name:    "get error",
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
			name:   "test setnx",
			before: func(ctx context.Context, t *testing.T) {},
			after: func(ctx context.Context, t *testing.T) {
				assert.NoError(t, rdb.Del(context.Background(), "testnx").Err())
			},
			key:     "testnx",
			val:     "test0001",
			wantVal: true,
		},
		{
			name: "test setnx fail",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.SetNX(ctx, "testnxf", "hello ecache", time.Minute).Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Del(ctx, "testnxf").Err())
			},
			key:     "testnxf",
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
			name: "test_get_set",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Set(context.Background(), "test_get_set", "hello ecache", time.Second*10).Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Del(context.Background(), "test_get_set").Err())
			},
			key:     "test_get_set",
			val:     "hello go",
			expire:  time.Second * 10,
			wantVal: "hello ecache",
		},
		{
			name:   "test_get_set",
			before: func(ctx context.Context, t *testing.T) {},
			after: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Del(context.Background(), "test_get_set").Err())
			},
			key:     "test_get_set",
			val:     "hello key notfound",
			expire:  time.Second * 10,
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
			name:   "test_cache_lpush",
			before: func(ctx context.Context, t *testing.T) {},
			after: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Del(context.Background(), "test_cache_lpush").Err())
			},
			key:     "test_cache_lpush",
			val:     []any{"1", "2"},
			wantVal: 2,
		},
		{
			name: "test_cache_lpush",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.LPush(context.Background(), "test_cache_lpush", "hello ecache", "hello go").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
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
			name: "test_cache_pop",
			before: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.LPush(context.Background(), "test_cache_pop", "1", "2", "3", "4").Err())
			},
			after: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Del(context.Background(), "test_cache_pop").Err())
			},
			key:     "test_cache_pop",
			wantVal: "4",
		},
		{
			name:   "test_cache_pop",
			before: func(ctx context.Context, t *testing.T) {},
			after: func(ctx context.Context, t *testing.T) {
				require.NoError(t, rdb.Del(context.Background(), "test_cache_pop").Err())
			},
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
