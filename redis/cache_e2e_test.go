// Copyright 2021 ecodeclub
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
