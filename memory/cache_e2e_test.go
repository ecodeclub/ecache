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

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_e2e_Set(t *testing.T) {
	lru := expirable.NewLRU[string, any](5, nil, time.Second*10)

	testCase := []struct {
		name  string
		after func(t *testing.T)

		key        string
		val        string
		expiration time.Duration

		wantErr error
	}{
		{
			name: "set e2e value",
			after: func(t *testing.T) {
				result, ok := lru.Get("test")
				assert.Equal(t, true, ok)
				assert.Equal(t, "hello ecache", result.(string))
				assert.Equal(t, true, lru.Remove("test"))
			},
			key:        "test",
			val:        "hello ecache",
			expiration: time.Minute,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(lru)

			err := c.Set(ctx, tc.key, tc.val, tc.expiration)
			require.NoError(t, err)
			tc.after(t)
		})
	}
}

func TestCache_e2e_Get(t *testing.T) {
	lru := expirable.NewLRU[string, any](5, nil, time.Second*10)

	testCase := []struct {
		name   string
		before func(t *testing.T)
		after  func(t *testing.T)

		key string

		wantVal string
		wantErr error
	}{
		{
			name: "get e2e value",
			before: func(t *testing.T) {
				assert.Equal(t, false, lru.Add("test", "hello ecache"))
			},
			after: func(t *testing.T) {
				assert.Equal(t, true, lru.Remove("test"))
			},
			key:     "test",
			wantVal: "hello ecache",
		},
		{
			name:    "get e2e value err",
			before:  func(t *testing.T) {},
			after:   func(t *testing.T) {},
			key:     "test",
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(lru)

			tc.before(t)
			result := c.Get(ctx, tc.key)
			val, err := result.String()
			assert.Equal(t, tc.wantVal, val)
			assert.Equal(t, tc.wantErr, err)
			tc.after(t)
		})
	}
}

func TestCache_e2e_SetNX(t *testing.T) {
	lru := expirable.NewLRU[string, any](5, nil, time.Second*10)

	testCase := []struct {
		name   string
		before func(t *testing.T)
		after  func(t *testing.T)

		key     string
		val     string
		expire  time.Duration
		wantVal bool
	}{
		{
			name:   "setnx e2e value",
			before: func(t *testing.T) {},
			after: func(t *testing.T) {
				assert.Equal(t, true, lru.Remove("test"))
			},
			key:     "test",
			val:     "hello ecache",
			expire:  time.Minute,
			wantVal: true,
		},
		{
			name: "setnx e2e value exist",
			before: func(t *testing.T) {
				assert.Equal(t, false, lru.Add("test", "hello ecache"))
			},
			after: func(t *testing.T) {
				assert.Equal(t, true, lru.Remove("test"))
			},
			key:     "test",
			val:     "hello world",
			expire:  time.Minute,
			wantVal: false,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(lru)

			tc.before(t)
			result, err := c.SetNX(ctx, tc.key, tc.val, tc.expire)
			assert.Equal(t, tc.wantVal, result)
			require.NoError(t, err)
			tc.after(t)
		})
	}
}

func TestCache_e2e_GetSet(t *testing.T) {
	lru := expirable.NewLRU[string, any](5, nil, time.Second*10)

	testCase := []struct {
		name   string
		before func(t *testing.T)
		after  func(t *testing.T)

		key     string
		val     string
		wantVal string
		wantErr error
	}{
		{
			name: "getset e2e value",
			before: func(t *testing.T) {
				assert.Equal(t, false, lru.Add("test", "hello ecache"))
			},
			after: func(t *testing.T) {
				result, ok := lru.Get("test")
				assert.Equal(t, true, ok)
				assert.Equal(t, "hello world", result)
				assert.Equal(t, true, lru.Remove("test"))
			},
			key:     "test",
			val:     "hello world",
			wantVal: "hello ecache",
		},
		{
			name:   "getset e2e value not key error",
			before: func(t *testing.T) {},
			after: func(t *testing.T) {
				result, ok := lru.Get("test")
				assert.Equal(t, true, ok)
				assert.Equal(t, "hello world", result)
				assert.Equal(t, true, lru.Remove("test"))
			},
			key:     "test",
			val:     "hello world",
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
			defer cancelFunc()
			c := NewCache(lru)

			tc.before(t)
			result := c.GetSet(ctx, tc.key, tc.val)
			val, err := result.String()
			assert.Equal(t, tc.wantVal, val)
			assert.Equal(t, tc.wantErr, err)
			tc.after(t)
		})
	}
}
