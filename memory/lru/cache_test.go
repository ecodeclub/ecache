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
	"testing"
	"time"

	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ecache/mocks"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCache_Set(t *testing.T) {
	testCases := []struct {
		name string

		mock func(*gomock.Controller) simplelru.LRUCache[string, any]

		key        string
		value      string
		expiration time.Duration
		wantErr    error
	}{
		{
			name: "set value",
			mock: func(ctrl *gomock.Controller) simplelru.LRUCache[string, any] {
				cmd := mocks.NewMockLRUCache[string, any](ctrl)
				cmd.EXPECT().
					Add("name", "set value").
					Return(false)
				return cmd
			},
			key:        "name",
			value:      "set value",
			expiration: time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			c := NewCache(tc.mock(ctrl))
			err := c.Set(context.Background(), tc.key, tc.value, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestTestCache_Get(t *testing.T) {
	testCases := []struct {
		name string

		mock func(*gomock.Controller) simplelru.LRUCache[string, any]

		key     string
		wantErr error
		wantVal string
	}{
		{
			name: "get value",
			mock: func(ctrl *gomock.Controller) simplelru.LRUCache[string, any] {
				cmd := mocks.NewMockLRUCache[string, any](ctrl)
				cmd.EXPECT().
					Get("name").
					Return("get value", true)
				return cmd
			},
			key:     "name",
			wantVal: "get value",
		},
		{
			name: "get value not key",
			mock: func(ctrl *gomock.Controller) simplelru.LRUCache[string, any] {
				cmd := mocks.NewMockLRUCache[string, any](ctrl)
				cmd.EXPECT().
					Get("name").
					Return("", false)
				return cmd
			},
			key:     "name",
			wantVal: "",
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			c := NewCache(tc.mock(ctrl))
			val := c.Get(context.Background(), tc.key)
			assert.Equal(t, tc.wantErr, val.Err)
			assert.Equal(t, tc.wantVal, val.Val)
		})
	}
}

func TestCache_SetNX(t *testing.T) {
	testCase := []struct {
		name       string
		mock       func(*gomock.Controller) simplelru.LRUCache[string, any]
		key        string
		val        string
		expiration time.Duration
		wantVal    bool
	}{
		{
			name: "setnx value",
			mock: func(ctrl *gomock.Controller) simplelru.LRUCache[string, any] {
				cmd := mocks.NewMockLRUCache[string, any](ctrl)
				cmd.EXPECT().
					Contains("setnx_key").
					Return(false)

				cmd.EXPECT().
					Add("setnx_key", "hello ecache").
					Return(true)
				return cmd
			},
			key:        "setnx_key",
			val:        "hello ecache",
			expiration: time.Second * 10,
			wantVal:    true,
		},
		{
			name: "setnx error",
			mock: func(ctrl *gomock.Controller) simplelru.LRUCache[string, any] {
				cmd := mocks.NewMockLRUCache[string, any](ctrl)
				cmd.EXPECT().
					Contains("setnx_key").
					Return(true)
				return cmd
			},
			key:        "setnx_key",
			val:        "hello ecache",
			expiration: time.Second * 10,
			wantVal:    false,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := NewCache(tc.mock(ctrl))
			val, err := c.SetNX(context.Background(), tc.key, tc.val, tc.expiration)
			require.NoError(t, err)
			assert.Equal(t, tc.wantVal, val)
		})
	}
}

func TestCache_GetSet(t *testing.T) {
	testCase := []struct {
		name    string
		mock    func(*gomock.Controller) simplelru.LRUCache[string, any]
		key     string
		val     string
		wantVal string
		wantErr error
	}{
		{
			name: "getset value",
			mock: func(ctrl *gomock.Controller) simplelru.LRUCache[string, any] {
				cmd := mocks.NewMockLRUCache[string, any](ctrl)
				cmd.EXPECT().
					Get("test_get_set").
					Return("hello world", true)

				cmd.EXPECT().
					Add("test_get_set", "hello ecache").
					Return(true)
				return cmd
			},
			key:     "test_get_set",
			val:     "hello ecache",
			wantVal: "hello world",
		},
		{
			name: "getset error",
			mock: func(ctrl *gomock.Controller) simplelru.LRUCache[string, any] {
				cmd := mocks.NewMockLRUCache[string, any](ctrl)
				cmd.EXPECT().
					Get("test_get_set").
					Return("", false)

				cmd.EXPECT().
					Add("test_get_set", "hello ecache").
					Return(true)

				return cmd
			},
			key:     "test_get_set",
			val:     "hello ecache",
			wantVal: "",
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCase {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := NewCache(tc.mock(ctrl))
		val := c.GetSet(context.Background(), tc.key, tc.val)
		assert.Equal(t, tc.wantVal, val.Val)
		assert.Equal(t, tc.wantErr, val.Err)
	}
}
