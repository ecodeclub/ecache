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

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ecache/mocks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestCache_Set(t *testing.T) {
	testCases := []struct {
		name string

		mock func(*gomock.Controller) redis.Cmdable

		key        string
		value      string
		expiration time.Duration

		wantErr error
	}{
		{
			name: "set value",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				status := redis.NewStatusCmd(context.Background())
				status.SetVal("OK")
				cmd.EXPECT().
					Set(context.Background(), "name", "大明", time.Minute).
					Return(status)
				return cmd
			},
			key:        "name",
			value:      "大明",
			expiration: time.Minute,
		},
		{
			name: "timeout",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				status := redis.NewStatusCmd(context.Background())
				status.SetErr(context.DeadlineExceeded)
				cmd.EXPECT().
					Set(context.Background(), "name", "大明", time.Minute).
					Return(status)
				return cmd
			},
			key:        "name",
			value:      "大明",
			expiration: time.Minute,

			wantErr: context.DeadlineExceeded,
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

func TestCache_Get(t *testing.T) {
	testCases := []struct {
		name string

		mock func(*gomock.Controller) redis.Cmdable

		key string

		wantErr error
		wantVal string
	}{
		{
			name: "get value",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				status := redis.NewStringCmd(context.Background())
				status.SetVal("大明")
				cmd.EXPECT().
					Get(context.Background(), "name").
					Return(status)
				return cmd
			},
			key: "name",

			wantVal: "大明",
		},
		{
			name: "get error",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				status := redis.NewStringCmd(context.Background())
				status.SetErr(redis.Nil)
				cmd.EXPECT().
					Get(context.Background(), "name").
					Return(status)
				return cmd
			},
			key: "name",

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
			if val.Err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val.Val.(string))
		})
	}
}

func TestCache_SetNX(t *testing.T) {
	testCase := []struct {
		name       string
		mock       func(*gomock.Controller) redis.Cmdable
		key        string
		val        string
		expiration time.Duration
		result     bool
	}{
		{
			name: "setnx value",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				boolCmd := redis.NewBoolCmd(context.Background())
				boolCmd.SetVal(true)
				cmd.EXPECT().
					SetNX(context.Background(), "setnx_key", "hello ecache", time.Second*10).
					Return(boolCmd)
				return cmd
			},
			key:        "setnx_key",
			val:        "hello ecache",
			expiration: time.Second * 10,
			result:     true,
		},
		{
			name: "setnx error",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				boolCmd := redis.NewBoolCmd(context.Background())
				boolCmd.SetVal(false)
				cmd.EXPECT().
					SetNX(context.Background(), "setnx-key", "hello ecache", time.Second*10).
					Return(boolCmd)

				return cmd
			},
			key:        "setnx-key",
			val:        "hello ecache",
			expiration: time.Second * 10,
			result:     false,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := NewCache(tc.mock(ctrl))
			val, err := c.SetNX(context.Background(), tc.key, tc.val, tc.expiration)
			require.NoError(t, err)
			assert.Equal(t, tc.result, val)
		})
	}
}

func TestCache_GetSet(t *testing.T) {
	testCase := []struct {
		name    string
		mock    func(*gomock.Controller) redis.Cmdable
		key     string
		val     string
		wantErr error
	}{
		{
			name: "getset value",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				str := redis.NewStringCmd(context.Background())
				str.SetVal("hello ecache")
				cmd.EXPECT().
					GetSet(context.Background(), "test_get_set", "hello go").
					Return(str)
				return cmd
			},
			key: "test_get_set",
			val: "hello go",
		},
		{
			name: "getset error",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				str := redis.NewStringCmd(context.Background())
				str.SetErr(redis.Nil)
				cmd.EXPECT().
					GetSet(context.Background(), "test_get_set_err", "hello ecache").
					Return(str)
				return cmd
			},
			key:     "test_get_set_err",
			val:     "hello ecache",
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCase {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := NewCache(tc.mock(ctrl))
		val := c.GetSet(context.Background(), tc.key, tc.val)
		assert.Equal(t, tc.wantErr, val.Err)
	}
}

func TestCache_LPush(t *testing.T) {
	testCase := []struct {
		name    string
		mock    func(*gomock.Controller) redis.Cmdable
		key     string
		val     []any
		wantVal int64
		wantErr error
	}{
		{
			name: "lpush value",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				result := redis.NewIntCmd(context.Background())
				result.SetVal(2)
				cmd.EXPECT().
					LPush(context.Background(), "test_list_push", "1", "2").
					Return(result)
				return cmd
			},
			key:     "test_list_push",
			val:     []any{"1", "2"},
			wantVal: 2,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := NewCache(tc.mock(ctrl))
			length, err := c.LPush(context.Background(), tc.key, tc.val...)
			assert.Equal(t, tc.wantVal, length)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestCache_LPop(t *testing.T) {
	testCase := []struct {
		name    string
		mock    func(*gomock.Controller) redis.Cmdable
		key     string
		wantVal string
		wantErr error
	}{
		{
			name: "lpop value",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				str := redis.NewStringCmd(context.Background())
				str.SetVal("test")
				cmd.EXPECT().
					LPop(context.Background(), "test_cache_lpop").
					Return(str)
				return cmd
			},
			key:     "test_cache_lpop",
			wantVal: "test",
		},
		{
			name: "lpop error",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				str := redis.NewStringCmd(context.Background())
				str.SetErr(redis.Nil)
				cmd.EXPECT().
					LPop(context.Background(), "test_cache_lpop").
					Return(str)
				return cmd
			},
			key:     "test_cache_lpop",
			wantVal: "",
			wantErr: errs.ErrKeyNotExist,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := NewCache(tc.mock(ctrl))
			val := c.LPop(context.Background(), tc.key)
			assert.Equal(t, tc.wantVal, val.Val)
			assert.Equal(t, tc.wantErr, val.Err)
		})
	}
}

func TestCache_SAdd(t *testing.T) {
	testCase := []struct {
		name    string
		mock    func(*gomock.Controller) redis.Cmdable
		key     string
		val     []any
		wantVal int64
		wantErr error
	}{
		{
			name: "sadd value",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				result := redis.NewIntCmd(context.Background())
				result.SetVal(2)
				cmd.EXPECT().
					SAdd(context.Background(), "test_sadd", "hello ecache", "hello go").
					Return(result)
				return cmd
			},
			key:     "test_sadd",
			val:     []any{"hello ecache", "hello go"},
			wantVal: 2,
		},
		{
			name: "sadd ignore",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				result := redis.NewIntCmd(context.Background())
				result.SetVal(1)
				cmd.EXPECT().
					SAdd(context.Background(), "test_sadd", "hello", "hello").
					Return(result)
				return cmd
			},
			key:     "test_sadd",
			val:     []any{"hello", "hello"},
			wantVal: 1,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := NewCache(tc.mock(ctrl))
			length, err := c.SAdd(context.Background(), tc.key, tc.val...)
			assert.Equal(t, length, tc.wantVal)
			assert.Equal(t, err, tc.wantErr)
		})
	}
}

func TestCache_SRem(t *testing.T) {
	testCase := []struct {
		name    string
		mock    func(*gomock.Controller) redis.Cmdable
		key     string
		val     []any
		wantVal int64
		wantErr error
	}{
		{
			name: "srem value",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				result := redis.NewIntCmd(context.Background())
				result.SetVal(2)
				cmd.EXPECT().
					SRem(context.Background(), "test_srem", "hello", "hello go").
					Return(result)
				return cmd
			},
			key:     "test_srem",
			val:     []any{"hello", "hello go"},
			wantVal: 2,
		},
		{
			name: "srem ignore",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				result := redis.NewIntCmd(context.Background())
				result.SetVal(0)
				cmd.EXPECT().
					SRem(context.Background(), "test_srem", "hello").
					Return(result)
				return cmd
			},
			key:     "test_srem",
			val:     []any{"hello"},
			wantVal: 0,
		},
		{
			name: "srem error",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				result := redis.NewIntCmd(context.Background())
				result.SetVal(0)
				result.SetErr(nil)
				cmd.EXPECT().
					SRem(context.Background(), "test_srem", "hello").
					Return(result)
				return cmd
			},
			key:     "test_srem",
			val:     []any{"hello"},
			wantVal: 0,
			wantErr: nil,
		},
		{
			name: "srem section ignore",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				result := redis.NewIntCmd(context.Background())
				result.SetVal(1)
				cmd.EXPECT().
					SRem(context.Background(), "test_srem", "hello", "go").
					Return(result)
				return cmd
			},
			key:     "test_srem",
			val:     []any{"hello", "go"},
			wantVal: 1,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := NewCache(tc.mock(ctrl))
			result := c.SRem(context.Background(), tc.key, tc.val...)
			assert.Equal(t, result.Val, tc.wantVal)
			assert.Equal(t, result.Err, tc.wantErr)
		})
	}
}
