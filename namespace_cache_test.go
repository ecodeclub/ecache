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

package ecache

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit"
	"go.uber.org/mock/gomock"
)

// 所有测试的目的在于检验namespacecache有没有正确处理key
func TestNamespaceCache_DecrBy(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx   context.Context
		key   string
		value int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "test_decrby",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx:   context.Background(),
				key:   "key",
				value: 1,
			},
			want:    1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().DecrBy(tt.args.ctx, tt.fields.Namespace+tt.args.key, tt.args.value).Return(tt.want, nil)
			got, err := c.DecrBy(tt.args.ctx, tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecrBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DecrBy() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceCache_Delete(t *testing.T) {
	tests := []struct {
		name      string
		keys      []string
		mock      func(ctrl *gomock.Controller) Cache
		wantCnt   int64
		wantError bool
	}{
		{
			name:      "test_delete",
			keys:      []string{"key1", "key2"},
			wantCnt:   2,
			wantError: false,
			mock: func(ctrl *gomock.Controller) Cache {
				c := NewMockCache(ctrl)
				c.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(int64(2), nil)
				return c
			},
		},
		{
			name:      "test_delete_1",
			keys:      []string{"key1"},
			wantCnt:   1,
			wantError: false,
			mock: func(ctrl *gomock.Controller) Cache {
				c := NewMockCache(ctrl)
				c.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(int64(1), nil)
				return c
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NamespaceCache{
				C:         tt.mock(gomock.NewController(t)),
				Namespace: "app1:",
			}
			got, err := c.Delete(context.Background(), tt.keys...)
			if (err != nil) != tt.wantError {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			if got != tt.wantCnt {
				t.Errorf("Delete() got = %v, want %v", got, tt.wantCnt)
			}
		})
	}
}

func TestNamespaceCache_GetSet(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx context.Context
		key string
		val string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Value
	}{
		{
			name: "test_getset",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx: context.Background(),
				key: "key",
				val: "val",
			},
			want: Value{
				AnyValue: ekit.AnyValue{
					Val: "val",
					Err: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().GetSet(tt.args.ctx, tt.fields.Namespace+tt.args.key, tt.args.val).Return(tt.want)
			if got := c.GetSet(tt.args.ctx, tt.args.key, tt.args.val); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceCache_IncrBy(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx   context.Context
		key   string
		value int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "test_incrby",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx:   context.Background(),
				key:   "key",
				value: 1,
			},
			want:    1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().IncrBy(tt.args.ctx, tt.fields.Namespace+tt.args.key, tt.args.value).Return(tt.want, nil)
			got, err := c.IncrBy(tt.args.ctx, tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("IncrBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("IncrBy() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceCache_IncrByFloat(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx   context.Context
		key   string
		value float64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    float64
		wantErr bool
	}{
		{
			name: "test_incrbyfloat",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx:   context.Background(),
				key:   "key",
				value: 1.0,
			},
			want:    1.0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().IncrByFloat(tt.args.ctx, tt.fields.Namespace+tt.args.key, tt.args.value).Return(tt.want, nil)
			got, err := c.IncrByFloat(tt.args.ctx, tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("IncrByFloat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("IncrByFloat() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceCache_LPop(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx context.Context
		key string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Value
	}{
		{
			name: "test_lpop",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx: context.Background(),
				key: "key",
			},
			want: Value{
				AnyValue: ekit.AnyValue{
					Val: "val",
					Err: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().LPop(tt.args.ctx, tt.fields.Namespace+tt.args.key).Return(tt.want)
			if got := c.LPop(tt.args.ctx, tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LPop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceCache_LPush(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx context.Context
		key string
		val []any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "test_lpush",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx: context.Background(),
				key: "key",
				val: []any{"val1", "val2"},
			},
			want:    1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().LPush(tt.args.ctx, tt.fields.Namespace+tt.args.key, tt.args.val...).Return(tt.want, nil)
			got, err := c.LPush(tt.args.ctx, tt.args.key, tt.args.val...)
			if (err != nil) != tt.wantErr {
				t.Errorf("LPush() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LPush() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceCache_SAdd(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx     context.Context
		key     string
		members []any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "test_sadd",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx:     context.Background(),
				key:     "key",
				members: []any{"member1", "member2"},
			},
			want:    1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().SAdd(tt.args.ctx, tt.fields.Namespace+tt.args.key, tt.args.members...).Return(tt.want, nil)
			got, err := c.SAdd(tt.args.ctx, tt.args.key, tt.args.members...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SAdd() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SAdd() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceCache_SRem(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx     context.Context
		key     string
		members []any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "test_srem",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx:     context.Background(),
				key:     "key",
				members: []any{"member1", "member2"},
			},
			want:    1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().SRem(tt.args.ctx, tt.fields.Namespace+tt.args.key, tt.args.members...).Return(tt.want, nil)
			got, err := c.SRem(tt.args.ctx, tt.args.key, tt.args.members...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SRem() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SRem() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceCache_Set(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx        context.Context
		key        string
		val        any
		expiration time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test_set",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx:        context.Background(),
				key:        "key",
				val:        "val",
				expiration: time.Second,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().Set(tt.args.ctx, tt.fields.Namespace+tt.args.key, tt.args.val, tt.args.expiration).Return(nil)
			if err := c.Set(tt.args.ctx, tt.args.key, tt.args.val, tt.args.expiration); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNamespaceCache_SetNX(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx        context.Context
		key        string
		val        any
		expiration time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "test_setnx",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx:        context.Background(),
				key:        "key",
				val:        "val",
				expiration: time.Second,
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			tt.fields.C.EXPECT().SetNX(tt.args.ctx, tt.fields.Namespace+tt.args.key, tt.args.val, tt.args.expiration).Return(tt.want, nil)
			got, err := c.SetNX(tt.args.ctx, tt.args.key, tt.args.val, tt.args.expiration)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetNX() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SetNX() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValue_KeyNotFound(t *testing.T) {
	type fields struct {
		AnyValue ekit.AnyValue
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "test_key_not_found",
			fields: fields{
				AnyValue: ekit.AnyValue{
					Val: nil,
					Err: errs.ErrKeyNotExist,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Value{
				AnyValue: tt.fields.AnyValue,
			}
			if got := v.KeyNotFound(); got != tt.want {
				t.Errorf("KeyNotFound() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespaceCache_Get(t *testing.T) {
	type fields struct {
		C         *MockCache
		Namespace string
	}
	type args struct {
		ctx context.Context
		key string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Value
	}{
		{
			name: "test_get",
			fields: fields{
				C:         NewMockCache(gomock.NewController(t)),
				Namespace: "app1:",
			},
			args: args{
				ctx: context.Background(),
				key: "key",
			},
			want: Value{
				AnyValue: ekit.AnyValue{
					Val: nil,
					Err: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.C.EXPECT().Get(tt.args.ctx, tt.fields.Namespace+tt.args.key).Return(tt.want)
			c := &NamespaceCache{
				C:         tt.fields.C,
				Namespace: tt.fields.Namespace,
			}
			if got := c.Get(tt.args.ctx, tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}
