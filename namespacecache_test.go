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
	"testing"
	"time"

	"github.com/ecodeclub/ekit"
	"go.uber.org/mock/gomock"
)

// 测试namespace 是否真的正确处理了 key
func TestKeyChanged(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockcache := NewMockCache(ctl)
	namespace := "test:"
	key := "key"
	mockNamespaceCache := NewMockNamespaceCache(mockcache, namespace)

	mockcache.EXPECT().Set(context.Background(), namespace+key, "value", time.Duration(0)).Return(nil)
	mockcache.EXPECT().Get(context.Background(), namespace+key).Return(Value{ekit.AnyValue{
		Val: "value",
		Err: nil,
	}})

	err := mockNamespaceCache.Set(context.Background(), key, "value", time.Duration(0))
	if err != nil {
		t.Errorf("mockNamespaceCache.Set() error = %v, wantErr %v", err, nil)
		return
	}
	v := mockNamespaceCache.Get(context.Background(), key)
	if v.Err != nil {
		t.Errorf("mockNamespaceCache.Get() error = %v, wantErr %v", v.Err, nil)
		return
	}
	if v.Val != "value" {
		t.Errorf("mockNamespaceCache.Get() = %v, want %v", v.Val, "value")
	}
}
