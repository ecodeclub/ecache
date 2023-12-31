package ecache

import (
	"context"
	"github.com/ecodeclub/ekit"
	"go.uber.org/mock/gomock"
	"testing"
	"time"
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

	mockNamespaceCache.Set(context.Background(), key, "value", time.Duration(0))
	v := mockNamespaceCache.Get(context.Background(), key)
	if v.Err != nil {
		t.Errorf("mockNamespaceCache.Get() error = %v, wantErr %v", v.Err, nil)
		return
	}
	if v.Val != "value" {
		t.Errorf("mockNamespaceCache.Get() = %v, want %v", v.Val, "value")
	}
}
