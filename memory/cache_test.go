package memory

import (
	"context"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSet(t *testing.T) {
	testCases := []struct {
		name       string
		cache      func() *Cache
		key        string
		val        any
		expiration time.Duration
		wantErr    error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:        "key1",
			val:        "val1",
			expiration: time.Minute,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			err := cache.Set(context.Background(), tc.key, tc.val, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestSetNX(t *testing.T) {
	testCases := []struct {
		name       string
		cache      func() *Cache
		key        string
		val        any
		expiration time.Duration
		wantErr    error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:        "key1",
			val:        "val1",
			expiration: time.Minute,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			_, err := cache.SetNX(context.Background(), tc.key, tc.val, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestGet(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		wantErr error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:     "key1",
			wantErr: errs.ErrKeyNotExist,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			ret := cache.Get(context.Background(), tc.key)
			assert.Equal(t, tc.wantErr, ret.Err)
		})
	}
}

func TestGetSet(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     string
		wantErr error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:     "key1",
			val:     "val1",
			wantErr: errs.ErrKeyNotExist,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			ret := cache.GetSet(context.Background(), tc.key, tc.val)
			assert.Equal(t, tc.wantErr, ret.Err)
		})
	}
}

func TestLPush(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     []any
		wantRet int64
		wantErr error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:     "key1",
			val:     []any{"val1"},
			wantRet: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			ret, err := cache.LPush(context.Background(), tc.key, tc.val...)
			assert.Equal(t, tc.wantRet, ret)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestLPop(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		wantRet int64
		wantErr error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:     "key1",
			wantErr: errs.ErrKeyNotExist,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			ret := cache.LPop(context.Background(), tc.key)
			assert.Equal(t, tc.wantErr, ret.Err)
		})
	}
}

func TestSAdd(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     []any
		wantRet int64
		wantErr error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:     "key1",
			val:     []any{"val1"},
			wantRet: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			ret, err := cache.SAdd(context.Background(), tc.key, tc.val...)
			assert.Equal(t, tc.wantRet, ret)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestSRem(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		wantRet int64
		wantErr error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:     "key1",
			wantErr: errs.ErrKeyNotExist,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			ret := cache.SRem(context.Background(), tc.key)
			assert.Equal(t, tc.wantErr, ret.Err)
		})
	}
}

func TestIncrBy(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     int64
		wantRet int64
		wantErr error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:     "key1",
			val:     1,
			wantRet: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			ret, err := cache.IncrBy(context.Background(), tc.key, tc.val)
			assert.Equal(t, tc.wantRet, ret)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestDecrBy(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     int64
		wantRet int64
		wantErr error
	}{
		{
			name: "覆盖测试，不报错就行了",
			cache: func() *Cache {
				client, _ := NewRBTreeClient()
				return NewCache(client)
			},
			key:     "key1",
			val:     1,
			wantRet: -1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			ret, err := cache.DecrBy(context.Background(), tc.key, tc.val)
			assert.Equal(t, tc.wantRet, ret)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
