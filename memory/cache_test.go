package memory

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestCache_Set(t *testing.T) {
	testCases := []struct {
		name       string
		cache      func() *Cache
		key        string
		val        any
		expiration time.Duration
		wantErr    error
	}{
		{
			name: "no err is ok",
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

func TestCache_Set2(t *testing.T) {
	startClient, _ := NewRBTreeClient(SetCacheLimit(100))
	cache := NewCache(startClient)
	key := "key"
	val := "val"

	wg := sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		j := i
		go func() {
			tempKey := fmt.Sprintf("%s%d", key, j)
			tempVal := fmt.Sprintf("%s%d", val, j)
			_ = cache.Set(context.Background(), tempKey, tempVal, time.Minute)
			wg.Done()
		}()
	}
	wg.Wait()

	endClient, _ := cache.client.(*RBTreeClient)
	fmt.Println(endClient.cacheNum)
}

func TestCache_SetNX(t *testing.T) {
	testCases := []struct {
		name       string
		cache      func() *Cache
		key        string
		val        any
		expiration time.Duration
		wantErr    error
	}{
		{
			name: "no err is ok",
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

func TestCache_Get(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		wantErr error
	}{
		{
			name: "no err is ok",
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

func TestCache_GetSet(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     string
		wantErr error
	}{
		{
			name: "no err is ok",
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

func TestCache_LPush(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     []any
		wantRet int64
		wantErr error
	}{
		{
			name: "no err is ok",
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

func TestCache_LPop(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		wantRet int64
		wantErr error
	}{
		{
			name: "no err is ok",
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

func TestCache_SAdd(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     []any
		wantRet int64
		wantErr error
	}{
		{
			name: "no err is ok",
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

func TestCache_SRem(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		wantRet int64
		wantErr error
	}{
		{
			name: "no err is ok",
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

func TestCache_IncrBy(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     int64
		wantRet int64
		wantErr error
	}{
		{
			name: "no err is ok",
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

func TestCache_DecrBy(t *testing.T) {
	testCases := []struct {
		name    string
		cache   func() *Cache
		key     string
		val     int64
		wantRet int64
		wantErr error
	}{
		{
			name: "no err is ok",
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
