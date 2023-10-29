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

package priority

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"github.com/stretchr/testify/assert"
)

// 测试用的，可以输入权重的结构
type testStructForPriority struct {
	priority int
}

func (ts testStructForPriority) Priority() int {
	return ts.priority
}

func compareTwoRBTreeClient(src *RBTreePriorityCache, dst *RBTreePriorityCache) bool {
	//如果缓存结构中的红黑树的大小一样，红黑树的每个key都有
	//键值对结点和数字结点中的元素一样，list和set结点中的元素数量一样
	//优先级队列长度一样，优先级队列顶部元素一样
	//那么就姑且认为两个缓存结构中的数据是一样的
	if src.cacheNum != dst.cacheNum {
		return false
	}
	if src.cacheData.Size() != dst.cacheData.Size() {
		return false
	}

	src.globalLock.RLock()
	srcKeys, srcNodes := src.cacheData.KeyValues()
	src.globalLock.RUnlock()
	srcKeysMap := make(map[string]*rbTreeCacheNode)
	for index, item := range srcKeys {
		srcKeysMap[item] = srcNodes[index]
	}
	dst.globalLock.RLock()
	dstKeys, dstNodes := dst.cacheData.KeyValues()
	dst.globalLock.RUnlock()
	dstKeysMap := make(map[string]*rbTreeCacheNode)
	for index, item := range dstKeys {
		dstKeysMap[item] = dstNodes[index]
	}

	for srcKey, srcNode := range srcKeysMap {
		dstNode, ok := dstKeysMap[srcKey]
		if !ok {
			return false
		}

		srcNodeVal1, ok1 := srcNode.value.(*list.LinkedList[any])
		if ok1 {
			dstNodeVal11, ok11 := dstNode.value.(*list.LinkedList[any])
			if !ok11 {
				return false
			}
			if srcNodeVal1.Len() != dstNodeVal11.Len() {
				return false
			}
			continue
		}

		srcNodeVal2, ok2 := srcNode.value.(*set.MapSet[any])
		if ok2 {
			dstNodeVal22, ok22 := dstNode.value.(*set.MapSet[any])
			if !ok22 {
				return false
			}
			if len(srcNodeVal2.Keys()) != len(dstNodeVal22.Keys()) {
				return false
			}
			continue
		}

		if srcNode.value != dstNode.value {
			return false
		}
	}

	if src.priorityData.Len() != dst.priorityData.Len() {
		return false
	}
	src.globalLock.Lock()
	srcTop, _ := src.priorityData.Peek()
	src.globalLock.Unlock()
	dst.globalLock.Lock()
	dstTop, _ := dst.priorityData.Peek()
	dst.globalLock.Unlock()
	if srcTop == nil && dstTop == nil {
		return true
	}
	if (srcTop == nil && dstTop != nil) || (srcTop != nil && dstTop == nil) {
		return false
	}
	if srcTop.key != dstTop.key {
		return false
	}

	return true
}

func TestRBTreePriorityCache_Set(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		value      any
		expiration time.Duration
		wantCache  func() *RBTreePriorityCache
		wantErr    error
	}{
		{
			name: "cache 0,add 1,ok",
			startCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				return client
			},
			key:   "key1",
			value: "value1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
		},
		{
			name: "cache 1,add 1,ok",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key2",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				cache.addNode(newKVRBTreeCacheNode("key2", "value2", 0))
				return cache
			},
		},
		{
			name: "cache 1,add 1,cover",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key1",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value2", 0))
				return cache
			},
		},
		{
			name: "limit 1,cache 1,add 1,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key2",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key2", "value2", 0))
				return cache
			},
		},
		{
			name: "limit 2,cache 2,add 1,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(2))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", testStructForPriority{priority: 1}, 0))
				cache.addNode(newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, 0))
				return cache
			},
			key:   "key3",
			value: testStructForPriority{priority: 3},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(2))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, 0))
				cache.addNode(newKVRBTreeCacheNode("key3", testStructForPriority{priority: 3}, 0))
				return cache
			},
		},
		{
			name: "limit 2,cache 2,add 1,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(2))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, 0))
				cache.addNode(newKVRBTreeCacheNode("key1", testStructForPriority{priority: 1}, 0))
				return cache
			},
			key:   "key3",
			value: testStructForPriority{priority: 3},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(2))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, 0))
				cache.addNode(newKVRBTreeCacheNode("key3", testStructForPriority{priority: 3}, 0))
				return cache
			},
		},
		{
			name: "limit 2,cache 2,add 1,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(2), WithDefaultPriority(5))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				cache.addNode(newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, 0))
				return cache
			},
			key:   "key3",
			value: testStructForPriority{priority: 3},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(2), WithDefaultPriority(5))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				cache.addNode(newKVRBTreeCacheNode("key3", testStructForPriority{priority: 3}, 0))
				return cache
			},
		},
		{
			name: "limit 1,cache 1,add 1,evict,cover empty priority queue top",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newKVRBTreeCacheNode("key1", testStructForPriority{priority: 1}, 0)
				cache.addNode(node1)
				cache.deleteNode(node1) //模拟删除结点，构造空的优先级队列头
				cache.addNode(newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, 0))
				return cache
			},
			key:   "key3",
			value: testStructForPriority{priority: 3},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key3", testStructForPriority{priority: 3}, 0))
				return cache
			},
		},
		{
			name: "limit 1,cache 1,add 1,evict,cover heap top nil,should not happen",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newKVRBTreeCacheNode("key1", "value1", 0)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				//这里不应该出现没有设置的情况，出现这种这种情况肯定有bug
				//cache.SetCacheNodePriority(node1)

				return cache
			},
			key:   "key2",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				//上面的bug导致这个结点没被删掉
				node1 := newKVRBTreeCacheNode("key1", "value1", 0)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.addNode(newKVRBTreeCacheNode("key2", "value2", 0))
				return cache
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			err := startCache.Set(context.Background(), tc.key, tc.value, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache()))
		})
	}
}

func TestCache_Set2(t *testing.T) {
	cacheLimit := 100
	cache, _ := NewRBTreePriorityCache(WithCacheLimit(cacheLimit))
	key := "key"
	value := "value"

	wg := sync.WaitGroup{}
	for i := 1; i <= 10000; i++ {
		wg.Add(1)
		j := i
		go func() {
			tempKey := fmt.Sprintf("%s%d", key, j)
			tempValue := fmt.Sprintf("%s%d", value, j)
			_ = cache.Set(context.Background(), tempKey, tempValue, 0)
			wg.Done()
		}()
	}
	wg.Wait()

	assert.Equal(t, cacheLimit, cache.cacheNum)
}

func TestRBTreePriorityCache_SetNX(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		value      any
		expiration time.Duration
		wantCache  func() *RBTreePriorityCache
		wantBool   bool
		wantErr    error
	}{
		{
			name: "cache 0,add 1,ok",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:   "key1",
			value: "value1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantBool: true,
		},
		{
			name: "cache 0,add 1,not conflict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key2",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				cache.addNode(newKVRBTreeCacheNode("key2", "value2", 0))
				return cache
			},
			wantBool: true,
		},
		{
			name: "cache 1,add 1,conflict,self",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key1",
			value: "value1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantBool: false,
		},
		{
			name: "cache 1,add 1,conflict,failed",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key1",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantBool: false,
		},
		{
			name: "cache 1,add 1,conflict,expired",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", -time.Minute))
				return cache
			},
			key:   "key1",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value2", 0))
				return cache
			},
			wantBool: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			retBool, err := startCache.SetNX(context.Background(), tc.key, tc.value, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantBool, retBool)
		})
	}
}

func TestRBTreePriorityCache_Get(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		wantCache  func() *RBTreePriorityCache
		wantValue  any
		wantErr    error
	}{
		{
			name: "cache 0,miss",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache 1,miss",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key: "key2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache 1,hit",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "cache num 1,hit,expire",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", -time.Minute))
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				return client
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache 1,hit,not expire",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantValue: "value1",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			value := startCache.Get(context.Background(), tc.key)
			assert.Equal(t, tc.wantErr, value.Err)
			if value.Err != nil {
				return
			}
			assert.Equal(t, tc.wantValue, value.Val)
		})
	}
}

func TestRBTreePriorityCache_doubleCheckInGet(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		node       *rbTreeCacheNode
		wantCache  func() *RBTreePriorityCache
	}{
		{
			name: "key not deleted by other thread",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", -time.Minute))
				return cache
			},
			node: newKVRBTreeCacheNode("key1", "value1", -time.Minute),
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newKVRBTreeCacheNode("key1", "value1", -time.Minute)
				cache.addNode(node1)
				cache.deleteNode(node1)
				return cache
			},
		},
		{
			name: "key deleted by other thread",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			node: newKVRBTreeCacheNode("key1", "value1", -time.Minute),
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			startCache.doubleCheckWhenExpire(tc.node, time.Now())
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache()))
		})
	}
}

func TestRBTreePriorityCache_GetSet(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		value      string
		wantCache  func() *RBTreePriorityCache
		wantValue  any
		wantErr    error
	}{
		{
			name: "cache 0,miss,add 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:   "key1",
			value: "value1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache 1,miss,add 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				cache.cacheNum++
				return cache
			},
			key:   "key2",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				cache.addNode(newKVRBTreeCacheNode("key2", "value2", 0))
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache 1,hit",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key1",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value2", 0))
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "cache 1,hit,expired",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", -time.Minute))
				return cache
			},
			key:   "key1",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value2", 0))
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "limit 1,cache 1,miss,add 1,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key2",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key2", "value2", 0))
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			value := startCache.GetSet(context.Background(), tc.key, tc.value)
			assert.Equal(t, tc.wantErr, value.Err)
			if value.Err != nil {
				return
			}
			assert.Equal(t, tc.wantValue, value.Val)
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache()))
		})
	}
}

func TestRBTreePriorityCache_LPush(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		value      []any
		wantCache  func() *RBTreePriorityCache
		wantNum    int64
		wantErr    error
	}{
		{
			name: "cache 0,push 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:   "key1",
			value: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := newListRBTreeCacheNode("key1")
				node1.value = valList
				cache.addNode(node1)
				return cache
			},
			wantNum: 1,
		},
		{
			name: "cache 1,item 1,push 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := newListRBTreeCacheNode("key1")
				node1.value = valList
				cache.addNode(node1)
				return cache
			},
			key:   "key1",
			value: []any{"value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				_ = valList.Append("value2")
				node1 := newListRBTreeCacheNode("key1")
				node1.value = valList
				cache.addNode(node1)
				return cache
			},
			wantNum: 1,
		},
		{
			name: "cache 0,push 2",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:   "key1",
			value: []any{"value1", "value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				_ = valList.Append("value2")
				node1 := newListRBTreeCacheNode("key1")
				node1.value = valList
				cache.addNode(node1)
				return cache
			},
			wantNum: 2,
		},
		{
			name: "limit 1,cache 1,push 1,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := newListRBTreeCacheNode("key1")
				node1.value = valList
				cache.addNode(node1)
				return cache
			},
			key:   "key2",
			value: []any{"value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value2")
				node1 := newListRBTreeCacheNode("key2")
				node1.value = valList
				cache.addNode(node1)
				return cache
			},
			wantNum: 1,
		},
		{
			name: "wrong type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantErr: errOnlyListCanLPUSH,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			num, err := startCache.LPush(context.Background(), tc.key, tc.value...)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantNum, num)
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache()))
		})
	}
}

func TestRBTreePriorityCache_LPop(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		wantCache  func() *RBTreePriorityCache
		wantValue  any
		wantErr    error
	}{
		{
			name: "cache 0,miss",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache 1,item 1,pop 1,delete node",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := newListRBTreeCacheNode("key1")
				node1.value = valList
				cache.addNode(node1)
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newListRBTreeCacheNode("key1")
				cache.addNode(node1)
				cache.deleteNode(node1)
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "cache 1,item 2,pop 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				_ = valList.Append("value2")
				node1 := newListRBTreeCacheNode("key1")
				node1.value = valList
				cache.addNode(node1)
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := newListRBTreeCacheNode("key1")
				node1.value = valList
				cache.addNode(node1)
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "wrong type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantErr: errOnlyListCanLPOP,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			value := startCache.LPop(context.Background(), tc.key)
			assert.Equal(t, tc.wantErr, value.Err)
			if value.Err != nil {
				return
			}
			assert.Equal(t, tc.wantValue, value.Val)
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache()))
		})
	}
}

func TestRBTreePriorityCache_SAdd(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		values     []any
		wantCache  func() *RBTreePriorityCache
		wantRet    int64
		wantErr    error
	}{
		{
			name: "cache 0,add 1",
			startCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				return client
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			wantRet: 1,
		},
		{
			name: "cache 1,add 1,not repeat",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			key:    "key1",
			values: []any{"value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				valSet1.Add("value2")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			wantRet: 1,
		},
		{
			name: "cache 1,add 1,repeat",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			wantRet: 0,
		},
		{
			name: "cache 0,add 2",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:    "key1",
			values: []any{"value1", "value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				valSet1.Add("value2")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			wantRet: 2,
		},
		{
			name: "limit 1,cache 1,add 1,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			key:    "key2",
			values: []any{"value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value2")
				node1 := newSetRBTreeCacheNode("key2", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			wantRet: 1,
		},
		{
			name: "wrong type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantRet: 0,
			wantErr: errOnlySetCanSAdd,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			ret, err := startCache.SAdd(context.Background(), tc.key, tc.values...)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRet, ret)
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache()))
		})
	}
}

func TestRBTreePriorityCache_SRem(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		values     []any
		wantCache  func() *RBTreePriorityCache
		wantRet    int64
		wantErr    error
	}{
		{
			name: "cache 0,rem 1,miss",
			startCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				return client
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				return client
			},
			wantRet: 0,
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache 1,item 1,rem 1,hit,delete node",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				cache.deleteNode(node1)
				return cache
			},
			wantRet: 1,
		},
		{
			name: "cache 1,item 1,rem 1,miss",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			key:    "key1",
			values: []any{"value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			wantRet: 0,
		},
		{
			name: "cache 1,item 2,rem 2,hit 2",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				valSet1.Add("value2")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				return cache
			},
			key:    "key1",
			values: []any{"value1", "value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				valSet1 := set.NewMapSet[any](8)
				valSet1.Add("value1")
				valSet1.Add("value2")
				node1 := newSetRBTreeCacheNode("key1", 8)
				node1.value = valSet1
				cache.addNode(node1)
				cache.deleteNode(node1)
				return cache
			},
			wantRet: 2,
		},
		{
			name: "wrong type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantRet: 0,
			wantErr: errOnlySetCanSRem,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			value, err := startCache.SRem(context.Background(), tc.key, tc.values...)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantRet, value)
			assert.True(t, compareTwoRBTreeClient(startCache, tc.wantCache()))
		})
	}
}

func TestRBTreePriorityCache_IncrBy(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		value      int64
		wantCache  func() *RBTreePriorityCache
		wantRet    int64
		wantErr    error
	}{
		{
			name: "cache 0,miss,add 1",
			startCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				return client
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key1")
				node1.value = int64(1)
				cache.addNode(node1)
				return cache
			},
			wantRet: 1,
		},
		{
			name: "cache 1,hit,value add 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key1")
				node1.value = int64(1)
				cache.addNode(node1)
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key1")
				node1.value = int64(2)
				cache.addNode(node1)
				return cache
			},
			wantRet: 2,
		},
		{
			name: "limit 1,cache 1,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key1")
				node1.value = int64(1)
				cache.addNode(node1)
				return cache
			},
			key:   "key2",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key2")
				node1.value = int64(1)
				cache.addNode(node1)
				return cache
			},
			wantRet: 1,
		},
		{
			name: "wrong string type ",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantErr: errOnlyNumCanIncrBy,
		},
		{
			name: "wrong float type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node := newFloatRBTreeCacheNode("key1")
				node.value = float64(3.14)
				cache.addNode(node)
				return cache
			},
			key:     "key1",
			value:   1,
			wantErr: errOnlyNumCanIncrBy,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			value, err := startCache.IncrBy(context.Background(), tc.key, tc.value)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRet, value)
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache()))
		})
	}
}

func TestRBTreePriorityCache_DecrBy(t *testing.T) {
	testCases := []struct {
		name       string
		startCache func() *RBTreePriorityCache
		key        string
		value      int64
		wantCache  func() *RBTreePriorityCache
		wantRet    int64
		wantErr    error
	}{
		{
			name: "cache 0,miss,add 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key1")
				node1.value = int64(-1)
				cache.addNode(node1)
				return cache
			},
			wantRet: -1,
		},
		{
			name: "cache 1,hit,value decr 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key1")
				node1.value = int64(1)
				cache.addNode(node1)
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key1")
				node1.value = int64(0)
				cache.addNode(node1)
				return cache
			},
			wantRet: 0,
		},
		{
			name: "limit 1,cache 1,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key1")
				node1.value = int64(1)
				cache.addNode(node1)
				return cache
			},
			key:   "key2",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node1 := newIntRBTreeCacheNode("key2")
				node1.value = int64(-1)
				cache.addNode(node1)
				return cache
			},
			wantRet: -1,
		},
		{
			name: "wrong type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			},
			wantErr: errOnlyNumCanDecrBy,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			value, err := startCache.DecrBy(context.Background(), tc.key, tc.value)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRet, value)
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache()))
		})
	}
}

func TestRBTreePriorityCache_autoClean(t *testing.T) {
	cache, _ := NewRBTreePriorityCache()
	key := "key"
	value := "value"

	wg := sync.WaitGroup{}
	for i := 1; i <= 6; i++ {
		wg.Add(1)
		j := i
		go func() {
			tempKey := fmt.Sprintf("%s%d", key, j)
			tempValue := fmt.Sprintf("%s%d", value, j)
			_ = cache.Set(context.Background(), tempKey, tempValue, time.Duration(j)*time.Second)
			wg.Done()
		}()
	}
	wg.Wait()

	value1 := cache.Get(context.Background(), "key1")
	value1Str, _ := value1.String()
	assert.Equal(t, "value1", value1Str)

	value6 := cache.Get(context.Background(), "key6")
	value6Str, _ := value6.String()
	assert.Equal(t, "value6", value6Str)

	time.Sleep(3 * time.Second)

	value1 = cache.Get(context.Background(), "key1")
	assert.Equal(t, errs.ErrKeyNotExist, value1.Err)

	value6 = cache.Get(context.Background(), "key6")
	value6Str, _ = value6.String()
	assert.Equal(t, "value6", value6Str)
}

func TestRBTreePriorityCache_UpdatePriority(t *testing.T) {

	testsCases := []struct {
		name         string
		startCache   func() *RBTreePriorityCache
		key          string
		priority     int
		wantKeyOrder []string
		wantNode     *rbTreeCacheNode
		wantRes      bool
		wantErr      error
	}{
		{
			name: "Increase the priority of node to the end of queue，success",
			startCache: func() *RBTreePriorityCache {
				cache, err := NewRBTreePriorityCache(WithCacheLimit(1))
				require.NoError(t, err)
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", testStructForPriority{priority: 1}, 0))
				cache.addNode(newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, 0))
				cache.addNode(newKVRBTreeCacheNode("key3", testStructForPriority{priority: 3}, 0))
				cache.addNode(newKVRBTreeCacheNode("key4", testStructForPriority{priority: 4}, 0))
				return cache
			},
			key:      "key1",
			priority: 100,
			wantNode: func() *rbTreeCacheNode {
				var deadline time.Time
				return &rbTreeCacheNode{
					key:       "key1",
					value:     testStructForPriority{priority: 1},
					deadline:  deadline,
					priority:  100,
					isDeleted: false,
				}
			}(),
			wantKeyOrder: []string{"key2", "key3", "key4", "key1"},
			wantRes:      true,
		},
		{
			name: "Decrease the priority of node to the top of queue，success",
			startCache: func() *RBTreePriorityCache {
				cache, err := NewRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)
				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", testStructForPriority{priority: 1}, 0))
				cache.addNode(newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, 0))
				cache.addNode(newKVRBTreeCacheNode("key3", testStructForPriority{priority: 3}, 0))
				cache.addNode(newKVRBTreeCacheNode("key4", testStructForPriority{priority: 4}, 0))
				return cache
			},
			key:      "key4",
			priority: 0,
			wantNode: func() *rbTreeCacheNode {
				var deadline time.Time
				return &rbTreeCacheNode{
					key:       "key4",
					value:     testStructForPriority{priority: 4},
					deadline:  deadline,
					priority:  0,
					isDeleted: false,
				}
			}(),
			wantKeyOrder: []string{"key4", "key1", "key2", "key3"},
			wantRes:      true,
		},
		{
			name: "Update the not exists node，fail",
			startCache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)
				return cache
			},
			key:      "key1",
			priority: 0,
			wantErr:  errs.ErrKeyNotExist,
		},
	}
	for _, tc := range testsCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.startCache()
			res, err := cache.updateNodePriority(context.Background(), tc.key, tc.priority)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}

			//校验出队顺序是否如预期
			rangeList := make([]string, 0, 8)
			for {
				topNode, er := cache.priorityData.Dequeue()
				if er != nil {
					break
				}
				if topNode.isDeleted {
					continue //空结点，直接回去，继续下一轮
				}
				tmp := topNode.key
				rangeList = append(rangeList, tmp)
			}
			node, err := cache.cacheData.Find(tc.key)
			require.NoError(t, err)
			assert.Equal(t, tc.wantNode, node)

			assert.Equal(t, tc.wantKeyOrder, rangeList)
			assert.Equal(t, tc.wantRes, res)

		})
	}
}

func TestRBTreePriorityCache_UpdatePriority_Concurrent(t *testing.T) {
	cache, err := NewRBTreePriorityCache()
	require.NoError(t, err)
	key := "key"

	cache.globalLock.Lock()
	cache.addNode(newKVRBTreeCacheNode(key, testStructForPriority{priority: -1}, 0))
	cache.globalLock.Unlock()

	//failCount := int32(0)

	label := struct {
		lock          *sync.RWMutex
		finalPriority int
		failcount     int
	}{
		lock:          &sync.RWMutex{},
		finalPriority: -1,
		failcount:     0,
	}

	wg := sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		j := i
		go func() {
			// 最后设置优先级的最终生效
			label.lock.Lock()
			defer label.lock.Unlock()
			ok, _ := cache.updateNodePriority(context.Background(), key, j)
			if ok {
				label.finalPriority = j
			}
			wg.Done()
		}()
	}
	wg.Wait()
	//assert.NotZero(t, failCount)
	node, cacheErr := cache.cacheData.Find(key)

	require.NoError(t, cacheErr)
	assert.Equal(t, node.priority, label.finalPriority)

	// 测试并发过程中被删除key
	wg = sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		j := i

		go func() {
			label.lock.Lock()
			defer label.lock.Unlock()
			ok, _ := cache.updateNodePriority(context.Background(), key, j)
			if !ok {
				label.failcount++
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			cache.globalLock.Lock()
			defer cache.globalLock.Unlock()
			checkNode, checkCacheErr := cache.cacheData.Find(key)
			if checkCacheErr != nil {
				wg.Done()
				return //被抢先删除了
			}
			cache.deleteNode(checkNode)
			wg.Done()
		}()
	}
	wg.Wait()
	assert.NotEqual(t, 0, label.failcount)
}

func TestRBTreePriorityCache_Delete(t *testing.T) {

	testCases := []struct {
		name    string
		cache   *RBTreePriorityCache
		keys    []string
		wantRes int64
		wantErr error
	}{
		{
			name: "cache 1 , delete 1",
			cache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)

				cache.globalLock.Lock()
				cache.addNode(newKVRBTreeCacheNode("key1", testStructForPriority{priority: -1}, 0))
				cache.globalLock.Unlock()
				return cache
			}(),
			keys:    []string{"key1"},
			wantRes: 1,
		},
		{
			name: "cache 1 , delete 0",
			cache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)

				cache.globalLock.Lock()
				cache.addNode(newKVRBTreeCacheNode("key1", testStructForPriority{priority: -1}, 0))
				cache.globalLock.Unlock()
				return cache
			}(),
			keys:    []string{"key2"},
			wantRes: 0,
		},
		{
			name: "cache 1, expired 1, delete 0",
			cache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)

				cache.globalLock.Lock()
				cache.addNode(newKVRBTreeCacheNode("key1", testStructForPriority{priority: -1}, time.Second))
				cache.globalLock.Unlock()

				time.Sleep(3 * time.Second)
				return cache
			}(),
			keys:    []string{"key1"},
			wantRes: 0,
		},
		{
			name: "cache 4, delete 3",
			cache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)

				cache.globalLock.Lock()
				cache.addNode(newKVRBTreeCacheNode("key1", testStructForPriority{priority: -1}, time.Second))
				cache.addNode(newKVRBTreeCacheNode("key2", testStructForPriority{priority: -1}, time.Second))
				cache.addNode(newKVRBTreeCacheNode("key3", testStructForPriority{priority: -1}, time.Second))
				cache.addNode(newKVRBTreeCacheNode("key4", testStructForPriority{priority: -1}, time.Second))
				cache.globalLock.Unlock()
				return cache
			}(),
			keys:    []string{"key1", "key2", "key3"},
			wantRes: 3,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache
			res, err := cache.Delete(context.Background(), tc.keys...)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)

			//确认已经被删除
			for _, key := range tc.keys {
				_, err = tc.cache.cacheData.Find(key)
				assert.NotNil(t, err)
			}
		})
	}
}

func TestRBTreePriorityCache_IncrByFloat(t *testing.T) {

	testCases := []struct {
		name      string
		cache     *RBTreePriorityCache
		key       string
		value     float64
		wantCache *RBTreePriorityCache
		wantRes   float64
		wantErr   error
	}{
		{
			name: "hit cache key1, increase key1",
			cache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)

				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node := newFloatRBTreeCacheNode("key1")
				node.value = float64(3.14)
				cache.addNode(node)
				return cache
			}(),
			key:     "key1",
			value:   0.1,
			wantRes: 3.24,
			wantCache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)

				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node := newFloatRBTreeCacheNode("key1")
				node.value = 3.24
				cache.addNode(node)
				return cache
			}(),
		},
		{
			name: "miss cache key1, create key1",
			cache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)
				return cache
			}(),
			key:     "key1",
			value:   0.1,
			wantRes: 0.1,
			wantCache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)

				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node := newFloatRBTreeCacheNode("key1")
				node.value = 0.1
				cache.addNode(node)
				return cache
			}(),
		},
		{
			name: "hit cache key1, wrong type",
			cache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(8))
				require.NoError(t, err)

				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				cache.addNode(newKVRBTreeCacheNode("key1", "value1", 0))
				return cache
			}(),
			key:     "key1",
			value:   0.1,
			wantErr: errOnlyNumCanIncrBy,
		},
		{
			name: "cache is full, increase key1, evited old key",
			cache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(1))
				require.NoError(t, err)

				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node := newFloatRBTreeCacheNode("key0")
				node.value = 3.14
				cache.addNode(node)
				return cache
			}(),
			key:     "key1",
			value:   0.1,
			wantRes: 0.1,
			wantCache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(1))
				require.NoError(t, err)

				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node := newFloatRBTreeCacheNode("key1")
				node.value = 0.1
				cache.addNode(node)
				return cache
			}(),
		},
		{
			name: "hit cache key1, convert int64 type to float64 type and increase",
			cache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(1))
				require.NoError(t, err)

				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node := newIntRBTreeCacheNode("key1")
				node.value = int64(3)
				cache.addNode(node)
				return cache
			}(),
			key:     "key1",
			value:   0.1,
			wantRes: 3.1,
			wantCache: func() *RBTreePriorityCache {
				cache, err := newRBTreePriorityCache(WithCacheLimit(1))
				require.NoError(t, err)

				cache.globalLock.Lock()
				defer cache.globalLock.Unlock()
				node := newFloatRBTreeCacheNode("key1")
				node.value = 3.1
				cache.addNode(node)
				return cache
			}(),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.cache
			value, err := startCache.IncrByFloat(context.Background(), tc.key, tc.value)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, value)
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache))
		})
	}
}
