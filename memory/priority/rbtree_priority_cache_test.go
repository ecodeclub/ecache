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

	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"github.com/stretchr/testify/assert"
)

// 测试用的，可以输入权重的结构
type testStructForPriority struct {
	priority int64
}

func (ts testStructForPriority) GetPriority() int64 {
	return ts.priority
}

func compareTwoRBTreeClient(src *RBTreePriorityCache, dst *RBTreePriorityCache) bool {
	//如果缓存结构中的红黑树的大小一样，红黑树的每个key都有，key对应的结点类型一样
	//键值对结点和数字结点中的元素一样，list和set结点中的元素数量一样
	//那么就姑且认为两个缓存结构中的数据是一样的，缓存结构中的优先级数据单独测试
	if src.cacheNum != dst.cacheNum {
		return false
	}
	if src.cacheData.Size() != dst.cacheData.Size() {
		return false
	}
	srcKeys, srcNodes := src.cacheData.KeyValues()
	srcKeysMap := make(map[string]*rbTreeCacheNode)
	for index, item := range srcKeys {
		srcKeysMap[item] = srcNodes[index]
	}
	dstKeys, dstNodes := dst.cacheData.KeyValues()
	dstKeysMap := make(map[string]*rbTreeCacheNode)
	for index, item := range dstKeys {
		dstKeysMap[item] = dstNodes[index]
	}
	for srcKey, srcNode := range srcKeysMap {
		dstNode, ok := dstKeysMap[srcKey]
		if !ok {
			return false
		}
		if srcNode.unitType == rbTreeCacheNodeTypeKV {
			if srcNode.value != dstNode.value {
				return false
			}
		}
		if srcNode.unitType == rbTreeCacheNodeTypeKVNX {
			if srcNode.value != dstNode.value {
				return false
			}
		}
		if srcNode.unitType == rbTreeCacheNodeTypeList {
			srcNodeVal, ok2 := srcNode.value.(*list.LinkedList[any])
			if !ok2 {
				return false
			}
			dstNodeVal, ok3 := dstNode.value.(*list.LinkedList[any])
			if !ok3 {
				return false
			}
			if srcNodeVal.Len() != dstNodeVal.Len() {
				return false
			}
		}
		if srcNode.unitType == rbTreeCacheNodeTypeSet {
			srcNodeVal, ok2 := srcNode.value.(*set.MapSet[any])
			if !ok2 {
				return false
			}
			dstNodeVal, ok3 := dstNode.value.(*set.MapSet[any])
			if !ok3 {
				return false
			}
			if len(srcNodeVal.Keys()) != len(dstNodeVal.Keys()) {
				return false
			}
		}
		if srcNode.unitType == rbTreeCacheNodeTypeNum {
			if srcNode.value != dstNode.value {
				return false
			}
		}
	}

	return true
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
			name: "cache num 0,add 1 cache",
			startCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				return client
			},
			key:        "key1",
			value:      "value1",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				cache.cacheNum++
				return cache
			},
		},
		{
			name: "cache num 1,add 1 cache",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				cache.cacheNum++
				return cache
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				cache.cacheNum++
				_ = cache.cacheData.Add("key2", newKVRBTreeCacheNode("key2", "value2", time.Minute))
				cache.cacheNum++
				return cache
			},
		},
		{
			name: "cache num 2,add 1 cache,cover",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			key:        "key1",
			value:      "value2",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value2", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
		},
		{
			name: "limit is 2,cache num 1,add 1 cache,not evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(2))
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				cache.cacheNum++
				return cache
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				cache.cacheNum++
				_ = cache.cacheData.Add("key2", newKVRBTreeCacheNode("key2", "value2", time.Minute))
				cache.cacheNum++
				return cache
			},
		},
		{
			name: "limit is 1,cache num 1,add 1 cache,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key2", "value2", time.Minute)
				_ = cache.cacheData.Add("key2", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
		},
		{
			name: "limit is 2,cache num 2,add 1 cache,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(2))
				node1 := newKVRBTreeCacheNode("key1", testStructForPriority{priority: 1}, time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				node2 := newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, time.Minute)
				_ = cache.cacheData.Add("key2", node2)
				cache.cacheNum++
				cache.setCacheNodePriority(node2)
				return cache
			},
			key:        "key3",
			value:      "value3",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node2 := newKVRBTreeCacheNode("key2", testStructForPriority{priority: 2}, time.Minute)
				_ = cache.cacheData.Add("key2", node2)
				cache.cacheNum++
				cache.setCacheNodePriority(node2)
				node3 := newKVRBTreeCacheNode("key3", "value3", time.Minute)
				_ = cache.cacheData.Add("key3", node3)
				cache.cacheNum++
				cache.setCacheNodePriority(node3)
				return cache
			},
		},
		{
			name: "limit is 2,cache num 2,add 1 cache,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(2))
				node1 := newKVRBTreeCacheNode("key1", testStructForPriority{priority: 2}, time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				node2 := newKVRBTreeCacheNode("key2", testStructForPriority{priority: 1}, time.Minute)
				_ = cache.cacheData.Add("key2", node2)
				cache.cacheNum++
				cache.setCacheNodePriority(node2)
				return cache
			},
			key:        "key3",
			value:      "value3",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", testStructForPriority{priority: 2}, time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				node3 := newKVRBTreeCacheNode("key3", "value3", time.Minute)
				_ = cache.cacheData.Add("key3", node3)
				cache.cacheNum++
				cache.setCacheNodePriority(node3)
				return cache
			},
		},
		{
			name: "limit is 1,cache num 1,add 1 cache,evict,test empty top",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				node3 := newKVRBTreeCacheNode("key3", "value3", time.Minute)
				_ = cache.cacheData.Add("key3", node3)
				cache.cacheNum++
				cache.setCacheNodePriority(node3)
				//模拟删除结点，构造空的优先级队列头
				cache.cacheData.Delete("key1")
				cache.cacheNum--
				cache.deleteCacheNodePriority(node1)

				return cache
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key2", "value2", time.Minute)
				_ = cache.cacheData.Add("key2", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
		},
		{
			name: "node type error",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:     "key1",
			wantErr: errOnlyKVCanSet,
		},
		//{
		//	//1缓存结点，新增1覆盖，理论上不应该出现这种情况，凑一下测试覆盖率
		//	name: "1cache,add1,cover,should not happen,just for coverage",
		//	startCache: func() *RBTreePriorityCache {
		//		cache, _ := NewRBTreePriorityCache()
		//
		//		node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
		//		_ = cache.cacheData.Add("key1", node1)
		//		cache.cacheNum++
		//		//这里不应该出现没有设置的情况，出现这种这种情况肯定有bug
		//		//cache.SetCacheNodePriority(node1)
		//
		//		return cache
		//	},
		//	key:        "key1",
		//	value:      "value2",
		//	expiration: time.Minute,
		//	wantCache: func() *RBTreePriorityCache {
		//		cache, _ := NewRBTreePriorityCache()
		//
		//		node1 := newKVRBTreeCacheNode("key1", "value2", time.Minute)
		//		_ = cache.cacheData.Add("key1", node1)
		//		cache.cacheNum++
		//		cache.setCacheNodePriority(node1)
		//
		//		return cache
		//	},
		//},
		{
			//1缓存容量，1缓存结点，新增触发淘汰，堆顶为空的情况，理论上不应该出现这种情况，凑一下测试覆盖率
			name: "1limit,1cache,add1,evict,heap top nil,should not happen,just for coverage",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				//这里不应该出现没有设置的情况，出现这种这种情况肯定有bug
				//cache.SetCacheNodePriority(node1)

				return cache
			},
			key:        "key3",
			value:      "value3",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()

				//上面的bug导致这个结点没被删掉
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)

				node3 := newKVRBTreeCacheNode("key3", "value3", time.Minute)
				_ = cache.cacheData.Add("key3", node3)
				cache.cacheNum++
				cache.setCacheNodePriority(node3)

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
			_ = cache.Set(context.Background(), tempKey, tempValue, time.Minute)
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
			name: "cache num 0,add 1 cache",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:        "key1",
			value:      "value1",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			wantBool: true,
		},
		{
			name: "cache num 0,add 1 cache,not conflict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				_ = cache.cacheData.Add("key2", newKVNXRBTreeCacheNode("key2", "value2", time.Minute))
				return cache
			},
			wantBool: true,
		},
		{
			name: "cache num 1,add 1 cache,conflict,self",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			key:        "key1",
			value:      "value1",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			wantBool: true,
		},
		{
			name: "cache num 1,add 1 cache,conflict,expired",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", -time.Minute))
				return cache
			},
			key:        "key1",
			value:      "value2",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value2", time.Minute))
				return cache
			},
			wantBool: true,
		},
		{
			name: "cache num 1,add 1 cache,conflict,failed",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			key:        "key1",
			value:      "value2",
			expiration: time.Minute,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			wantBool: false,
		},
		{
			name: "wrong cache node type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:     "key1",
			wantErr: errOnlyKVNXCanSetNX,
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

			name: "cache num 0,miss",
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
			name: "cache num 1,miss",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			key: "key2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				cache.cacheNum++
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache num 1,hit",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "cache num 1,hit,not expire",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", 0)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "cache num 1,hit,expire",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", -time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
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
			name: "wrong cache node type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:     "key1",
			wantErr: errOnlyKVCanGet,
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
		key        string
		wantCache  func() *RBTreePriorityCache
	}{
		{
			name: "key not deleted by other thread",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", -time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
		},
		{
			name: "key deleted by other thread",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			startCache.doubleCheckWhenExpire(tc.key, time.Now())
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
			name: "cache num 0,miss,add 1 cache",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:   "key1",
			value: "value1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				cache.cacheNum++
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache num 1,miss,add 1 cache",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				cache.cacheNum++
				return cache
			},
			key:   "key2",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				cache.cacheNum++
				_ = cache.cacheData.Add("key2", newKVRBTreeCacheNode("key2", "value2", time.Minute))
				cache.cacheNum++
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "cache num 1,hit",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			key:   "key1",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value2", time.Minute))
				cache.cacheNum++
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "cache num 1,hit,expired",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value1", -time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			key:   "key1",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := newKVRBTreeCacheNode("key1", "value2", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "limit is 1,cache num 1,miss,add 1 cache,evict",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache(WithCacheLimit(1))
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = cache.cacheData.Add("key1", node1)
				cache.cacheNum++
				cache.setCacheNodePriority(node1)
				return cache
			},
			key:   "key2",
			value: "value2",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key2", newKVRBTreeCacheNode("key2", "value2", time.Minute))
				cache.cacheNum++
				return cache
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "wrong cache node type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:     "key1",
			wantErr: errOnlyKVCanGetSet,
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
			name: "cache num 0,push 1 item",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:   "key1",
			value: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantNum: 1,
		},
		{
			name: "cache num 1,push 1 item",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList1,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:   "key1",
			value: []any{"value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				_ = valList1.Append("value2")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList1,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantNum: 1,
		},
		{
			name: "cache num 0,push 2 item",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:   "key1",
			value: []any{"value1", "value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				_ = valList1.Append("value2")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList1,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantNum: 2,
		},
		{
			name: "wrong cache node type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
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
			name: "cache num 0",
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
			name: "cache num 1,item num 1,left 0 item",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "cache num 1,item num 2,left 1 item",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				_ = valList.Append("value2")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value2")
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeList,
					value:    valList,
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantValue: "value1",
		},
		{
			name: "wrong cache node type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			key: "key1",
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
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
			name: "cache num 0,add 1 item",
			startCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				return client
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantRet: 1,
		},
		{
			name: "cache num 1,add 1 item,not repeat",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:    "key1",
			values: []any{"value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				valSet1.Add("value2")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantRet: 1,
		},
		{
			name: "cache num 1,add 1 item,repeat",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantRet: 0,
		},
		{
			name: "cache num 0,add 2 item",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:    "key1",
			values: []any{"value1", "value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				valSet1.Add("value2")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantRet: 2,
		},
		{
			name: "wrong cache node type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
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
			name: "cache num 0,delete 1 item,err",
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
			name: "cache num 1,item num 1,delete 1 item,hit",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			wantRet: 1,
		},
		{
			name: "cache num 1,item num 1,delete 1 item,miss",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:    "key1",
			values: []any{"value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantRet: 0,
		},
		{
			name: "cache num 1,item num 2,delete 2 item,hit 2",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeSet,
					value:    valSet1,
				}
				valSet1.Add("value1")
				valSet1.Add("value2")
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:    "key1",
			values: []any{"value1", "value2"},
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			wantRet: 2,
		},
		{
			name: "wrong cache node type",
			startCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key:    "key1",
			values: []any{"value1"},
			wantCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantRet: 0,
			wantErr: errOnlySetCanSRem,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCache := tc.startCache()
			value := startCache.SRem(context.Background(), tc.key, tc.values...)
			assert.Equal(t, tc.wantErr, value.Err)
			if value.Err != nil {
				return
			}
			assert.Equal(t, tc.wantRet, value.Val)
			assert.Equal(t, true, compareTwoRBTreeClient(startCache, tc.wantCache()))
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
			name: "cache num 0,miss,add 1 cache",
			startCache: func() *RBTreePriorityCache {
				client, _ := NewRBTreePriorityCache()
				return client
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeNum,
					value:    int64(1),
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantRet: 1,
		},
		{
			name: "cache num 1,hit,value add 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeNum,
					value:    int64(1),
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeNum,
					value:    int64(2),
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantRet: 2,
		},
		{
			name: "wrong cache node type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
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
			name: "cache num 0,miss,add 1 cache",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeNum,
					value:    int64(-1),
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantRet: -1,
		},
		{
			name: "cache num 1,hit,value decr 1",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeNum,
					value:    int64(1),
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				node1 := &rbTreeCacheNode{
					unitType: rbTreeCacheNodeTypeNum,
					value:    int64(0),
				}
				_ = cache.cacheData.Add("key1", node1)
				return cache
			},
			wantRet: 0,
		},
		{
			name: "wrong cache node type",
			startCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return cache
			},
			key:   "key1",
			value: 1,
			wantCache: func() *RBTreePriorityCache {
				cache, _ := NewRBTreePriorityCache()
				_ = cache.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
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
