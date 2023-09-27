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

package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func compareTwoCachePriority(src *cachePriority, dst *cachePriority) bool {
	//如果两个优先级队列中的结点数量一样，第一个结点的数据一样，那么就姑且认为两个优先级队列是一样的
	if src.priorityQueue.Len() != dst.priorityQueue.Len() {
		return false
	}
	srcTop, _ := src.priorityQueue.Peek()
	dstTop, _ := dst.priorityQueue.Peek()
	if srcTop.priority != dstTop.priority {
		return false
	}
	if srcTop.cacheNode == nil && dstTop.cacheNode != nil {
		return false
	}
	if srcTop.cacheNode != nil && dstTop.cacheNode == nil {
		return false
	}
	if srcTop.cacheNode != nil && dstTop.cacheNode != nil {
		if srcTop.cacheNode.key != dstTop.cacheNode.key {
			return false
		}
	}
	return true
}

func TestCachePriority_SetCacheNodePriority(t *testing.T) {
	testCases := []struct {
		name               string
		startCachePriority func() *cachePriority
		priority           int64
		rbTreeCacheNode    *rbTreeCacheNode
		wantCachePriority  func() *cachePriority
	}{
		{
			//优先级结点0个，设置时增加1个优先级结点
			name: "priority is 0,add 1 priority",
			startCachePriority: func() *cachePriority {
				return newCachePriority(priorityQueueInitSize)
			},
			priority:        1,
			rbTreeCacheNode: newKVRBTreeCacheNode("key1", "value1", 0),
			wantCachePriority: func() *cachePriority {
				wantCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = cacheNode1
				cacheNode1.priorityNode = priorityNode1
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode1)
				return wantCachePriority
			},
		},
		{
			//优先级结点1个，设置时增加1个优先级一样的结点
			name: "priority num 1,add 1 same priority",
			startCachePriority: func() *cachePriority {
				startCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = cacheNode1
				cacheNode1.priorityNode = priorityNode1
				_ = startCachePriority.priorityQueue.Enqueue(priorityNode1)
				return startCachePriority
			},
			priority:        1,
			rbTreeCacheNode: &rbTreeCacheNode{key: "key2"},
			wantCachePriority: func() *cachePriority {
				wantCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = cacheNode1
				cacheNode1.priorityNode = priorityNode1
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode1)
				cacheNode2 := newKVRBTreeCacheNode("key2", "value2", 0)
				priorityNode2 := newCachePriorityNode(1)
				priorityNode2.cacheNode = cacheNode2
				cacheNode2.priorityNode = priorityNode2
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode2)
				return wantCachePriority
			},
		},
		{
			//优先级结点1个，设置时增加1个优先级更大的结点
			name: "priority num 1,add 1 big priority",
			startCachePriority: func() *cachePriority {
				startCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = cacheNode1
				cacheNode1.priorityNode = priorityNode1
				_ = startCachePriority.priorityQueue.Enqueue(priorityNode1)
				return startCachePriority
			},
			priority:        2,
			rbTreeCacheNode: &rbTreeCacheNode{key: "key2"},
			wantCachePriority: func() *cachePriority {
				wantCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = cacheNode1
				cacheNode1.priorityNode = priorityNode1
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode1)
				cacheNode2 := newKVRBTreeCacheNode("key2", "value2", 0)
				priorityNode2 := newCachePriorityNode(2)
				priorityNode2.cacheNode = cacheNode2
				cacheNode2.priorityNode = priorityNode2
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode2)
				return wantCachePriority
			},
		},
		{
			//优先级结点1个，设置时增加1个优先级更小的结点
			name: "priority num 1,add 1 small priority",
			startCachePriority: func() *cachePriority {
				startCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = cacheNode1
				cacheNode1.priorityNode = priorityNode1
				_ = startCachePriority.priorityQueue.Enqueue(priorityNode1)
				return startCachePriority
			},
			priority:        -1,
			rbTreeCacheNode: &rbTreeCacheNode{key: "key2"},
			wantCachePriority: func() *cachePriority {
				wantCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = cacheNode1
				cacheNode1.priorityNode = priorityNode1
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode1)
				cacheNode2 := newKVRBTreeCacheNode("key2", "value2", 0)
				priorityNode2 := newCachePriorityNode(-1)
				priorityNode2.cacheNode = cacheNode2
				cacheNode2.priorityNode = priorityNode2
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode2)
				return wantCachePriority
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCachePriority := tc.startCachePriority()
			startCachePriority.setCacheNodePriority(tc.rbTreeCacheNode, tc.priority)
			wantCachePriority := tc.wantCachePriority()
			assert.Equal(t, compareTwoCachePriority(startCachePriority, wantCachePriority), true)
		})
	}
}

// 这里需要先执行cachePriority.setCacheNodePriority
// 这个步骤维护了双向的指针，测试用例不怎么好写
// 然后再测试cachePriority.DeleteCacheNodePriority
func TestCachePriority_DeleteCacheNodePriority(t *testing.T) {
	testCases := []struct {
		name               string
		startCachePriority func() *cachePriority
		rbTreeCacheNode    *rbTreeCacheNode
		priority           int64
		wantCachePriority  func() *cachePriority
	}{
		{
			//缓存元素1个，优先级结点1个，删除1个缓存元素
			name: "cache num 1,priority num 1,delete 1 cache",
			startCachePriority: func() *cachePriority {
				startCachePriority := newCachePriority(priorityQueueInitSize)
				return startCachePriority
			},
			rbTreeCacheNode: newKVRBTreeCacheNode("key1", "value1", 0),
			priority:        1,
			wantCachePriority: func() *cachePriority {
				wantCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = nil
				cacheNode1.priorityNode = nil
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode1)
				return wantCachePriority
			},
		},
		{
			//缓存元素2个，优先级结点2个，删除1个缓存元素
			name: "cache num 2,priority num 2,delete 1 cache",
			startCachePriority: func() *cachePriority {
				startCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = cacheNode1
				cacheNode1.priorityNode = priorityNode1
				_ = startCachePriority.priorityQueue.Enqueue(priorityNode1)
				return startCachePriority
			},
			rbTreeCacheNode: newKVRBTreeCacheNode("key2", "value2", 0),
			priority:        2,
			wantCachePriority: func() *cachePriority {
				wantCachePriority := newCachePriority(priorityQueueInitSize)
				cacheNode1 := newKVRBTreeCacheNode("key1", "value1", 0)
				priorityNode1 := newCachePriorityNode(1)
				priorityNode1.cacheNode = cacheNode1
				cacheNode1.priorityNode = priorityNode1
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode1)
				cacheNode2 := newKVRBTreeCacheNode("key2", "value2", 0)
				priorityNode2 := newCachePriorityNode(2)
				priorityNode2.cacheNode = nil
				cacheNode2.priorityNode = nil
				_ = wantCachePriority.priorityQueue.Enqueue(priorityNode2)
				return wantCachePriority
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCachePriority := tc.startCachePriority()
			startCachePriority.setCacheNodePriority(tc.rbTreeCacheNode, tc.priority)
			startCachePriority.deleteCacheNodePriority(tc.rbTreeCacheNode)
			wantCachePriority := tc.wantCachePriority()
			assert.Equal(t, compareTwoCachePriority(startCachePriority, wantCachePriority), true)
		})
	}
}
