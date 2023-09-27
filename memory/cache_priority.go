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
	"github.com/ecodeclub/ekit/queue"
)

// Priority 如果传进来的元素没有实现该接口，则默认优先级为0
type Priority interface {
	// GetPriority 获取元素的优先级
	GetPriority() int64
}

// cachePriority 缓存的优先级数据
type cachePriority struct {
	priorityQueue *queue.PriorityQueue[*cachePriorityNode] //优先级队列
}

func newCachePriority(initSize int) *cachePriority {
	priorityQueue := queue.NewPriorityQueue[*cachePriorityNode](initSize, comparatorCachePriorityNode())
	//这里的error传了compare就不可能出现，直接忽略
	return &cachePriority{
		priorityQueue: priorityQueue,
	}
}

// setCacheNodePriority 设置缓存结点的优先级数据
func (cp *cachePriority) setCacheNodePriority(cacheNode *rbTreeCacheNode, priority int64) {
	priorityNode := newCachePriorityNode(priority)
	//建立缓存结点和优先级结点的映射关系
	cacheNode.priorityNode = priorityNode
	priorityNode.cacheNode = cacheNode

	_ = cp.priorityQueue.Enqueue(priorityNode)
}

// deleteCacheNodePriority 移除缓存结点的优先级数据
func (cp *cachePriority) deleteCacheNodePriority(cacheNode *rbTreeCacheNode) {
	if cacheNode.priorityNode == nil {
		return //理论上缓存结点和优先级结点是对应上的，不应该出现走这里的情况。
	}
	priorityNode := cacheNode.priorityNode
	//删除缓存结点和优先级结点的映射关系
	cacheNode.priorityNode = nil
	priorityNode.cacheNode = nil
	//这里不删除优先级结点，等到触发淘汰的时候在处理
}
