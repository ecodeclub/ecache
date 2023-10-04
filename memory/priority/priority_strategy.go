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
	"errors"

	"github.com/ecodeclub/ekit/queue"
)

// 优先级类型
const (
	priorityTypeDefault = iota + 1 //优先级，默认
	priorityTypeLRU                //最近最少使用
	priorityTypeLFU                //最不经常使用
)

var (
	errWrongPriorityType = errors.New("ecache: 错误的优先级类型")
)

// Priority 如果传进来的元素没有实现该接口，则默认优先级为0
type Priority interface {
	// GetPriority 获取元素的优先级
	GetPriority() int64
}

// priorityStrategy 优先级策略
type priorityStrategy struct {
	priorityType  int                                 //优先级类型
	priorityQueue *queue.PriorityQueue[*priorityNode] //优先级队列
}

func newPriorityStrategy(priorityType int, initSize int) (*priorityStrategy, error) {
	if priorityType != priorityTypeDefault &&
		priorityType != priorityTypeLRU &&
		priorityType != priorityTypeLFU {
		return nil, errWrongPriorityType
	}

	priorityQueue := queue.NewPriorityQueue[*priorityNode](initSize, comparatorPriorityNode())
	//这里的error传了compare就不可能出现，直接忽略
	strategy := &priorityStrategy{
		priorityType:  priorityType,
		priorityQueue: priorityQueue,
	}

	return strategy, nil
}

// calculatePriority 获取缓存数据的优先级权重
func (cp *priorityStrategy) calculatePriority(node *rbTreeCacheNode) int64 {
	if cp.priorityType == priorityTypeLRU {
		if node.lastCallTime.IsZero() {
			return 0
		}
		return node.lastCallTime.Unix()
	}

	if cp.priorityType == priorityTypeLFU {
		return int64(node.totalCallTimes)
	}

	var priority int64
	//如果实现了Priority接口，那么就用接口的方法获取优先级权重
	val, ok := node.value.(Priority)
	if ok {
		priority = val.GetPriority()
	}
	return priority
}

// setCacheNodePriority 设置缓存结点的优先级数据
func (cp *priorityStrategy) setCacheNodePriority(cacheNode *rbTreeCacheNode) {
	priority := cp.calculatePriority(cacheNode)
	node := newPriorityNode(priority)
	//建立缓存结点和优先级结点的映射关系
	cacheNode.priorityNode = node
	node.cacheNode = cacheNode
	_ = cp.priorityQueue.Enqueue(node)
}

// deleteCacheNodePriority 移除缓存结点的优先级数据
func (cp *priorityStrategy) deleteCacheNodePriority(cacheNode *rbTreeCacheNode) {
	if cacheNode.priorityNode == nil {
		return //理论上缓存结点和优先级结点是对应上的，不应该出现走这里的情况。
	}
	node := cacheNode.priorityNode
	//删除缓存结点和优先级结点的映射关系
	cacheNode.priorityNode = nil
	node.cacheNode = nil
	//这里不删除优先级结点，等到触发淘汰的时候在处理
}

// 优先级是否会被缓存的get操作影响
func (cp *priorityStrategy) priorityAffectByGet() bool {
	if cp.priorityType == priorityTypeLRU || cp.priorityType == priorityTypeLFU {
		return true
	}
	return false
}
