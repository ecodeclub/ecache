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
	"time"

	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"

	"github.com/ecodeclub/ekit"
)

// rbTreeCacheNode 缓存结点
type rbTreeCacheNode struct {
	key       string    //键
	value     any       //值
	deadline  time.Time //有效期，默认0，永不过期
	priority  int       //优先级
	isDeleted bool      //是否被删除
}

func newKVRBTreeCacheNode(key string, value any, expiration time.Duration) *rbTreeCacheNode {
	node := &rbTreeCacheNode{
		key:   key,
		value: value,
	}
	node.setExpiration(expiration)
	return node
}

func newListRBTreeCacheNode(key string) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:   key,
		value: list.NewLinkedList[any](),
	}
}

func newSetRBTreeCacheNode(key string, initSize int) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:   key,
		value: set.NewMapSet[any](initSize),
	}
}

func newIntRBTreeCacheNode(key string) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:   key,
		value: int64(0),
	}
}

func newFloatRBTreeCacheNode(key string) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:   key,
		value: float64(0),
	}
}

// setExpiration 设置有效期
func (node *rbTreeCacheNode) setExpiration(expiration time.Duration) {
	var deadline time.Time
	if expiration != 0 {
		deadline = time.Now().Add(expiration)
	}
	node.deadline = deadline
}

// setExpiration 设置有效期
func (node *rbTreeCacheNode) setPriority(priority int) {
	node.priority = priority
}

// replace 重新设置缓存结点的value和有效期
func (node *rbTreeCacheNode) replace(value any, expiration time.Duration) {
	node.value = value
	node.setExpiration(expiration)
}

// beforeDeadline 检查传入的时间是不是在有效期之前
func (node *rbTreeCacheNode) beforeDeadline(checkTime time.Time) bool {
	if node.deadline.IsZero() {
		return true
	}
	return checkTime.Before(node.deadline)
}

// truncate 清空缓存结点中的数据
func (node *rbTreeCacheNode) truncate() {
	var nilValue any
	node.value = nilValue
	node.isDeleted = true
}

// comparatorRBTreeCacheNodeByKey 缓存结点根据key的比较方式（给红黑树用）
func comparatorRBTreeCacheNodeByKey() ekit.Comparator[string] {
	return func(src string, dst string) int {
		if src < dst {
			return -1
		} else if src == dst {
			return 0
		} else {
			return 1
		}
	}
}

// comparatorRBTreeCacheNodeByPriority 缓存结点根据优先级的比较方式（给优先级队列用）
func comparatorRBTreeCacheNodeByPriority() ekit.Comparator[*rbTreeCacheNode] {
	return func(src *rbTreeCacheNode, dst *rbTreeCacheNode) int {
		if src.priority < dst.priority {
			return -1
		} else if src.priority == dst.priority {
			return 0
		} else {
			return 1
		}
	}
}
