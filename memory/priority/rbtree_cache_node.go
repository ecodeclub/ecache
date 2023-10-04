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

	"github.com/ecodeclub/ekit"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
)

// 缓存结点类型
const (
	rbTreeCacheNodeTypeKV   = iota + 1 //普通键值对，只有普通键值对会参与淘汰
	rbTreeCacheNodeTypeKVNX            //NX键值对，这里和普通键值对区分开，因为 NX 键值对不参与淘汰
	rbTreeCacheNodeTypeList            //list，用list.LinkedList[any]实现
	rbTreeCacheNodeTypeSet             //set，用set.MapSet[any]实现
	rbTreeCacheNodeTypeNum             //int64，给IncrBy和DecrBy用
)

// 缓存结点
type rbTreeCacheNode struct {
	key            string        //键
	unitType       int           //缓存结点类型，有四种类型
	value          any           //值
	deadline       time.Time     //有效期，默认0，永不过期
	priorityNode   *priorityNode //优先级结点的映射
	lastCallTime   time.Time     //缓存最后一次被调用的时间
	totalCallTimes int           //缓存被调用的次数
}

func newKVRBTreeCacheNode(key string, val any, expiration time.Duration) *rbTreeCacheNode {
	node := &rbTreeCacheNode{
		key:      key,
		unitType: rbTreeCacheNodeTypeKV,
		value:    val,
	}
	node.setExpiration(expiration)
	return node
}

func newKVNXRBTreeCacheNode(key string, val any, expiration time.Duration) *rbTreeCacheNode {
	node := &rbTreeCacheNode{
		key:      key,
		unitType: rbTreeCacheNodeTypeKVNX,
		value:    val,
	}
	node.setExpiration(expiration)
	return node
}

func newListRBTreeCacheNode(key string) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:      key,
		unitType: rbTreeCacheNodeTypeList,
		value:    list.NewLinkedList[any](),
	}
}

func newSetRBTreeCacheNode(key string, initSize int) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:      key,
		unitType: rbTreeCacheNodeTypeSet,
		value:    set.NewMapSet[any](initSize),
	}
}

func newIntRBTreeCacheNode(key string) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:      key,
		unitType: rbTreeCacheNodeTypeNum,
		value:    int64(0),
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

// beforeDeadline 检查传入的时间是不是在有效期之前
func (node *rbTreeCacheNode) beforeDeadline(checkTime time.Time) bool {
	if node.deadline.IsZero() {
		return true
	}
	return checkTime.Before(node.deadline)
}

// comparatorRBTreeCacheNode 红黑树结点的比较方式
func comparatorRBTreeCacheNode() ekit.Comparator[string] {
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
