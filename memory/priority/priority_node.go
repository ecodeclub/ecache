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

import "github.com/ecodeclub/ekit"

// priorityNode 优先级结点
type priorityNode struct {
	priority  int64            //优先级
	cacheNode *rbTreeCacheNode //缓存结点的映射
}

func newPriorityNode(priority int64) *priorityNode {
	return &priorityNode{
		priority: priority,
	}
}

// comparatorPriorityNode 优先级结点的比较方式
func comparatorPriorityNode() ekit.Comparator[*priorityNode] {
	return func(src *priorityNode, dst *priorityNode) int {
		if src.priority < dst.priority {
			return -1
		} else if src.priority == dst.priority {
			return 0 //理论上在外面的map那里就已经判等了，不应该出现走这里的情况。
		} else {
			return 1
		}
	}
}
