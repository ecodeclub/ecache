package memory

import "github.com/ecodeclub/ekit"

// priorityNode 优先级结点
type priorityNode struct {
	priorityWeight int                         //优先级权重
	cacheData      map[string]*rbTreeCacheNode //缓存结点的映射
}

func newPriorityNode(priorityWeight int) *priorityNode {
	return &priorityNode{
		priorityWeight: priorityWeight,
		cacheData:      make(map[string]*rbTreeCacheNode),
	}
}

// comparatorPriorityNode 优先级结点的比较方式
func comparatorPriorityNode() ekit.Comparator[*priorityNode] {
	return func(src *priorityNode, dst *priorityNode) int {
		if src.priorityWeight < dst.priorityWeight {
			return -1
		} else if src.priorityWeight == dst.priorityWeight {
			return 0
		} else {
			return 1
		}
	}
}
