package memory

import (
	"errors"
	"github.com/ecodeclub/ekit/heap"
)

var (
	ErrPriorityUnitNotExist = errors.New("ecache: 没有找到相应权重的结点")
)

// Priority 如果传进来的元素没有实现了该接口，则使用默认权重
type Priority interface {
	// GetPriorityWeight 获取元素的优先级
	GetPriorityWeight() int
}

// CachePriority 缓存的优先级数据
type CachePriority struct {
	priorityData      *heap.MinHeap[*priorityNode] //优先级数据
	priorityWeightMap map[int]*priorityNode        //方便快速找某个权重值的结点
}

func newCachePriority(initSize int) *CachePriority {
	//这里的error只会是ErrMinHeapComparatorIsNull，传了compare就不可能出现的
	priorityData, _ := heap.NewMinHeap[*priorityNode](comparatorPriorityNode(), initSize)
	return &CachePriority{
		priorityData:      priorityData,
		priorityWeightMap: make(map[int]*priorityNode),
	}
}

// SetCacheNodePriority 设置缓存结点的优先级数据
func (cp *CachePriority) SetCacheNodePriority(priorityWeight int, node *rbTreeCacheNode) {
	priorityUnit, priorityErr := cp.findPriorityNodeByPriorityWeight(priorityWeight)
	//这里的error只会是ErrPriorityUnitNotExist
	if priorityErr == ErrPriorityUnitNotExist {
		// 如果优先级结点不存在就新建一个
		priorityUnit = newPriorityNode(priorityWeight)
		cp.priorityData.Add(priorityUnit)
		cp.priorityWeightMap[priorityWeight] = priorityUnit
	}
	//建立缓存节点和优先级结点的映射关系
	node.priorityUnit = priorityUnit
	priorityUnit.cacheData[node.key] = node
}

// DeleteCacheNodePriority 移除缓存结点的优先级数据
func (cp *CachePriority) DeleteCacheNodePriority(node *rbTreeCacheNode) {
	priorityUnit := node.priorityUnit
	node.priorityUnit = nil
	delete(priorityUnit.cacheData, node.key)
	//这里不删除空的优先级结点，可能前脚刚删掉，后脚就被新建出来了
	//触发缓存淘汰的时候再删，那个时候删的是顶部的，应该不会那么快就被新建出来
}

// 用优先级权重查找优先级结点
func (cp *CachePriority) findPriorityNodeByPriorityWeight(priorityWeight int) (*priorityNode, error) {
	if val, ok := cp.priorityWeightMap[priorityWeight]; ok {
		return val, nil
	} else {
		return nil, ErrPriorityUnitNotExist
	}
}
