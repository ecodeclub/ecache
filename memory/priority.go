package memory

import (
	"errors"
	"github.com/ecodeclub/ekit"
	"github.com/ecodeclub/ekit/heap"
)

var (
	ErrPriorityUnitNotExist = errors.New("没有找到相应权重的结点")
)

// Priority 如果传进来的元素没有实现了该接口，则使用默认权重
type Priority interface {
	// GetPriorityWeight 获取元素的优先级
	GetPriorityWeight() int
}

type CachePriority struct {
	priorityData      *heap.MinHeap[*PriorityUnit] //优先级数据
	priorityWeightMap map[int]*PriorityUnit        //方便快速找某个权重的结点
}

func newCachePriority() *CachePriority {
	priorityData, _ := heap.NewMinHeap[*PriorityUnit](ComparatorPriorityUnit())
	return &CachePriority{
		priorityData:      priorityData,
		priorityWeightMap: make(map[int]*PriorityUnit),
	}
}

func (cp *CachePriority) AddUnit(priorityWeight int, node *rbTreeNode) {
	existPriorityUnit, priorityErr := cp.FindUnitByPriorityWeight(priorityWeight)
	if priorityErr == nil {
		existPriorityUnit.cacheData[node.key] = node
		node.priorityUnit = existPriorityUnit
	} else {
		newPriorityUnit := NewPriorityUnit(priorityWeight)
		newPriorityUnit.cacheData[node.key] = node
		node.priorityUnit = newPriorityUnit
		cp.priorityData.Add(newPriorityUnit)
	}
}

func (cp *CachePriority) FindUnitByPriorityWeight(priorityWeight int) (*PriorityUnit, error) {
	if val, ok := cp.priorityWeightMap[priorityWeight]; ok {
		return val, nil
	} else {
		return nil, ErrPriorityUnitNotExist
	}
}

type PriorityUnit struct {
	priorityWeight int
	cacheData      map[string]*rbTreeNode
}

func NewPriorityUnit(priorityWeight int) *PriorityUnit {
	return &PriorityUnit{
		priorityWeight: priorityWeight,
		cacheData:      make(map[string]*rbTreeNode),
	}
}

func ComparatorPriorityUnit() ekit.Comparator[*PriorityUnit] {
	return func(src *PriorityUnit, dst *PriorityUnit) int {
		if src.priorityWeight < dst.priorityWeight {
			return -1
		} else if src.priorityWeight == dst.priorityWeight {
			return 0
		} else {
			return 1
		}
	}
}
