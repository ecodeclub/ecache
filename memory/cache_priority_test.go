package memory

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var ()

func compareTwoCachePriority(src *CachePriority, dst *CachePriority) bool {
	//如果两个小根堆中结点数量一样，堆顶结点一样，堆顶的权重和缓存数据一样，
	//那么就姑且认为两个小根堆是一样的
	if src.priorityData.Size() != dst.priorityData.Size() {
		return false
	}
	srcTop, _ := src.priorityData.GetTop()
	dstTop, _ := dst.priorityData.GetTop()
	if srcTop.priorityWeight != dstTop.priorityWeight {
		return false
	}
	if len(srcTop.cacheData) != len(dstTop.cacheData) {
		return false
	}
	if len(src.priorityWeightMap) != len(dst.priorityWeightMap) {
		return false
	}
	return true
}

func TestCachePrioritySaveCacheNodePriority(t *testing.T) {
	testCases := []struct {
		name               string
		startCachePriority func() *CachePriority
		priorityWeight     int64
		rbTreeCacheNode    *rbTreeCacheNode
		wantCachePriority  func() *CachePriority
	}{
		{
			name: "0优先级结点，设置时增加1个新的优先级结点",
			startCachePriority: func() *CachePriority {
				return newCachePriority(minHeapInitSize)
			},
			priorityWeight:  1,
			rbTreeCacheNode: &rbTreeCacheNode{key: "key1"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(minHeapInitSize)

				node := newPriorityNode(1)
				node.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node

				return cachePriority
			},
		},
		{
			name: "1优先级结点，设置时不增加新的优先级结点",
			startCachePriority: func() *CachePriority {

				cachePriority := newCachePriority(minHeapInitSize)
				node := newPriorityNode(1)
				node.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node

				return cachePriority
			},
			priorityWeight:  1,
			rbTreeCacheNode: &rbTreeCacheNode{key: "key2"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(minHeapInitSize)

				node := newPriorityNode(1)
				node.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				node.cacheData["key2"] = &rbTreeCacheNode{key: "key2"}
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node

				return cachePriority
			},
		},
		{
			name: "1优先级结点，设置时增加1个新的优先级结点",
			startCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(minHeapInitSize)

				node1 := newPriorityNode(1)
				node1.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node1)
				cachePriority.priorityWeightMap[1] = node1

				return cachePriority
			},
			priorityWeight:  2,
			rbTreeCacheNode: &rbTreeCacheNode{key: "key2"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(minHeapInitSize)

				node1 := newPriorityNode(1)
				node1.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node1)
				cachePriority.priorityWeightMap[1] = node1

				node2 := newPriorityNode(2)
				node2.cacheData["key2"] = &rbTreeCacheNode{key: "key2"}
				cachePriority.priorityData.Add(node2)
				cachePriority.priorityWeightMap[2] = node2

				return cachePriority
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCachePriority := tc.startCachePriority()
			startCachePriority.SetCacheNodePriority(tc.priorityWeight, tc.rbTreeCacheNode)
			wantCachePriority := tc.wantCachePriority()
			assert.Equal(t, compareTwoCachePriority(startCachePriority, wantCachePriority), true)
		})
	}
}

func TestCachePriorityDeleteCacheNodePriority(t *testing.T) {
	testCases := []struct {
		name                string
		startCachePriority  func() *CachePriority
		priorityWeight      int64
		rbTreeCacheNode     *rbTreeCacheNode
		wantRBTreeCacheNode *rbTreeCacheNode
		wantCachePriority   func() *CachePriority
	}{
		{
			name: "优先级结点1缓存元素1，删除1个缓存元素",
			startCachePriority: func() *CachePriority {
				return newCachePriority(minHeapInitSize)
			},
			priorityWeight:      1,
			rbTreeCacheNode:     &rbTreeCacheNode{key: "key1"},
			wantRBTreeCacheNode: &rbTreeCacheNode{key: "key1"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(minHeapInitSize)
				node := newPriorityNode(1)
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node
				return cachePriority
			},
		},
		{
			name: "优先级结点1缓存元素2，删除1个缓存元素",
			startCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(minHeapInitSize)
				node := newPriorityNode(1)
				node.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node
				return cachePriority
			},
			priorityWeight:      1,
			rbTreeCacheNode:     &rbTreeCacheNode{key: "key2"},
			wantRBTreeCacheNode: &rbTreeCacheNode{key: "key2"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(minHeapInitSize)
				node := newPriorityNode(1)
				node.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node
				return cachePriority
			},
		},
		{
			name: "优先级结点2缓存元素各1，删除1个缓存元素",
			startCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(minHeapInitSize)
				node := newPriorityNode(1)
				node.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node
				return cachePriority
			},
			priorityWeight:      2,
			rbTreeCacheNode:     &rbTreeCacheNode{key: "key2"},
			wantRBTreeCacheNode: &rbTreeCacheNode{key: "key2"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(minHeapInitSize)
				node1 := newPriorityNode(1)
				node1.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node1)
				cachePriority.priorityWeightMap[1] = node1
				node2 := newPriorityNode(2)
				cachePriority.priorityData.Add(node2)
				cachePriority.priorityWeightMap[2] = node2
				return cachePriority
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startCachePriority := tc.startCachePriority()
			startCachePriority.SetCacheNodePriority(tc.priorityWeight, tc.rbTreeCacheNode)
			startCachePriority.DeleteCacheNodePriority(tc.rbTreeCacheNode)
			wantCachePriority := tc.wantCachePriority()
			assert.Equal(t, tc.rbTreeCacheNode.priorityUnit, tc.wantRBTreeCacheNode.priorityUnit)
			assert.Equal(t, compareTwoCachePriority(startCachePriority, wantCachePriority), true)
		})
	}
}

func TestComparatorPriorityNode(t *testing.T) {
	testCases := []struct {
		name    string
		src     *priorityNode
		dst     *priorityNode
		wantRet int
	}{
		{
			name:    "理论上不应该出现这种情况，凑一下测试覆盖率",
			src:     &priorityNode{priorityWeight: 1},
			dst:     &priorityNode{priorityWeight: 1},
			wantRet: 0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, comparatorPriorityNode()(tc.src, tc.dst), tc.wantRet)
		})
	}
}
