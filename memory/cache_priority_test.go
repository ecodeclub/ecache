package memory

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

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

func TestCachePriority_SetCacheNodePriority(t *testing.T) {
	testCases := []struct {
		name               string
		startCachePriority func() *CachePriority
		priorityWeight     int64
		rbTreeCacheNode    *rbTreeCacheNode
		wantCachePriority  func() *CachePriority
	}{
		{
			//0优先级结点，设置时增加1个新的优先级结点
			name: "priority0,add priority1",
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
			//1优先级结点，设置时不增加新的优先级结点
			name: "priority1,not add priority1",
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
			//1优先级结点，设置时增加1个新的优先级结点
			name: "priority1,add priority1",
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

func TestCachePriority_DeleteCacheNodePriority(t *testing.T) {
	testCases := []struct {
		name                string
		startCachePriority  func() *CachePriority
		priorityWeight      int64
		rbTreeCacheNode     *rbTreeCacheNode
		wantRBTreeCacheNode *rbTreeCacheNode
		wantCachePriority   func() *CachePriority
	}{
		{
			//1优先级结点，缓存1元素，删除1缓存元素
			name: "priority1，each has 1cache, delete 1cache",
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
			//1优先级结点，2缓存元素，删除1缓存元素
			name: "priority1，each has 2cache, delete 1cache",
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
			//2优先级结点，各1缓存元素，删除1个缓存元素
			name: "priority2，each has 1cache, delete 1cache",
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
			//这里需要先set再delete，互相维护了指针，不好测试
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
			//理论上不应该出现这种情况，凑一下测试覆盖率
			name:    "should not happen,just for coverage",
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
