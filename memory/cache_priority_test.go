package memory

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	initSize = 8
)

func compareTwoCachePriority(src *CachePriority, dst *CachePriority) bool {
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
		priorityWeight     int
		rbTreeCacheNode    *rbTreeCacheNode
		wantCachePriority  func() *CachePriority
	}{
		{
			name: "优先级结点0，设置时增加1个新的优先级结点",
			startCachePriority: func() *CachePriority {
				return newCachePriority(initSize)
			},
			priorityWeight:  1,
			rbTreeCacheNode: &rbTreeCacheNode{key: "key1"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(initSize)
				node := newPriorityNode(1)
				node.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node
				return cachePriority
			},
		},
		{
			name: "优先级结点1，设置时不增加新的优先级结点",
			startCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(initSize)
				node := newPriorityNode(1)
				node.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node
				return cachePriority
			},
			priorityWeight:  1,
			rbTreeCacheNode: &rbTreeCacheNode{key: "key2"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(initSize)
				node := newPriorityNode(1)
				node.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				node.cacheData["key2"] = &rbTreeCacheNode{key: "key2"}
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node
				return cachePriority
			},
		},
		{
			name: "优先级结点1，设置时增加1个新的优先级结点",
			startCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(initSize)
				node1 := newPriorityNode(1)
				node1.cacheData["key1"] = &rbTreeCacheNode{key: "key1"}
				cachePriority.priorityData.Add(node1)
				cachePriority.priorityWeightMap[1] = node1
				return cachePriority
			},
			priorityWeight:  2,
			rbTreeCacheNode: &rbTreeCacheNode{key: "key2"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(initSize)
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
		priorityWeight      int
		rbTreeCacheNode     *rbTreeCacheNode
		wantRBTreeCacheNode *rbTreeCacheNode
		wantCachePriority   func() *CachePriority
	}{
		{
			name: "优先级结点1缓存元素1，删除1个缓存元素",
			startCachePriority: func() *CachePriority {
				return newCachePriority(initSize)
			},
			priorityWeight:      1,
			rbTreeCacheNode:     &rbTreeCacheNode{key: "key1"},
			wantRBTreeCacheNode: &rbTreeCacheNode{key: "key1"},
			wantCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(initSize)
				node := newPriorityNode(1)
				cachePriority.priorityData.Add(node)
				cachePriority.priorityWeightMap[1] = node
				return cachePriority
			},
		},
		{
			name: "优先级结点1缓存元素2，删除1个缓存元素",
			startCachePriority: func() *CachePriority {
				cachePriority := newCachePriority(initSize)
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
				cachePriority := newCachePriority(initSize)
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
				cachePriority := newCachePriority(initSize)
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
				cachePriority := newCachePriority(initSize)
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
