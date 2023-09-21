package memory

import (
	"context"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func compareTwoRBTreeClient(src *RBTreeClient, dst *RBTreeClient) bool {
	if src.cacheData.Size() != dst.cacheData.Size() {
		return false
	}
	srcKeys, srcNodes := src.cacheData.KeyValues()
	srcKeysMap := make(map[string]*rbTreeCacheNode)
	for index, item := range srcKeys {
		srcKeysMap[item] = srcNodes[index]
	}
	dstKeys, dstNodes := dst.cacheData.KeyValues()
	dstKeysMap := make(map[string]*rbTreeCacheNode)
	for index, item := range dstKeys {
		dstKeysMap[item] = dstNodes[index]
	}
	for srcKey, srcNode := range srcKeysMap {
		dstNode, ok := dstKeysMap[srcKey]
		if !ok {
			return false
		}
		if srcNode.unitType == unitTypeKV {
			if srcNode.val != dstNode.val {
				return false
			}
		}
		if srcNode.unitType == unitTypeList {
			srcNodeVal, ok2 := srcNode.val.(*list.LinkedList[any])
			if !ok2 {
				return false
			}
			dstNodeVal, ok3 := dstNode.val.(*list.LinkedList[any])
			if !ok3 {
				return false
			}
			if srcNodeVal.Len() != dstNodeVal.Len() {
				return false
			}
		}
		if srcNode.unitType == unitTypeSet {
			srcNodeVal, ok2 := srcNode.val.(*set.MapSet[any])
			if !ok2 {
				return false
			}
			dstNodeVal, ok3 := dstNode.val.(*set.MapSet[any])
			if !ok3 {
				return false
			}
			if len(srcNodeVal.Keys()) != len(dstNodeVal.Keys()) {
				return false
			}
		}
	}

	return true
}

func TestRBTreeClientSet(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		value       any
		expiration  time.Duration
		wantClient  func() *RBTreeClient
		wantErr     error
	}{
		{
			name: "缓存容量0，新增1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key:        "key1",
			value:      "value1",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
		},
		{
			name: "缓存容量1，新增1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				_ = client.cacheData.Add("key2", newKVRBTreeCacheNode("key2", "value2", time.Minute))
				return client
			},
		},
		{
			name: "缓存容量1，覆盖",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				key1Node := newKVRBTreeCacheNode("key1", "value2", time.Minute)
				_ = client.cacheData.Add("key1", key1Node)
				key1NodePriorityWeight := client.getValPriorityWeight(key1Node)
				client.priorityData.SetCacheNodePriority(key1NodePriorityWeight, key1Node)
				return client
			},
			key:        "key1",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				key1Node := newKVRBTreeCacheNode("key1", "value2", time.Minute)
				_ = client.cacheData.Add("key1", key1Node)
				key1NodePriorityWeight := client.getValPriorityWeight(key1Node)
				client.priorityData.SetCacheNodePriority(key1NodePriorityWeight, key1Node)
				return client
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			err := startClient.Set(context.Background(), tc.key, tc.value, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}

func TestRBTreeClientLPUSH(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		value       []any
		expiration  time.Duration
		wantClient  func() *RBTreeClient
		wantNum     int64
		wantErr     error
	}{
		{
			name: "lpush one value to empty cache",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key:        "key1",
			value:      []any{"value1"},
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", node)
				return client
			},
			wantNum: 1,
		},
		{
			name: "lpush two value to empty cache",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key:        "key1",
			value:      []any{"value1", "value2"},
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				_ = valList.Append("value2")
				node := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", node)
				return client
			},
			wantNum: 2,
		},
		{
			name: "lpush value to one value cache",
			startClient: func() *RBTreeClient {
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", node)
				return client
			},
			key:        "key1",
			value:      []any{"value2"},
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				_ = valList.Append("value2")
				node := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", node)
				return client
			},
			wantNum: 1,
		},
		{
			name: "lpush anther key value to cache",
			startClient: func() *RBTreeClient {
				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				node := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList1,
				}
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", node)
				return client
			},
			key:        "key2",
			value:      []any{"value2"},
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				node := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList1,
				}
				valList2 := list.NewLinkedList[any]()
				_ = valList2.Append("value1")
				node2 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList2,
				}
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", node)
				_ = client.cacheData.Add("key2", node2)
				return client
			},
			wantNum: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			num, err := startClient.LPush(context.Background(), tc.key, tc.value...)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantNum, num)
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}
