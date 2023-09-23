package memory

import (
	"context"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

// 测试用的，可以输入权重的结构
type testStruct struct {
	priorityWeight int64
}

func (ts testStruct) GetPriorityWeight() int64 {
	return ts.priorityWeight
}

// 测试用的，模拟不同内存大小
type testStructSize1 struct {
	int1 int64
}

type testStructSize2 struct {
	int1 int64
	int2 int64
	int3 int64
	int4 int64
}

func compareTwoRBTreeClient(src *RBTreeClient, dst *RBTreeClient) bool {
	//如果缓存结构中的红黑树的大小一样，红黑树的每个key都有，key对应的结点类型一样
	//键值对结点和数字结点中的元素一样，list和set结点中的元素数量一样
	//那么就姑且认为两个缓存结构中的数据是一样的，缓存结构中的优先级数据单独测试
	if src.cacheNum != dst.cacheNum {
		return false
	}
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
		if srcNode.unitType == unitTypeKVNX {
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
		if srcNode.unitType == unitTypeNum {
			if srcNode.val != dstNode.val {
				return false
			}
		}
	}

	return true
}

func TestNewRBTreeClient(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() (*RBTreeClient, error)
		wantErr     error
	}{
		{
			name: "传错的优先级类型",
			startClient: func() (*RBTreeClient, error) {
				client, err := NewRBTreeClient(SetPriorityType(0))
				return client, err
			},
			wantErr: ErrWrongPriorityType,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.startClient()
			assert.Equal(t, tc.wantErr, err)
		})
	}
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
			name: "0缓存结点，新增",
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
				client.cacheNum++
				return client
			},
		},
		{
			name: "1缓存结点，新增",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				client.cacheNum++
				return client
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				client.cacheNum++
				_ = client.cacheData.Add("key2", newKVRBTreeCacheNode("key2", "value2", time.Minute))
				client.cacheNum++
				return client
			},
		},
		{
			name: "1缓存结点，新增覆盖",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:        "key1",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value2", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
		},
		{
			name: "1缓存结点，新增覆盖，理论上不应该出现这种情况，凑一下测试覆盖率",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				//这里不应该出现没有设置的情况，出现这种这种情况肯定有bug
				//client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:        "key1",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value2", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
		},
		{
			name: "2缓存容量，1缓存结点，新增不触发淘汰",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetCacheLimit(2))
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				client.cacheNum++
				return client
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				client.cacheNum++
				_ = client.cacheData.Add("key2", newKVRBTreeCacheNode("key2", "value2", time.Minute))
				client.cacheNum++
				return client
			},
		},
		{
			name: "1缓存容量，1缓存结点，权重模式正序，新增触发淘汰",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetCacheLimit(1))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key2", "value2", time.Minute)
				_ = client.cacheData.Add("key2", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
		},
		{
			name: "2缓存容量，2缓存结点，权重不一样，权重模式正序，新增触发淘汰",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetCacheLimit(2))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				node2 := newKVRBTreeCacheNode("key2", testStruct{priorityWeight: 2}, time.Minute)
				_ = client.cacheData.Add("key2", node2)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node2), node2)

				return client
			},
			key:        "key3",
			value:      "value3",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node2 := newKVRBTreeCacheNode("key2", testStruct{priorityWeight: 2}, time.Minute)
				_ = client.cacheData.Add("key2", node2)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node2), node2)

				node3 := newKVRBTreeCacheNode("key3", "value3", time.Minute)
				_ = client.cacheData.Add("key3", node3)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node3), node3)

				return client
			},
		},
		{
			name: "2缓存容量，2缓存结点，权重不一样，默认权重更大，权重模式正序，新增触发淘汰",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetCacheLimit(2), SetDefaultPriorityWeight(3))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				node2 := newKVRBTreeCacheNode("key2", testStruct{priorityWeight: 2}, time.Minute)
				_ = client.cacheData.Add("key2", node2)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node2), node2)

				return client
			},
			key:        "key3",
			value:      "value3",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				node3 := newKVRBTreeCacheNode("key3", "value3", time.Minute)
				_ = client.cacheData.Add("key3", node3)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node3), node3)

				return client
			},
		},
		{
			name: "2缓存容量，2缓存结点，权重不一样，权重模式倒序，新增触发淘汰",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetCacheLimit(2), SetOrderByASC(false))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				node2 := newKVRBTreeCacheNode("key2", testStruct{priorityWeight: 2}, time.Minute)
				_ = client.cacheData.Add("key2", node2)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node2), node2)

				return client
			},
			key:        "key3",
			value:      "value3",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				node3 := newKVRBTreeCacheNode("key3", "value3", time.Minute)
				_ = client.cacheData.Add("key3", node3)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node3), node3)

				return client
			},
		},
		{
			name: "1缓存容量，1缓存结点，lfu模式，新增触发淘汰，测试淘汰时，堆顶为空的情况",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetCacheLimit(1), SetPriorityType(PriorityTypeLFU))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				_ = client.Get(context.Background(), "key1")

				return client
			},
			key:        "key3",
			value:      "value3",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node3 := newKVRBTreeCacheNode("key3", "value3", time.Minute)
				_ = client.cacheData.Add("key3", node3)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node3), node3)

				return client
			},
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:     "key1",
			wantErr: ErrOnlyKVCanSet,
		},
		{
			name: "1缓存容量，1缓存结点，新增触发淘汰，堆顶为空的情况，理论上不应该出现这种情况，凑一下测试覆盖率",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetCacheLimit(1))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				//这里不应该出现没有设置的情况，出现这种这种情况肯定有bug
				//client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:        "key3",
			value:      "value3",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				//上面的bug导致这个结点没被删掉
				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				node3 := newKVRBTreeCacheNode("key3", "value3", time.Minute)
				_ = client.cacheData.Add("key3", node3)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node3), node3)

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

func TestRBTreeClientSetNX(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		value       any
		expiration  time.Duration
		wantClient  func() *RBTreeClient
		wantBool    bool
		wantErr     error
	}{
		{
			name: "0缓存结点，新增",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key:        "key1",
			value:      "value1",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantBool: true,
		},
		{
			name: "1缓存结点，新增不冲突",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				_ = client.cacheData.Add("key2", newKVNXRBTreeCacheNode("key2", "value2", time.Minute))
				return client
			},
			wantBool: true,
		},
		{
			name: "1缓存结点，覆盖自己的",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key:        "key1",
			value:      "value1",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantBool: true,
		},
		{
			name: "1缓存结点，新增冲突但是过期",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", -time.Minute))
				return client
			},
			key:        "key1",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value2", time.Minute))
				return client
			},
			wantBool: true,
		},
		{
			name: "1缓存结点，新增冲突返回失败",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key:        "key1",
			value:      "value2",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVNXRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantBool: false,
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:     "key1",
			wantErr: ErrOnlyKVNXCanSetNX,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			retBool, err := startClient.SetNX(context.Background(), tc.key, tc.value, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantBool, retBool)
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}

func TestRBTreeClientGet(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		wantClient  func() *RBTreeClient
		wantVal     any
		wantErr     error
	}{
		{
			name: "0缓存结点，查询未命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "1缓存结点，查询未命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				client.cacheNum++
				return client
			},
			key: "key2",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				client.cacheNum++
				return client
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "1缓存结点，查询命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				client.cacheNum++
				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				client.cacheNum++
				return client
			},
			wantVal: "value1",
		},
		{
			name: "1缓存结点，不会过期，查询命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", 0)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			wantVal: "value1",
		},
		{
			name: "1缓存结点，查询命中但是过期",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", -time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:     "key1",
			wantErr: ErrOnlyKVCanGet,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			val := startClient.Get(context.Background(), tc.key)
			assert.Equal(t, tc.wantErr, val.Err)
			if val.Err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val.Val)
		})
	}
}

func TestDoubleCheckInGet(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		wantClient  func() *RBTreeClient
	}{
		{
			name: "key没有被别的线程删除",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", -time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
		},
		{
			name: "key已经被别的线程删除了",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			now := time.Now()
			startClient.doubleCheckInGet(tc.key, now)
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}

func TestLRU(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		wantMap0    map[string]string
		wantMap1    map[string]string
	}{
		{
			name: "1缓存结点",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeLRU))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:      "key1",
			wantMap0: map[string]string{},
			wantMap1: map[string]string{"key1": "key1"},
		},
		{
			name: "2缓存结点，key1最近访问，key2最久未访问",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeLRU))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				priorityWeight := client.getValPriorityWeight(node1)
				client.priorityData.SetCacheNodePriority(priorityWeight, node1)

				node2 := newKVRBTreeCacheNode("key2", "value2", time.Minute)
				_ = client.cacheData.Add("key2", node2)
				priorityWeight2 := client.getValPriorityWeight(node2)
				client.priorityData.SetCacheNodePriority(priorityWeight2, node2)

				return client
			},
			key:      "key1",
			wantMap0: map[string]string{"key2": "key2"},
			wantMap1: map[string]string{"key1": "key1"},
		},
		{
			name: "2缓存结点，key1最近访问，key2最久未访问，但是没设置LRU",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				priorityWeight := client.getValPriorityWeight(node1)
				client.priorityData.SetCacheNodePriority(priorityWeight, node1)

				node2 := newKVRBTreeCacheNode("key2", "value2", time.Minute)
				_ = client.cacheData.Add("key2", node2)
				priorityWeight2 := client.getValPriorityWeight(node2)
				client.priorityData.SetCacheNodePriority(priorityWeight2, node2)

				return client
			},
			key:      "key1",
			wantMap0: map[string]string{"key1": "key1", "key2": "key2"},
			wantMap1: map[string]string{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			_ = startClient.Get(context.Background(), tc.key)

			//判断一下最前面两个结点是否符合预期

			top0, _ := startClient.priorityData.priorityData.ExtractTop()
			keyMap0 := make(map[string]string)
			for key := range top0.cacheData {
				keyMap0[key] = key
			}
			result0 := true
			for key := range tc.wantMap0 {
				if _, ok := keyMap0[key]; !ok {
					result0 = false
				}
			}
			assert.Equal(t, true, len(keyMap0) == len(tc.wantMap0))
			assert.Equal(t, true, result0)

			top1, err1 := startClient.priorityData.priorityData.ExtractTop()
			if err1 != nil {
				return //如果没有第二个结点，就不用判断了
			}
			keyMap1 := make(map[string]string)
			for key := range top1.cacheData {
				keyMap1[key] = key
			}
			result1 := true
			for key := range tc.wantMap1 {
				if _, ok := keyMap1[key]; !ok {
					result1 = false
				}
			}
			assert.Equal(t, true, len(keyMap1) == len(tc.wantMap1))
			assert.Equal(t, true, result1)
		})
	}
}

func TestLFU(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		wantMap0    map[string]string
		wantMap1    map[string]string
	}{
		{
			name: "1缓存结点，key1访问0，访问key1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeLFU))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				priorityWeight := client.getValPriorityWeight(node1)
				client.priorityData.SetCacheNodePriority(priorityWeight, node1)

				return client
			},
			key:      "key1",
			wantMap0: map[string]string{},
			wantMap1: map[string]string{"key1": "key1"},
		},
		{
			name: "2缓存结点，key1访问0，key2访问0，访问key1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeLFU))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				priorityWeight := client.getValPriorityWeight(node1)
				client.priorityData.SetCacheNodePriority(priorityWeight, node1)

				node2 := newKVRBTreeCacheNode("key2", "value2", time.Minute)
				_ = client.cacheData.Add("key2", node2)
				priorityWeight2 := client.getValPriorityWeight(node2)
				client.priorityData.SetCacheNodePriority(priorityWeight2, node2)

				return client
			},
			key:      "key1",
			wantMap0: map[string]string{"key2": "key2"},
			wantMap1: map[string]string{"key1": "key1"},
		},
		{
			name: "2缓存结点，key1访问0，key2访问0，访问key1，但是没设置LFU",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				priorityWeight := client.getValPriorityWeight(node1)
				client.priorityData.SetCacheNodePriority(priorityWeight, node1)

				node2 := newKVRBTreeCacheNode("key2", "value2", time.Minute)
				_ = client.cacheData.Add("key2", node2)
				priorityWeight2 := client.getValPriorityWeight(node2)
				client.priorityData.SetCacheNodePriority(priorityWeight2, node2)

				return client
			},
			key:      "key1",
			wantMap0: map[string]string{"key1": "key1", "key2": "key2"},
			wantMap1: map[string]string{},
		},
		{
			name: "2缓存结点，key1访问1，key2访问1，访问key1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeLFU))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				priorityWeight := client.getValPriorityWeight(node1)
				client.priorityData.SetCacheNodePriority(priorityWeight, node1)

				node2 := newKVRBTreeCacheNode("key2", "value2", time.Minute)
				_ = client.cacheData.Add("key2", node2)
				priorityWeight2 := client.getValPriorityWeight(node2)
				client.priorityData.SetCacheNodePriority(priorityWeight2, node2)

				_ = client.Get(context.Background(), "key1")
				_ = client.Get(context.Background(), "key2")

				return client
			},
			key:      "key1",
			wantMap0: map[string]string{},
			wantMap1: map[string]string{"key2": "key2"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			_ = startClient.Get(context.Background(), tc.key)

			//判断一下最前面两个结点是否符合预期

			top0, _ := startClient.priorityData.priorityData.ExtractTop()
			keyMap0 := make(map[string]string)
			for key := range top0.cacheData {
				keyMap0[key] = key
			}
			result0 := true
			for key := range tc.wantMap0 {
				if _, ok := keyMap0[key]; !ok {
					result0 = false
				}
			}
			assert.Equal(t, true, len(keyMap0) == len(tc.wantMap0))
			assert.Equal(t, true, result0)

			top1, err1 := startClient.priorityData.priorityData.ExtractTop()
			if err1 != nil {
				return //如果没有第二个结点，就不用判断了
			}
			keyMap1 := make(map[string]string)
			for key := range top1.cacheData {
				keyMap1[key] = key
			}
			result1 := true
			for key := range tc.wantMap1 {
				if _, ok := keyMap1[key]; !ok {
					result1 = false
				}
			}
			assert.Equal(t, true, len(keyMap1) == len(tc.wantMap1))
			assert.Equal(t, true, result1)
		})
	}
}

func TestMemory(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		val         any
		wantMap0    map[string]string
		wantMap1    map[string]string
	}{
		{
			name: "0缓存结点，新增",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeMemory))
				return client
			},
			key:      "key1",
			val:      testStructSize1{},
			wantMap0: map[string]string{"key1": "key1"},
			wantMap1: map[string]string{},
		},
		{
			name: "1缓存结点，内存模式正序，新增一个一样的",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeMemory))

				node1 := newKVRBTreeCacheNode("key1", testStructSize1{}, time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:      "key2",
			val:      testStructSize1{},
			wantMap0: map[string]string{"key1": "key1", "key2": "key2"},
			wantMap1: map[string]string{},
		},
		{
			name: "1缓存结点，内存模式正序，新增一个更大的",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeMemory))

				node1 := newKVRBTreeCacheNode("key1", testStructSize1{}, time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:      "key2",
			val:      testStructSize2{1, 2, 3, 4},
			wantMap0: map[string]string{"key1": "key1"},
			wantMap1: map[string]string{"key2": "key2"},
		},
		{
			name: "1缓存结点，内存模式正序，新增一个更小的",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeMemory))

				node2 := newKVRBTreeCacheNode("key2", testStructSize2{}, time.Minute)
				_ = client.cacheData.Add("key2", node2)
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node2), node2)

				return client
			},
			key:      "key1",
			val:      testStructSize1{},
			wantMap0: map[string]string{"key1": "key1"},
			wantMap1: map[string]string{"key2": "key2"},
		},
		{
			name: "1缓存结点，内存模式倒序，新增一个更大的",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeMemory), SetOrderByASC(false))

				node1 := newKVRBTreeCacheNode("key1", testStructSize1{}, time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:      "key2",
			val:      testStructSize2{},
			wantMap0: map[string]string{"key2": "key2"},
			wantMap1: map[string]string{"key1": "key1"},
		},
		{
			name: "1缓存结点，内存模式倒序，新增一个更小的",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeMemory), SetOrderByASC(false))

				node2 := newKVRBTreeCacheNode("key2", testStructSize2{}, time.Minute)
				_ = client.cacheData.Add("key2", node2)
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node2), node2)

				return client
			},
			key:      "key1",
			val:      testStructSize1{},
			wantMap0: map[string]string{"key2": "key2"},
			wantMap1: map[string]string{"key1": "key1"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			_ = startClient.Set(context.Background(), tc.key, tc.val, 0)

			//判断一下最前面两个结点是否符合预期

			top0, _ := startClient.priorityData.priorityData.ExtractTop()
			keyMap0 := make(map[string]string)
			for key := range top0.cacheData {
				keyMap0[key] = key
			}
			result0 := true
			for key := range tc.wantMap0 {
				if _, ok := keyMap0[key]; !ok {
					result0 = false
				}
			}
			assert.Equal(t, true, len(keyMap0) == len(tc.wantMap0))
			assert.Equal(t, true, result0)

			top1, err1 := startClient.priorityData.priorityData.ExtractTop()
			if err1 != nil {
				return //如果没有第二个结点，就不用判断了
			}
			keyMap1 := make(map[string]string)
			for key := range top1.cacheData {
				keyMap1[key] = key
			}
			result1 := true
			for key := range tc.wantMap1 {
				if _, ok := keyMap1[key]; !ok {
					result1 = false
				}
			}
			assert.Equal(t, true, len(keyMap1) == len(tc.wantMap1))
			assert.Equal(t, true, result1)
		})
	}
}

func TestWeight(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		val         any
		wantMap0    map[string]string
		wantMap1    map[string]string
	}{
		{
			name: "权重超过最大值",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeWeight))

				node1 := newKVRBTreeCacheNode("key1", testStruct{priorityWeight: math.MaxInt64 / 2}, time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:      "key2",
			val:      testStruct{priorityWeight: math.MaxInt64},
			wantMap0: map[string]string{"key1": "key1", "key2": "key2"},
			wantMap1: map[string]string{},
		},
		{
			name: "权重超过最小值",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetPriorityType(PriorityTypeWeight))

				node1 := newKVRBTreeCacheNode("key1", testStruct{priorityWeight: 0}, time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key:      "key2",
			val:      testStruct{priorityWeight: -1},
			wantMap0: map[string]string{"key1": "key1", "key2": "key2"},
			wantMap1: map[string]string{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			_ = startClient.Set(context.Background(), tc.key, tc.val, 0)

			//判断一下最前面两个结点是否符合预期

			top0, _ := startClient.priorityData.priorityData.ExtractTop()
			keyMap0 := make(map[string]string)
			for key := range top0.cacheData {
				keyMap0[key] = key
			}
			result0 := true
			for key := range tc.wantMap0 {
				if _, ok := keyMap0[key]; !ok {
					result0 = false
				}
			}
			assert.Equal(t, true, len(keyMap0) == len(tc.wantMap0))
			assert.Equal(t, true, result0)

			top1, err1 := startClient.priorityData.priorityData.ExtractTop()
			if err1 != nil {
				return //如果没有第二个结点，就不用判断了
			}
			keyMap1 := make(map[string]string)
			for key := range top1.cacheData {
				keyMap1[key] = key
			}
			result1 := true
			for key := range tc.wantMap1 {
				if _, ok := keyMap1[key]; !ok {
					result1 = false
				}
			}
			assert.Equal(t, true, len(keyMap1) == len(tc.wantMap1))
			assert.Equal(t, true, result1)
		})
	}
}

func TestRBTreeClientGetSet(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		val         string
		wantClient  func() *RBTreeClient
		wantVal     any
		wantErr     error
	}{
		{
			name: "0缓存结点，查询未命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key: "key1",
			val: "value1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				client.cacheNum++
				return client
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "1缓存结点，查询未命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				client.cacheNum++
				return client
			},
			key: "key2",
			val: "value2",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				client.cacheNum++
				_ = client.cacheData.Add("key2", newKVRBTreeCacheNode("key2", "value2", time.Minute))
				client.cacheNum++
				return client
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "1缓存结点，查询命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key: "key1",
			val: "value2",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value2", time.Minute))
				client.cacheNum++
				return client
			},
			wantVal: "value1",
		},
		{
			name: "1缓存结点，查询命中但是过期",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value1", -time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key: "key1",
			val: "value2",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := newKVRBTreeCacheNode("key1", "value2", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			wantVal: "value1",
		},
		{
			name: "1缓存容量，1缓存结点，新增触发淘汰",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(SetCacheLimit(1))

				node1 := newKVRBTreeCacheNode("key1", "value1", time.Minute)
				_ = client.cacheData.Add("key1", node1)
				client.cacheNum++
				client.priorityData.SetCacheNodePriority(client.getValPriorityWeight(node1), node1)

				return client
			},
			key: "key2",
			val: "value2",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key2", newKVRBTreeCacheNode("key2", "value2", time.Minute))
				client.cacheNum++
				return client
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:     "key1",
			wantErr: ErrOnlyKVCanGetSet,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			val := startClient.GetSet(context.Background(), tc.key, tc.val)
			assert.Equal(t, tc.wantErr, val.Err)
			if val.Err != nil {
				return
			}
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
			assert.Equal(t, tc.wantVal, val.Val)
		})
	}
}

func TestRBTreeClientLPUSH(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		value       []any
		wantClient  func() *RBTreeClient
		wantNum     int64
		wantErr     error
	}{
		{
			name: "缓存容量0，新增1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key:   "key1",
			value: []any{"value1"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantNum: 1,
		},
		{
			name: "缓存容量0，新增2",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key:   "key1",
			value: []any{"value1", "value2"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				_ = valList1.Append("value2")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList1,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantNum: 2,
		},
		{
			name: "缓存容量1，新增1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList1,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:   "key1",
			value: []any{"value2"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				_ = valList1.Append("value2")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList1,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantNum: 1,
		},
		{
			name: "缓存容量1，新增1，创建新结点",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList1,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:   "key2",
			value: []any{"value2"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList1,
				}
				_ = client.cacheData.Add("key1", node1)

				valList2 := list.NewLinkedList[any]()
				_ = valList2.Append("value1")
				node2 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList2,
				}
				_ = client.cacheData.Add("key2", node2)

				return client
			},
			wantNum: 1,
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantErr: ErrOnlyListCanLPUSH,
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

func TestRBTreeClientLPop(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		wantClient  func() *RBTreeClient
		wantVal     any
		wantErr     error
	}{
		{
			name: "1缓存结点，未命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "1缓存结点，命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			wantVal: "value1",
		},
		{
			name: "1缓存结点，2个元素，命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList := list.NewLinkedList[any]()
				_ = valList.Append("value1")
				_ = valList.Append("value2")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList := list.NewLinkedList[any]()
				_ = valList.Append("value2")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList,
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantVal: "value1",
		},
		{
			name: "1缓存结点，各1个元素，命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList1 := list.NewLinkedList[any]()
				_ = valList1.Append("value1")
				node1 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList1,
				}
				_ = client.cacheData.Add("key1", node1)

				valList2 := list.NewLinkedList[any]()
				_ = valList2.Append("value2")
				node2 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList2,
				}
				_ = client.cacheData.Add("key2", node2)

				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valList2 := list.NewLinkedList[any]()
				_ = valList2.Append("value2")
				node2 := &rbTreeCacheNode{
					unitType: unitTypeList,
					val:      valList2,
				}
				_ = client.cacheData.Add("key2", node2)

				return client
			},
			wantVal: "value1",
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key: "key1",
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantErr: ErrOnlyListCanLPOP,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			val := startClient.LPop(context.Background(), tc.key)
			assert.Equal(t, tc.wantErr, val.Err)
			if val.Err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val.Val)
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}

func TestRBTreeClientSAdd(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		vals        []any
		wantClient  func() *RBTreeClient
		wantRet     int64
		wantErr     error
	}{
		{
			name: "0缓存结点，新增1元素",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key:  "key1",
			vals: []any{"value1"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 1,
		},
		{
			name: "0缓存结点，新增2元素",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key:  "key1",
			vals: []any{"value1", "value2"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")
				valSet1.Add("value2")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 2,
		},
		{
			name: "1缓存结点，1元素，新增1元素，不重复",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:  "key1",
			vals: []any{"value2"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")
				valSet1.Add("value2")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 1,
		},
		{
			name: "1缓存结点，1元素，新增1元素，重复",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")

				_ = client.cacheData.Add("key1", node1)
				return client
			},
			key:  "key1",
			vals: []any{"value1"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 0,
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key:  "key1",
			vals: []any{"value1"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantRet: 0,
			wantErr: ErrOnlySetCanSAdd,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			ret, err := startClient.SAdd(context.Background(), tc.key, tc.vals...)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRet, ret)
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}

func TestRBTreeClientSRem(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		vals        []any
		wantClient  func() *RBTreeClient
		wantRet     int64
		wantErr     error
	}{
		{
			name: "0缓存结点，删除1元素，报错",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key:  "key1",
			vals: []any{"value1"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			wantRet: 0,
			wantErr: errs.ErrKeyNotExist,
		},
		{
			name: "1缓存结点，1元素，删除1元素，命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:  "key1",
			vals: []any{"value1"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			wantRet: 1,
		},
		{
			name: "1缓存结点，1元素，删除1元素，未命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:  "key1",
			vals: []any{"value2"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 0,
		},
		{
			name: "1缓存结点，2元素，删除1元素，命中",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value1")
				valSet1.Add("value2")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key:  "key1",
			vals: []any{"value1"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				valSet1 := set.NewMapSet[any](mapSetInitSize)
				node1 := &rbTreeCacheNode{
					unitType: unitTypeSet,
					val:      valSet1,
				}
				valSet1.Add("value2")
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 1,
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key:  "key1",
			vals: []any{"value1"},
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantRet: 0,
			wantErr: ErrOnlySetCanSRem,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			val := startClient.SRem(context.Background(), tc.key, tc.vals...)
			assert.Equal(t, tc.wantErr, val.Err)
			if val.Err != nil {
				return
			}
			assert.Equal(t, tc.wantRet, val.Val)
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}

func TestRBTreeClientIncrBy(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		val         int64
		wantClient  func() *RBTreeClient
		wantRet     int64
		wantErr     error
	}{
		{
			name: "0缓存结点，加1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key: "key1",
			val: 1,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 1,
		},
		{
			name: "1缓存结点，缓存值1，加1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key: "key1",
			val: 1,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(2),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 2,
		},
		{
			name: "1缓存结点，缓存值1，加2",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key: "key1",
			val: 2,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(3),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 3,
		},
		{
			name: "1缓存结点，缓存值各1，加1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key1", node1)

				node2 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key2", node2)

				return client
			},
			key: "key1",
			val: 1,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(2),
				}
				_ = client.cacheData.Add("key1", node1)

				node2 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key2", node2)

				return client
			},
			wantRet: 2,
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key: "key1",
			val: 1,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantErr: ErrOnlyNumCanIncrBy,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			val, err := startClient.IncrBy(context.Background(), tc.key, tc.val)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRet, val)
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}

func TestRBTreeClientDecrBy(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		val         int64
		wantClient  func() *RBTreeClient
		wantRet     int64
		wantErr     error
	}{
		{
			name: "0缓存结点，减1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				return client
			},
			key: "key1",
			val: 1,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(-1),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: -1,
		},
		{
			name: "1缓存结点，缓存值1，减1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key: "key1",
			val: 1,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(0),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: 0,
		},
		{
			name: "1缓存结点，缓存值1，减2",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			key: "key1",
			val: 2,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(-1),
				}
				_ = client.cacheData.Add("key1", node1)

				return client
			},
			wantRet: -1,
		},
		{
			name: "1缓存结点，缓存值各1，减1",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key1", node1)

				node2 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key2", node2)

				return client
			},
			key: "key1",
			val: 1,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()

				node1 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(0),
				}
				_ = client.cacheData.Add("key1", node1)

				node2 := &rbTreeCacheNode{
					unitType: unitTypeNum,
					val:      int64(1),
				}
				_ = client.cacheData.Add("key2", node2)

				return client
			},
			wantRet: 0,
		},
		{
			name: "结点类型错误",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			key: "key1",
			val: 1,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient()
				_ = client.cacheData.Add("key1", newKVRBTreeCacheNode("key1", "value1", time.Minute))
				return client
			},
			wantErr: ErrOnlyNumCanDecrBy,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			val, err := startClient.DecrBy(context.Background(), tc.key, tc.val)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRet, val)
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}
