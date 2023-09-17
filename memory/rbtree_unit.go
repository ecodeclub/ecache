package memory

import (
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"time"
)

// 缓存单元类型
const (
	unitTypeKV   = iota + 1 //普通键值对
	unitTypeList            //list，用list.LinkedList[any]实现
	unitTypeSet             //set，用set.MapSet[any]实现
)

// 缓存单元
type rbTreeNode struct {
	key          string        //键
	expiration   time.Duration //有效期
	unitType     int           //单元类型
	val          any           //值有三种情况
	priorityUnit *PriorityUnit
}

func newKVRBTreeUnit(key string, val any, expiration time.Duration) *rbTreeNode {
	return &rbTreeNode{
		key:        key,
		expiration: expiration,
		unitType:   unitTypeKV,
		val:        val,
	}
}

func newListRBTreeUnit(key string) *rbTreeNode {
	return &rbTreeNode{
		key:      key,
		unitType: unitTypeList,
		val:      list.NewLinkedList[any](),
	}
}

func newSetRBTreeUnit(key string) *rbTreeNode {
	return &rbTreeNode{
		key:      key,
		unitType: unitTypeSet,
		val:      set.NewMapSet[any](8),
	}
}
