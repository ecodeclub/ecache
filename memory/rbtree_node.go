package memory

import (
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"time"
)

// 缓存结点类型
const (
	unitTypeKV   = iota + 1 //普通键值对
	unitTypeList            //list，用list.LinkedList[any]实现
	unitTypeSet             //set，用set.MapSet[any]实现
	unitTypeNum             //int64，给IncrBy和DecrBy用
)

// 缓存结点
type rbTreeNode struct {
	key          string    //键
	deadline     time.Time //有效期
	unitType     int       //单元类型
	val          any       //值有四种情况
	priorityUnit *PriorityUnit
}

func newKVRBTreeNode(key string, val any, expiration time.Duration) *rbTreeNode {
	var deadline time.Time
	if expiration > 0 {
		deadline = time.Now().Add(expiration)
	}

	return &rbTreeNode{
		key:      key,
		deadline: deadline,
		unitType: unitTypeKV,
		val:      val,
	}
}

func newListRBTreeNode(key string) *rbTreeNode {
	return &rbTreeNode{
		key:      key,
		unitType: unitTypeList,
		val:      list.NewLinkedList[any](),
	}
}

func newSetRBTreeNode(key string) *rbTreeNode {
	return &rbTreeNode{
		key:      key,
		unitType: unitTypeSet,
		val:      set.NewMapSet[any](8),
	}
}

func newIntRBTreeNode(key string) *rbTreeNode {
	return &rbTreeNode{
		key:      key,
		unitType: unitTypeNum,
		val:      0,
	}
}

// 检查一下传入的时间是不是在缓存有效时间之前
func (node *rbTreeNode) beforeDeadline(checkTime time.Time) bool {
	if node.deadline.IsZero() {
		// 如果没有设置过期时间，那就不会过期
		return true
	}
	// 否则比较一下校验时间是不是在过期时间之前
	return checkTime.Before(node.deadline)
}
