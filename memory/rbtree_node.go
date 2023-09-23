package memory

import (
	"github.com/ecodeclub/ekit"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"time"
)

// 缓存结点类型
const (
	unitTypeKV   = iota + 1 //普通键值对，只有普通键值对会参与淘汰
	unitTypeKVNX            //NX键值对，这里和普通键值对区分开，因为 NX 键值对不参与淘汰
	unitTypeList            //list，用list.LinkedList[any]实现
	unitTypeSet             //set，用set.MapSet[any]实现
	unitTypeNum             //int64，给IncrBy和DecrBy用
)

// 缓存结点
type rbTreeCacheNode struct {
	key          string        //键
	unitType     int           //单元类型
	val          any           //值有四种情况
	deadline     time.Time     //有效期，默认0，表示永不过期
	callTime     time.Time     //缓存最后一次被调用的时间
	callTimes    int           //缓存被调用的次数
	priorityUnit *priorityNode //优先级数据的映射
}

func newKVRBTreeCacheNode(key string, val any, expiration time.Duration) *rbTreeCacheNode {
	var deadline time.Time
	if expiration != 0 {
		deadline = time.Now().Add(expiration)
	}

	return &rbTreeCacheNode{
		key:      key,
		unitType: unitTypeKV,
		val:      val,
		deadline: deadline,
	}
}

func newKVNXRBTreeCacheNode(key string, val any, expiration time.Duration) *rbTreeCacheNode {
	var deadline time.Time
	if expiration != 0 {
		deadline = time.Now().Add(expiration)
	}

	return &rbTreeCacheNode{
		key:      key,
		unitType: unitTypeKVNX,
		val:      val,
		deadline: deadline,
	}
}

func newListRBTreeCacheNode(key string) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:      key,
		unitType: unitTypeList,
		val:      list.NewLinkedList[any](),
	}
}

func newSetRBTreeCacheNode(key string, initSize int) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:      key,
		unitType: unitTypeSet,
		val:      set.NewMapSet[any](initSize),
	}
}

func newIntRBTreeCacheNode(key string) *rbTreeCacheNode {
	return &rbTreeCacheNode{
		key:      key,
		unitType: unitTypeNum,
		val:      int64(0),
	}
}

// comparatorRBTreeCacheNode 红黑树结点的比较方式
func comparatorRBTreeCacheNode() ekit.Comparator[string] {
	return func(src string, dst string) int {
		if src < dst {
			return -1
		} else if src == dst {
			return 0
		} else {
			return 1
		}
	}
}

// beforeDeadline 检查一下传入的时间是不是在缓存有效时间之前
func (node *rbTreeCacheNode) beforeDeadline(checkTime time.Time) bool {
	if node.deadline.IsZero() {
		return true // 如果没有设置过期时间，那就不会过期
	}
	return checkTime.Before(node.deadline) // 否则比较一下校验时间是不是在过期时间之前
}
