package memory

import (
	"context"
	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ekit"
	"github.com/ecodeclub/ekit/tree"
	"sync"
	"time"
)

type RBTreeClient struct {
	clientLock            *sync.RWMutex                     //读写锁，保护缓存数据和优先级数据
	cacheData             *tree.RBTree[string, *rbTreeNode] //缓存数据
	priorityData          *CachePriority                    //优先级数据
	defaultPriorityWeight int                               //默认的优先级权重
	orderByASC            bool                              //true=按权重从小到大排;按false=权重从大到小排;
}

func NewRBTreeClient(compare ekit.Comparator[string]) (*RBTreeClient, error) {
	cacheData, err := tree.NewRBTree[string, *rbTreeNode](compare)
	if err != nil {
		return nil, err
	}
	return &RBTreeClient{
		clientLock:            &sync.RWMutex{},
		cacheData:             cacheData,
		priorityData:          newCachePriority(),
		defaultPriorityWeight: 0,
		orderByASC:            true,
	}, nil
}

func ComparatorRBTreeUnit() ekit.Comparator[string] {
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

func (r *RBTreeClient) getValPriorityWeight(input any) int {
	switch val := input.(type) {
	case Priority:
		return val.GetPriorityWeight()
	default:
		return r.defaultPriorityWeight
	}
}

func (r *RBTreeClient) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	newRBTreeUnit := newKVRBTreeUnit(key, val, expiration)
	existRBTreeUnit, cacheErr := r.cacheData.Find(key)
	if cacheErr == nil {
		//如果有err证明找到了，那就是修改
		//覆盖缓存数据
		cacheErr = r.cacheData.Set(key, newRBTreeUnit)
		if cacheErr == nil {
			//移除优先级数据
			priorityUnit := existRBTreeUnit.priorityUnit
			delete(priorityUnit.cacheData, key)
			//设置新的优先级数据
			newPriorityWeight := r.getValPriorityWeight(val)
			r.priorityData.AddUnit(newPriorityWeight, newRBTreeUnit)
		}
	} else {
		//如果有err证明没找到，那就是新增
		cacheErr = r.cacheData.Add(key, newRBTreeUnit)
		if cacheErr == nil {
			//设置新的优先级数据
			newPriorityWeight := r.getValPriorityWeight(val)
			r.priorityData.AddUnit(newPriorityWeight, newRBTreeUnit)
		}
	}
	return nil
}

func (r *RBTreeClient) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) Get(ctx context.Context, key string) (val ecache.Value) {
	existRBTreeUnit, err := r.cacheData.Find(key)
	if err == nil {
		val.Val = existRBTreeUnit.val
	} else {
		val.Err = err
	}
	return
}

func (r *RBTreeClient) GetSet(ctx context.Context, key string, val string) ecache.Value {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) LPop(ctx context.Context, key string) ecache.Value {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) SRem(ctx context.Context, key string, members ...any) ecache.Value {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	//TODO implement me
	panic("implement me")
}
