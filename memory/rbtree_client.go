package memory

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"github.com/ecodeclub/ekit/tree"
	"sync"
	"time"
)

var (
	ErrOnlyListCanLPUSH = errors.New("只有 list 类型的数据，才能执行 LPush")
	ErrOnlyListCanLPOP  = errors.New("只有 list 类型的数据，才能执行 LPop")
	ErrOnlySetCanSAdd   = errors.New("只有 set 类型的数据，才能执行 SAdd")
	ErrOnlySetCanSRem   = errors.New("只有 set 类型的数据，才能执行 SRem")
	ErrOnlyNumCanIncrBy = errors.New("只有数字类型的数据，才能执行 IncrBy")
	ErrOnlyNumCanDecrBy = errors.New("只有数字类型的数据，才能执行 DecrBy")
)

type RBTreeClient struct {
	clientLock            *sync.RWMutex                     //读写锁，保护缓存数据和优先级数据
	cacheData             *tree.RBTree[string, *rbTreeNode] //缓存数据
	memoryLimit           int                               //缓存键值对数量限制
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

func (r *RBTreeClient) isFull() bool {
	return r.cacheData.Size() < r.memoryLimit
}

func (r *RBTreeClient) deleteByPriority() {
	topPriorityUnit, topErr := r.priorityData.priorityData.GetTop()
	if topErr != nil {
		return
	}
	for key, val := range topPriorityUnit.cacheData {
		r.cacheData.Delete(key)
		r.priorityData.DeleteUnit(val)
		break
	}
}

func (r *RBTreeClient) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	existNode, cacheErr := r.cacheData.Find(key)
	if cacheErr == nil {
		//如果没有err，证明能找到缓存数据，修改
		existNode.val = val
		// 计算过期时间
		var deadline time.Time
		if expiration > 0 {
			deadline = time.Now().Add(expiration)
		}
		existNode.deadline = deadline
		//移除优先级数据
		r.priorityData.DeleteUnit(existNode)
		//设置新的优先级数据
		newPriorityWeight := r.getValPriorityWeight(val)
		r.priorityData.AddUnit(newPriorityWeight, existNode)
	} else {
		//如果有err，证明没找到缓存数据，新增

		//容量满了触发淘汰
		for r.isFull() {
			r.deleteByPriority()
		}

		newNode := newKVRBTreeNode(key, val, expiration)
		cacheErr = r.cacheData.Add(key, newNode)
		if cacheErr == nil {
			//设置新的优先级数据
			newPriorityWeight := r.getValPriorityWeight(val)
			r.priorityData.AddUnit(newPriorityWeight, newNode)
		}
	}
	return nil
}

func (r *RBTreeClient) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	r.clientLock.RLock()
	_, cacheErr := r.cacheData.Find(key)
	r.clientLock.RUnlock()

	if cacheErr == nil {
		//如果没有err，证明能找到缓存数据，那SetNX就失败了
		return false, nil
	} else {
		//如果有err，证明没找到缓存数据，可以进行SetNX
		r.clientLock.Lock()
		defer r.clientLock.Unlock()

		//容量满了触发淘汰
		for r.isFull() {
			r.deleteByPriority()
		}

		newNode := newKVRBTreeNode(key, val, expiration)
		cacheErr = r.cacheData.Add(key, newNode)
		if cacheErr == nil {
			//设置新的优先级数据
			newPriorityWeight := r.getValPriorityWeight(val)
			r.priorityData.AddUnit(newPriorityWeight, newNode)
		}
		return true, nil
	}
}

func (r *RBTreeClient) Get(ctx context.Context, key string) (val ecache.Value) {
	r.clientLock.RLock()
	node, cacheErr := r.cacheData.Find(key)
	r.clientLock.RUnlock()

	if cacheErr != nil {
		//如果有err，证明没找到缓存数据
		val.Err = errs.ErrKeyNotExist
	}
	//如果没有err，证明能找到缓存数据
	//判断缓存过期没有
	now := time.Now()
	if !node.beforeDeadline(now) {
		// 缓存过期，删除缓存，需要加写锁。
		r.clientLock.Lock()
		defer r.clientLock.Unlock()
		// 二次校验，防止别的线程抢先删除了
		checkNode, checkCacheErr := r.cacheData.Find(key)
		if checkCacheErr != nil {
			val.Err = errs.ErrKeyNotExist
		}
		if !checkNode.beforeDeadline(now) {
			//移除优先级数据
			r.priorityData.DeleteUnit(checkNode)
			//移除缓存数据
			r.cacheData.Delete(key)
		}
		// 缓存过期可以归类为找不到
		val.Err = errs.ErrKeyNotExist
		return
	}
	val.Val = node.val
	return
}

func (r *RBTreeClient) GetSet(ctx context.Context, key string, val string) ecache.Value {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr == nil {
		//如果没有err，证明能找到缓存数据
		if node.unitType != unitTypeList {
			return 0, ErrOnlyListCanLPUSH
		}
	} else {
		//如果有err，证明没找到缓存数据，要先新增缓存结点
		node = newListRBTreeNode(key)
		cacheErr = r.cacheData.Add(key, node)
		if cacheErr != nil {
			return 0, cacheErr
		}
	}

	// 校验一下缓存结点的类型
	nodeVal, ok := node.val.(*list.LinkedList[any])
	if !ok {
		return 0, ErrOnlyListCanLPUSH
	}

	// 依次执行 lpush
	successNum := 0
	for item := range val {
		listErr := nodeVal.Append(item)
		if listErr != nil {
			return int64(successNum), listErr
		}
		successNum++
	}
	return int64(successNum), nil
}

func (r *RBTreeClient) LPop(ctx context.Context, key string) ecache.Value {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr == nil {
		//如果没有err，证明能找到缓存数据
		if node.unitType != unitTypeSet {
			return 0, ErrOnlySetCanSAdd
		}
	} else {
		//如果有err，证明没找到缓存数据，要先新增缓存结点
		node = newSetRBTreeNode(key)
		cacheErr = r.cacheData.Add(key, node)
		if cacheErr != nil {
			return 0, cacheErr
		}
	}

	// 校验一下缓存结点的类型
	nodeVal, ok := node.val.(*set.MapSet[any])
	if !ok {
		return 0, ErrOnlySetCanSAdd
	}

	// 依次执行sadd
	successNum := 0
	for item := range members {
		isExist := nodeVal.Exist(item)
		if !isExist {
			nodeVal.Add(item)
			successNum++
		}
	}
	return int64(successNum), nil
}

func (r *RBTreeClient) SRem(ctx context.Context, key string, members ...any) ecache.Value {
	//TODO implement me
	panic("implement me")
}

func (r *RBTreeClient) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr == nil {
		//如果没有err，证明能找到缓存数据
		if node.unitType != unitTypeNum {
			return 0, ErrOnlyNumCanIncrBy
		}
	} else {
		//如果有err，证明没找到缓存数据，要先新增缓存结点
		node = newIntRBTreeNode(key)
		cacheErr = r.cacheData.Add(key, node)
		if cacheErr != nil {
			return 0, cacheErr
		}
	}

	// 校验一下缓存结点的类型
	nodeVal, ok := node.val.(int64)
	if !ok {
		return 0, ErrOnlyNumCanIncrBy
	}

	// 修改值
	newVal := nodeVal + value
	node.val = newVal

	return newVal, nil
}

func (r *RBTreeClient) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr == nil {
		//如果没有err，证明能找到缓存数据
		if node.unitType != unitTypeNum {
			return 0, ErrOnlyNumCanDecrBy
		}
	} else {
		//如果有err，证明没找到缓存数据，要先新增缓存结点
		node = newIntRBTreeNode(key)
		cacheErr = r.cacheData.Add(key, node)
		if cacheErr != nil {
			return 0, cacheErr
		}
	}

	// 校验一下缓存结点的类型
	nodeVal, ok := node.val.(int64)
	if !ok {
		return 0, ErrOnlyNumCanDecrBy
	}

	// 修改值
	newVal := nodeVal - value
	node.val = newVal

	return newVal, nil
}
