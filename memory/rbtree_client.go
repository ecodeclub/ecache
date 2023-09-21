package memory

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit/bean/option"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"github.com/ecodeclub/ekit/tree"
	"math"
	"sync"
	"time"
)

var (
	ErrOnlyKVCanSet     = errors.New("ecache: 只有 kv 类型的数据，才能执行 Set")
	ErrOnlyKVCanGet     = errors.New("ecache: 只有 kv 类型的数据，才能执行 Get")
	ErrOnlyKVCanGetSet  = errors.New("ecache: 只有 kv 类型的数据，才能执行 GetSet")
	ErrOnlyListCanLPUSH = errors.New("ecache: 只有 list 类型的数据，才能执行 LPush")
	ErrOnlyListCanLPOP  = errors.New("ecache: 只有 list 类型的数据，才能执行 LPop")
	ErrOnlySetCanSAdd   = errors.New("ecache: 只有 set 类型的数据，才能执行 SAdd")
	ErrOnlySetCanSRem   = errors.New("ecache: 只有 set 类型的数据，才能执行 SRem")
	ErrOnlyNumCanIncrBy = errors.New("ecache: 只有数字类型的数据，才能执行 IncrBy")
	ErrOnlyNumCanDecrBy = errors.New("ecache: 只有数字类型的数据，才能执行 DecrBy")
)

type RBTreeClient struct {
	clientLock *sync.RWMutex //读写锁，保护缓存数据和优先级数据

	cacheData  *tree.RBTree[string, *rbTreeCacheNode] //缓存数据
	cacheNum   int                                    //键值对数量
	cacheLimit int                                    //键值对数量限制，默认没有限制

	priorityData          *CachePriority //优先级数据
	defaultPriorityWeight int            //默认的优先级权重
	maxPriorityWeight     int            //最大优先级权重（倒过来使用小根堆的时候用的）
	orderByASC            bool           //true=按权重从小到大排;按false=权重从大到小排;
}

func NewRBTreeClient(opts ...option.Option[RBTreeClient]) (*RBTreeClient, error) {
	cacheData, err := tree.NewRBTree[string, *rbTreeCacheNode](comparatorRBTreeCacheNode())
	if err != nil {
		return nil, err
	}
	client := &RBTreeClient{
		clientLock:            &sync.RWMutex{},
		cacheData:             cacheData,
		cacheNum:              0,
		cacheLimit:            0,
		priorityData:          newCachePriority(8),
		defaultPriorityWeight: 0,
		maxPriorityWeight:     math.MaxInt32 / 2,
		orderByASC:            true,
	}
	option.Apply(client, opts...)
	return client, nil
}

func SetCacheLimit(cacheLimit int) option.Option[RBTreeClient] {
	return func(opt *RBTreeClient) {
		opt.cacheLimit = cacheLimit
	}
}

func SetDefaultPriorityWeight(priorityWeight int) option.Option[RBTreeClient] {
	return func(opt *RBTreeClient) {
		opt.defaultPriorityWeight = priorityWeight
	}
}

func SetOrderBy(isASC bool) option.Option[RBTreeClient] {
	return func(opt *RBTreeClient) {
		opt.orderByASC = isASC
	}
}

// getValPriorityWeight 获取缓存数据的优先级权重
func (r *RBTreeClient) getValPriorityWeight(input any) int {
	priorityWeight := r.defaultPriorityWeight

	switch val := input.(type) {
	case Priority:
		priorityWeight = val.GetPriorityWeight()
	}

	// 限制一下最小和最大权重，方便用小根堆反向排序时候的操作
	if priorityWeight < 0 {
		priorityWeight = 0
	}
	if priorityWeight > r.maxPriorityWeight {
		priorityWeight = r.maxPriorityWeight
	}

	if r.orderByASC {
		//如果是权重从小到大排，那么直接返回就可以。
		return priorityWeight
	} else {
		//如果是权重从大到小排，那么用权重最大值和大权重做一个差值。
		//这样大权重的结果就变小了，权重越大计算完越小，在小根堆里越靠前。
		return r.maxPriorityWeight - priorityWeight
	}
}

// isFull 键值对数量满了没有
func (r *RBTreeClient) isFull() bool {
	if r.cacheLimit <= 0 {
		return false
	}
	return r.cacheNum >= r.cacheLimit
}

// deleteByPriority 根据优先级淘汰数据
func (r *RBTreeClient) deleteByPriority() {
	topPriorityUnit, topErr := r.priorityData.priorityData.GetTop()
	//这里的error只会是ErrMinHeapIsEmpty
	if topErr != nil {
		return
	}
	if len(topPriorityUnit.cacheData) <= 0 {
		//如果堆结构顶部的结点没有缓存数据，那么就移除这个结点
		_, _ = r.priorityData.priorityData.ExtractTop()
		//直接回去，下一轮继续
		return
	}
	for key, val := range topPriorityUnit.cacheData {
		//删除缓存数据
		r.cacheData.Delete(key)
		r.cacheNum--
		//删除优先级数据
		r.priorityData.DeleteCacheNodePriority(val)
		//删一个就回去，下一轮继续
		break
	}
}

func (r *RBTreeClient) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr == nil {
		//如果没有err，证明能找到缓存数据，修改
		if node.unitType != unitTypeKV {
			return ErrOnlyKVCanSet
		}

		// 覆盖旧值
		node.val = val
		var deadline time.Time
		if expiration > 0 {
			deadline = time.Now().Add(expiration)
		}
		node.deadline = deadline

		//移除旧的优先级数据
		r.priorityData.DeleteCacheNodePriority(node)
		//设置新的优先级数据
		newPriorityWeight := r.getValPriorityWeight(val)
		r.priorityData.SetCacheNodePriority(newPriorityWeight, node)
	} else {
		//如果有err，证明没找到缓存数据，新增

		//容量满了触发淘汰
		for r.isFull() {
			r.deleteByPriority()
		}

		node = newKVRBTreeCacheNode(key, val, expiration)
		cacheErr = r.cacheData.Add(key, node)
		if cacheErr == nil {
			r.cacheNum++
			//设置新的优先级数据
			newPriorityWeight := r.getValPriorityWeight(val)
			r.priorityData.SetCacheNodePriority(newPriorityWeight, node)
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

		newNode := newKVRBTreeCacheNode(key, val, expiration)
		cacheErr = r.cacheData.Add(key, newNode)
		if cacheErr == nil {
			//设置新的优先级数据
			newPriorityWeight := r.getValPriorityWeight(val)
			r.priorityData.SetCacheNodePriority(newPriorityWeight, newNode)
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
			r.priorityData.DeleteCacheNodePriority(checkNode)
			//移除缓存数据
			r.cacheData.Delete(key)
			r.cacheNum--
		}
		// 缓存过期可以归类为找不到
		val.Err = errs.ErrKeyNotExist
		return
	}
	val.Val = node.val
	return
}

func (r *RBTreeClient) GetSet(ctx context.Context, key string, val string) ecache.Value {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	var retVal ecache.Value
	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据
		retVal.Err = errs.ErrKeyNotExist

		//容量满了触发淘汰
		for r.isFull() {
			r.deleteByPriority()
		}

		newNode := newKVRBTreeCacheNode(key, val, 0)
		cacheErr = r.cacheData.Add(key, newNode)
		if cacheErr == nil {
			//设置新的优先级数据
			newPriorityWeight := r.getValPriorityWeight(val)
			r.priorityData.SetCacheNodePriority(newPriorityWeight, newNode)
		}
		return retVal
	}
	//如果没有err，证明能找到缓存数据
	//判断缓存过期没有
	now := time.Now()
	if !node.beforeDeadline(now) {
		//缓存过期，删除缓存
		//移除缓存数据
		r.cacheData.Delete(key)
		r.cacheNum--
		//移除优先级数据
		r.priorityData.DeleteCacheNodePriority(node)
		// 缓存过期可以归类为找不到
		retVal.Err = errs.ErrKeyNotExist

		//容量满了触发淘汰
		for r.isFull() {
			r.deleteByPriority()
		}

		newNode := newKVRBTreeCacheNode(key, val, 0)
		cacheErr = r.cacheData.Add(key, newNode)
		if cacheErr == nil {
			//设置新的优先级数据
			newPriorityWeight := r.getValPriorityWeight(val)
			r.priorityData.SetCacheNodePriority(newPriorityWeight, newNode)
		}
		return retVal
	}
	retVal.Val = node.val

	// 覆盖旧值
	node.val = val
	var deadline time.Time
	node.deadline = deadline

	//移除旧的优先级数据
	r.priorityData.DeleteCacheNodePriority(node)
	//设置新的优先级数据
	newPriorityWeight := r.getValPriorityWeight(val)
	r.priorityData.SetCacheNodePriority(newPriorityWeight, node)

	return retVal
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
		node = newListRBTreeCacheNode(key)
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
		listErr := nodeVal.Add(0, item)
		if listErr != nil {
			return int64(successNum), listErr
		}
		successNum++
	}
	return int64(successNum), nil
}

func (r *RBTreeClient) LPop(ctx context.Context, key string) ecache.Value {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	var retVal ecache.Value
	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据
		retVal.Err = cacheErr
		return retVal
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeList {
		retVal.Err = ErrOnlyListCanLPOP
		return retVal
	}

	// 校验一下缓存结点的类型
	nodeVal, ok := node.val.(*list.LinkedList[any])
	if !ok {
		retVal.Err = ErrOnlyListCanLPOP
		return retVal
	}

	retVal.Val, retVal.Err = nodeVal.Delete(0)
	return retVal
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
		node = newSetRBTreeCacheNode(key)
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
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	var retVal ecache.Value
	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据
		retVal.Err = cacheErr
		return retVal
	}

	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeSet {
		retVal.Err = ErrOnlySetCanSRem
		return retVal
	}

	// 校验一下缓存结点的类型
	nodeVal, ok := node.val.(*set.MapSet[any])
	if !ok {
		retVal.Err = ErrOnlySetCanSRem
		return retVal
	}

	// 依次执行srem
	successNum := 0
	for item := range members {
		isExist := nodeVal.Exist(item)
		if isExist {
			nodeVal.Delete(item)
			successNum++
		}
	}
	retVal.Val = int64(successNum)
	return retVal
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
		node = newIntRBTreeCacheNode(key)
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
		node = newIntRBTreeCacheNode(key)
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
