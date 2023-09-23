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
	"reflect"
	"sync"
	"time"
)

var (
	ErrWrongPriorityType = errors.New("ecache: 错误的优先级类型")
	ErrOnlyKVCanSet      = errors.New("ecache: 只有 kv 类型的数据，才能执行 Set")
	ErrOnlyKVCanGet      = errors.New("ecache: 只有 kv 类型的数据，才能执行 Get")
	ErrOnlyKVNXCanSetNX  = errors.New("ecache: 只有 SetNX 创建的数据，才能执行 SetNX")
	ErrOnlyKVCanGetSet   = errors.New("ecache: 只有 kv 类型的数据，才能执行 GetSet")
	ErrOnlyListCanLPUSH  = errors.New("ecache: 只有 list 类型的数据，才能执行 LPush")
	ErrOnlyListCanLPOP   = errors.New("ecache: 只有 list 类型的数据，才能执行 LPop")
	ErrOnlySetCanSAdd    = errors.New("ecache: 只有 set 类型的数据，才能执行 SAdd")
	ErrOnlySetCanSRem    = errors.New("ecache: 只有 set 类型的数据，才能执行 SRem")
	ErrOnlyNumCanIncrBy  = errors.New("ecache: 只有数字类型的数据，才能执行 IncrBy")
	ErrOnlyNumCanDecrBy  = errors.New("ecache: 只有数字类型的数据，才能执行 DecrBy")
)

// 四种优先级类型
const (
	PriorityTypeLRU    = iota + 1 //最近最少使用
	PriorityTypeLFU               //最不经常使用
	PriorityTypeMemory            //内存大小（可以设置正序倒序）
	PriorityTypeWeight            //权重（可以设置正序倒序）
)

var (
	//这两个变量还没有想到好的办法，option模式感觉不好搞，如果外部没有传设置的option，怎么办呢
	minHeapInitSize = 8 //优先级数据，小根堆的初始大小
	mapSetInitSize  = 8 //缓存set结点，set.MapSet的初始大小
)

type RBTreeClient struct {
	clientLock *sync.RWMutex //读写锁，保护缓存数据和优先级数据

	cacheData  *tree.RBTree[string, *rbTreeCacheNode] //缓存数据
	cacheNum   int                                    //键值对数量
	cacheLimit int                                    //键值对数量限制，默认0，表示没有限制

	priorityData          *CachePriority //优先级数据
	priorityType          int            //默认的优先级类型
	defaultPriorityWeight int64          //默认的优先级权重
	maxPriorityWeight     int64          //最大优先级权重（倒过来使用小根堆的时候用的）
	orderByASC            bool           //true=按权重从小到大排;按false=权重从大到小排;
}

func NewRBTreeClient(opts ...option.Option[RBTreeClient]) (*RBTreeClient, error) {
	cacheData, _ := tree.NewRBTree[string, *rbTreeCacheNode](comparatorRBTreeCacheNode())
	//这里的error只会是ErrRBTreeComparatorIsNull，传了compare就不可能出现的，直接忽略

	client := &RBTreeClient{
		clientLock:            &sync.RWMutex{},
		cacheData:             cacheData,
		cacheNum:              0,
		cacheLimit:            0,
		priorityData:          newCachePriority(minHeapInitSize),
		priorityType:          PriorityTypeWeight,
		defaultPriorityWeight: 0,
		maxPriorityWeight:     math.MaxInt64 / 2, //一般不会有权重会这么大
		orderByASC:            true,
	}
	option.Apply(client, opts...)

	if client.priorityType != PriorityTypeLRU && client.priorityType != PriorityTypeLFU &&
		client.priorityType != PriorityTypeMemory && client.priorityType != PriorityTypeWeight {
		return nil, ErrWrongPriorityType
	}

	//如果是lru调用时间越早，或者lfu调用次数越少，越先淘汰，这里就必须是升序
	if client.priorityType == PriorityTypeLRU || client.priorityType == PriorityTypeLFU {
		client.orderByASC = true
	}

	return client, nil
}

func SetCacheLimit(cacheLimit int) option.Option[RBTreeClient] {
	return func(opt *RBTreeClient) {
		opt.cacheLimit = cacheLimit
	}
}

func SetPriorityType(priorityType int) option.Option[RBTreeClient] {
	return func(opt *RBTreeClient) {
		opt.priorityType = priorityType
	}
}

func SetDefaultPriorityWeight(priorityWeight int64) option.Option[RBTreeClient] {
	return func(opt *RBTreeClient) {
		opt.defaultPriorityWeight = priorityWeight
	}
}

func SetOrderByASC(isASC bool) option.Option[RBTreeClient] {
	return func(opt *RBTreeClient) {
		opt.orderByASC = isASC
	}
}

// getValPriorityWeight 获取缓存数据的优先级权重
func (r *RBTreeClient) getValPriorityWeight(node *rbTreeCacheNode) int64 {
	if r.priorityType == PriorityTypeLRU {
		if node.callTime.IsZero() {
			return 0
		}
		return node.callTime.Unix()
	}

	if r.priorityType == PriorityTypeLFU {
		return int64(node.callTimes)
	}

	priorityWeight := r.defaultPriorityWeight

	if r.priorityType == PriorityTypeMemory {
		//这里node.val是any，用unsafe.Sizeof是不行的
		priorityWeight = int64(reflect.TypeOf(node.val).Size())
	}

	if r.priorityType == PriorityTypeWeight {
		//如果实现了Priority接口，那么就用接口的方法获取优先级权重
		val, ok := node.val.(Priority)
		if ok {
			priorityWeight = val.GetPriorityWeight()
		}
	}

	if r.priorityType == PriorityTypeMemory || r.priorityType == PriorityTypeWeight {
		// 限制一下最小和最大权重，方便用小根堆反向排序时候的操作
		if priorityWeight < 0 {
			priorityWeight = 0
		}
		if priorityWeight > r.maxPriorityWeight {
			priorityWeight = r.maxPriorityWeight
		}
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
		return false //0表示没有限制
	}
	return r.cacheNum >= r.cacheLimit
}

// deleteByPriority 根据优先级淘汰数据
func (r *RBTreeClient) deleteByPriority() bool {
	topPriorityUnit, topErr := r.priorityData.priorityData.GetTop()
	if topErr != nil {
		//这里的err只会是ErrMinHeapIsEmpty，不用管。
		//理论上缓存结点和优先级结点是对应上的，不应该出现走这里的情况，走这里铁有bug。
		return false
	}
	if len(topPriorityUnit.cacheData) <= 0 {
		_, _ = r.priorityData.priorityData.ExtractTop() //如果堆结构顶部的结点没有缓存数据，那么就移除这个结点
		return true                                     //直接回去，下一轮继续
	}
	for key, val := range topPriorityUnit.cacheData {
		r.cacheData.Delete(key) //删除缓存数据
		r.cacheNum--
		r.priorityData.DeleteCacheNodePriority(val) //删除优先级数据
		break                                       //删一个就有位置了，后面的不够再说
	}
	return false
}

func (r *RBTreeClient) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，只会是ErrRBTreeNotRBNode，证明没找到缓存数据，执行新增
		if r.isFull() {
			//容量满了触发淘汰，这里不需要循环因为已经 lock 住了
			needContinue := true
			for needContinue {
				//这里需要循环，是因为有的优先级结点是空的
				needContinue = r.deleteByPriority()
			}
		}

		node = newKVRBTreeCacheNode(key, val, expiration)
		_ = r.cacheData.Add(key, node) //这里的error只会是ErrRBTreeSameRBNode，理论上不会出现
		r.cacheNum++

		r.priorityData.SetCacheNodePriority(r.getValPriorityWeight(node), node) //设置新的优先级数据
		return nil
	}
	//如果没有err，证明能找到缓存数据，执行修改
	if node.unitType != unitTypeKV {
		return ErrOnlyKVCanSet
	}
	// 覆盖旧值
	node.val = val

	var deadline time.Time
	if expiration != 0 {
		deadline = time.Now().Add(expiration)
	}
	node.deadline = deadline

	r.priorityData.DeleteCacheNodePriority(node)                            //移除旧的优先级数据
	r.priorityData.SetCacheNodePriority(r.getValPriorityWeight(node), node) //设置新的优先级数据
	return nil
}

func (r *RBTreeClient) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，只会是ErrRBTreeNotRBNode，证明没找到缓存数据，可以进行SetNX
		node = newKVNXRBTreeCacheNode(key, val, expiration)
		_ = r.cacheData.Add(key, node) //这里的error只会是ErrRBTreeSameRBNode，理论上不会出现
		return true, nil
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeKVNX {
		return false, ErrOnlyKVNXCanSetNX
	}
	//判断是不是自己的
	if node.val == val {
		var deadline time.Time
		if expiration != 0 {
			deadline = time.Now().Add(expiration)
		}
		node.deadline = deadline //是自己的，则更新过时间
		return true, nil
	}
	//如果不是自己的，先判断过期没有
	now := time.Now()
	if !node.beforeDeadline(now) {
		// 缓存过期，先删除旧的，然后进行SetNX
		r.cacheData.Delete(key)
		newNode := newKVNXRBTreeCacheNode(key, val, expiration)
		_ = r.cacheData.Add(key, newNode) //这里的error只会是ErrRBTreeSameRBNode，理论上不会出现
		return true, nil
	}
	return false, nil
}

func (r *RBTreeClient) Get(ctx context.Context, key string) (val ecache.Value) {
	r.clientLock.RLock()
	node, cacheErr := r.cacheData.Find(key)
	r.clientLock.RUnlock()
	if cacheErr != nil {
		//如果有err，只会是ErrRBTreeNotRBNode，证明没找到缓存数据
		val.Err = errs.ErrKeyNotExist
		return
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeKV {
		val.Err = ErrOnlyKVCanGet
		return
	}
	//判断缓存过期没有
	now := time.Now()
	if !node.beforeDeadline(now) {
		r.doubleCheckInGet(key, now)
		val.Err = errs.ErrKeyNotExist // 缓存过期可以归类为找不到
		return
	}
	val.Val = node.val
	node.callTime = now
	node.callTimes++
	if r.priorityType == PriorityTypeLRU || r.priorityType == PriorityTypeLFU {
		r.priorityData.DeleteCacheNodePriority(node)                            //移除旧的优先级数据
		r.priorityData.SetCacheNodePriority(r.getValPriorityWeight(node), node) //设置新的优先级数据
	}
	return
}

// doubleCheckInGet 执行 Get 时的二次校验，防止别的线程抢先删除了，裂开写是为了好测试
func (r *RBTreeClient) doubleCheckInGet(key string, now time.Time) {
	// 缓存过期，删除缓存，需要加写锁。
	r.clientLock.Lock()
	defer r.clientLock.Unlock()
	// 二次校验，防止别的线程抢先删除了
	checkNode, checkCacheErr := r.cacheData.Find(key)
	if checkCacheErr != nil {
		return
	}
	if !checkNode.beforeDeadline(now) {
		r.cacheData.Delete(key) //移除缓存数据
		r.cacheNum--
		r.priorityData.DeleteCacheNodePriority(checkNode) //移除优先级数据
	}
	return
}

func (r *RBTreeClient) GetSet(ctx context.Context, key string, val string) ecache.Value {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	var retVal ecache.Value
	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，只会是ErrRBTreeNotRBNode，证明没找到缓存数据
		retVal.Err = errs.ErrKeyNotExist

		if r.isFull() {
			//容量满了触发淘汰，这里不需要循环因为已经 lock 住了
			needContinue := true
			for needContinue {
				//这里需要循环，是因为有的优先级结点是空的
				needContinue = r.deleteByPriority()
			}
		}

		newNode := newKVRBTreeCacheNode(key, val, 0)
		_ = r.cacheData.Add(key, newNode) //这里的error只会是ErrRBTreeSameRBNode，理论上不会出现
		r.cacheNum++
		r.priorityData.SetCacheNodePriority(r.getValPriorityWeight(newNode), newNode) //设置新的优先级数据

		return retVal
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeKV {
		retVal.Err = ErrOnlyKVCanGetSet
		return retVal
	}
	//这里不需要判断缓存过期没有，取出旧值放入新值就完事了
	now := time.Now()
	retVal.Val = node.val
	node.callTime = now
	node.callTimes++

	node.val = val                                                          //覆盖旧值
	r.priorityData.DeleteCacheNodePriority(node)                            //移除旧的优先级数据
	r.priorityData.SetCacheNodePriority(r.getValPriorityWeight(node), node) //设置新的优先级数据

	return retVal
}

func (r *RBTreeClient) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，只会是ErrRBTreeNotRBNode，证明没找到缓存数据，要先新增缓存结点
		node = newListRBTreeCacheNode(key)
		_ = r.cacheData.Add(key, node) //这里的error只会是ErrRBTreeSameRBNode，理论上不会出现
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeList {
		return 0, ErrOnlyListCanLPUSH
	}
	nodeVal, _ := node.val.(*list.LinkedList[any])

	// 依次执行 lpush
	successNum := 0
	for item := range val {
		_ = nodeVal.Add(0, item)
		// 这里的err只会是NewErrIndexOutOfRange，lpush的逻辑是不会出现的
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
		//如果有err，只会是ErrRBTreeNotRBNode，证明没找到缓存数据
		retVal.Err = errs.ErrKeyNotExist
		return retVal
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeList {
		retVal.Err = ErrOnlyListCanLPOP
		return retVal
	}
	nodeVal, _ := node.val.(*list.LinkedList[any])

	retVal.Val, retVal.Err = nodeVal.Delete(0)

	if nodeVal.Len() == 0 {
		r.cacheData.Delete(key) //如果列表为空，删除缓存结点
	}
	return retVal
}

func (r *RBTreeClient) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，只会是ErrRBTreeNotRBNode，证明没找到缓存数据，要先新增缓存结点
		node = newSetRBTreeCacheNode(key, mapSetInitSize)
		_ = r.cacheData.Add(key, node) //这里的error只会是ErrRBTreeSameRBNode，理论上不会出现
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeSet {
		return 0, ErrOnlySetCanSAdd
	}
	nodeVal, _ := node.val.(*set.MapSet[any])

	// 依次执行sadd
	successNum := 0
	for _, item := range members {
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
		retVal.Err = errs.ErrKeyNotExist
		return retVal
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeSet {
		retVal.Err = ErrOnlySetCanSRem
		return retVal
	}
	nodeVal, _ := node.val.(*set.MapSet[any])

	// 依次执行srem
	successNum := 0
	for _, item := range members {
		isExist := nodeVal.Exist(item)
		if isExist {
			nodeVal.Delete(item)
			successNum++
		}
	}
	//如果集合为空，删除缓存结点
	if len(nodeVal.Keys()) == 0 {
		r.cacheData.Delete(key)
	}
	retVal.Val = int64(successNum)
	return retVal
}

func (r *RBTreeClient) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，只会是ErrRBTreeNotRBNode，证明没找到缓存数据，要先新增缓存结点
		node = newIntRBTreeCacheNode(key)
		_ = r.cacheData.Add(key, node) //这里的error只会是ErrRBTreeSameRBNode，理论上不会出现
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeNum {
		return 0, ErrOnlyNumCanIncrBy
	}
	nodeVal, _ := node.val.(int64)

	// 修改值
	newVal := nodeVal + value
	node.val = newVal

	return newVal, nil
}

func (r *RBTreeClient) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，只会是ErrRBTreeNotRBNode，证明没找到缓存数据，要先新增缓存结点
		node = newIntRBTreeCacheNode(key)
		_ = r.cacheData.Add(key, node) //这里的error只会是ErrRBTreeSameRBNode，理论上不会出现
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != unitTypeNum {
		return 0, ErrOnlyNumCanDecrBy
	}
	nodeVal, _ := node.val.(int64)

	// 修改值
	newVal := nodeVal - value
	node.val = newVal

	return newVal, nil
}
