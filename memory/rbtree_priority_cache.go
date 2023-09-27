// Copyright 2023 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit/bean/option"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"github.com/ecodeclub/ekit/tree"
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

// 优先级类型
const (
	PriorityTypeDefault = iota + 1 //优先级，默认
	PriorityTypeLRU                //最近最少使用
	PriorityTypeLFU                //最不经常使用
)

var (
	//这两个变量还没有想到好的办法，option模式感觉不好搞，如果外部没有传设置的option怎么办呢
	priorityQueueInitSize = 8 //优先级数据，小根堆的初始大小
	mapSetInitSize        = 8 //缓存set结点，set.MapSet的初始大小
)

type RBTreePriorityCache struct {
	globalLock *sync.RWMutex //内部全局读写锁，保护缓存数据和优先级数据

	cacheData  *tree.RBTree[string, *rbTreeCacheNode] //缓存数据
	cacheNum   int                                    //键值对数量
	cacheLimit int                                    //键值对数量限制，默认0，表示没有限制

	priorityData *cachePriority //优先级数据
	priorityType int            //优先级类型
}

func NewRBTreePriorityCache(opts ...option.Option[RBTreePriorityCache]) (*RBTreePriorityCache, error) {
	rbTree, _ := tree.NewRBTree[string, *rbTreeCacheNode](comparatorRBTreeCacheNode())
	//这里的error传了compare就不可能出现的，直接忽略

	cache := &RBTreePriorityCache{
		globalLock:   &sync.RWMutex{},
		cacheData:    rbTree,
		cacheNum:     0,
		cacheLimit:   0,
		priorityData: newCachePriority(priorityQueueInitSize),
		priorityType: PriorityTypeDefault,
	}
	option.Apply(cache, opts...)

	if cache.priorityType != PriorityTypeDefault &&
		cache.priorityType != PriorityTypeLRU &&
		cache.priorityType != PriorityTypeLFU {
		return nil, ErrWrongPriorityType
	}

	go cache.autoClean()

	return cache, nil
}

func WithCacheLimit(cacheLimit int) option.Option[RBTreePriorityCache] {
	return func(opt *RBTreePriorityCache) {
		opt.cacheLimit = cacheLimit
	}
}

func WithPriorityType(priorityType int) option.Option[RBTreePriorityCache] {
	return func(opt *RBTreePriorityCache) {
		opt.priorityType = priorityType
	}
}

// autoClean 自动清理过期缓存
func (r *RBTreePriorityCache) autoClean() {
	for {
		time.Sleep(time.Second)
		r.globalLock.RLock()
		_, values := r.cacheData.KeyValues()
		r.globalLock.RUnlock()

		now := time.Now()
		for _, value := range values {
			if !value.beforeDeadline(now) {
				r.doubleCheckWhenExpire(value.key, now)
			}
		}
	}
}

// calculatePriority 获取缓存数据的优先级权重
func (r *RBTreePriorityCache) calculatePriority(node *rbTreeCacheNode) int64 {
	if r.priorityType == PriorityTypeLRU {
		if node.lastCallTime.IsZero() {
			return 0
		}
		return node.lastCallTime.Unix()
	}

	if r.priorityType == PriorityTypeLFU {
		return int64(node.totalCallTimes)
	}

	var priorityWeight int64
	//如果实现了Priority接口，那么就用接口的方法获取优先级权重
	val, ok := node.value.(Priority)
	if ok {
		priorityWeight = val.GetPriority()
	}
	return priorityWeight
}

// isFull 键值对数量满了没有
func (r *RBTreePriorityCache) isFull() bool {
	if r.cacheLimit <= 0 {
		return false //0表示没有限制
	}
	return r.cacheNum >= r.cacheLimit
}

// deleteByPriority 根据优先级淘汰数据
func (r *RBTreePriorityCache) deleteByPriority() {
	//这里不需要加锁，因为触发淘汰的时候肯定是走了set逻辑，已经锁过了
	needContinue := true
	for needContinue {
		//这里需要循环，因为有的优先级结点是空的
		topPriorityNode, topErr := r.priorityData.priorityQueue.Dequeue()
		if topErr != nil {
			//走这里铁有bug，不可能缓存满了但是优先级队列是空的
			return
		}
		if topPriorityNode.cacheNode == nil {
			needContinue = true
			continue //优先级结点是空的，直接回去，继续下一轮
		}
		r.cacheData.Delete(topPriorityNode.cacheNode.key)
		r.cacheNum--
		needContinue = false
	}
}

func (r *RBTreePriorityCache) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据，执行新增
		if r.isFull() {
			r.deleteByPriority() //容量满了触发淘汰
		}
		node = newKVRBTreeCacheNode(key, val, expiration)
		_ = r.cacheData.Add(key, node) //这里的error理论上不会出现
		r.cacheNum++
		r.priorityData.setCacheNodePriority(node, r.calculatePriority(node)) //设置新的优先级数据
		return nil
	}
	//如果没有err，证明能找到缓存数据，执行修改
	if node.unitType != rbTreeCacheNodeTypeKV {
		return ErrOnlyKVCanSet
	}
	node.value = val //覆盖旧值
	node.setExpiration(expiration)
	r.priorityData.deleteCacheNodePriority(node)                         //移除旧的优先级数据
	r.priorityData.setCacheNodePriority(node, r.calculatePriority(node)) //设置新的优先级数据
	return nil
}

func (r *RBTreePriorityCache) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据，可以进行SetNX
		node = newKVNXRBTreeCacheNode(key, val, expiration)
		_ = r.cacheData.Add(key, node) //这里的error理论上不会出现
		return true, nil
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != rbTreeCacheNodeTypeKVNX {
		return false, ErrOnlyKVNXCanSetNX
	}
	//判断是不是自己的
	if node.value == val {
		node.setExpiration(expiration) //是自己的，则更新过期时间
		return true, nil
	}
	//如果不是自己的，先判断过期没有
	now := time.Now()
	if !node.beforeDeadline(now) {
		// 如果是过期的，则可以进行SetNX，key一样的，覆盖就好
		node.value = val
		node.setExpiration(expiration)
		return true, nil
	}
	return false, nil
}

func (r *RBTreePriorityCache) Get(ctx context.Context, key string) (val ecache.Value) {
	r.globalLock.RLock()
	node, cacheErr := r.cacheData.Find(key)
	r.globalLock.RUnlock()

	if cacheErr != nil {
		//如果有err，证明没找到缓存数据
		val.Err = errs.ErrKeyNotExist
		return
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != rbTreeCacheNodeTypeKV {
		val.Err = ErrOnlyKVCanGet
		return
	}
	//判断缓存到期没有
	now := time.Now()
	if !node.beforeDeadline(now) {
		r.doubleCheckWhenExpire(key, now)
		val.Err = errs.ErrKeyNotExist // 缓存过期可以归类为找不到
		return
	}
	val.Val = node.value

	if r.priorityType == PriorityTypeLRU || r.priorityType == PriorityTypeLFU {
		node.lastCallTime = now
		node.totalCallTimes++
		r.priorityData.deleteCacheNodePriority(node)                         //移除旧的优先级数据
		r.priorityData.setCacheNodePriority(node, r.calculatePriority(node)) //设置新的优先级数据
	}
	return
}

// doubleCheckWhenExpire 缓存过期删除时的二次校验，防止别的线程抢先删除了
func (r *RBTreePriorityCache) doubleCheckWhenExpire(key string, now time.Time) {
	// 缓存过期，删除缓存，需要加写锁
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	// 二次校验，防止别的线程抢先删除了
	checkNode, checkCacheErr := r.cacheData.Find(key)
	if checkCacheErr != nil {
		return
	}
	if !checkNode.beforeDeadline(now) {
		r.cacheData.Delete(key) //移除缓存数据
		r.cacheNum--
		r.priorityData.deleteCacheNodePriority(checkNode) //移除优先级数据
	}
	return
}

func (r *RBTreePriorityCache) GetSet(ctx context.Context, key string, val string) ecache.Value {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	var retVal ecache.Value
	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据
		retVal.Err = errs.ErrKeyNotExist

		if r.isFull() {
			r.deleteByPriority() //容量满了触发淘汰
		}

		newNode := newKVRBTreeCacheNode(key, val, 0)
		_ = r.cacheData.Add(key, newNode) //这里的error理论上不会出现
		r.cacheNum++
		r.priorityData.setCacheNodePriority(newNode, r.calculatePriority(newNode)) //设置新的优先级数据

		return retVal
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != rbTreeCacheNodeTypeKV {
		retVal.Err = ErrOnlyKVCanGetSet
		return retVal
	}
	//这里不需要判断缓存过期没有，取出旧值放入新值就完事了
	retVal.Val = node.value
	node.value = val

	if r.priorityType == PriorityTypeLRU || r.priorityType == PriorityTypeLFU {
		now := time.Now()
		node.lastCallTime = now
		node.totalCallTimes++
	}

	r.priorityData.deleteCacheNodePriority(node)                         //移除旧的优先级数据
	r.priorityData.setCacheNodePriority(node, r.calculatePriority(node)) //设置新的优先级数据

	return retVal
}

func (r *RBTreePriorityCache) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据，要先新增缓存结点
		node = newListRBTreeCacheNode(key)
		_ = r.cacheData.Add(key, node) //这里的error理论上不会出现
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != rbTreeCacheNodeTypeList {
		return 0, ErrOnlyListCanLPUSH
	}
	nodeVal, _ := node.value.(*list.LinkedList[any])

	// 依次执行 lpush
	successNum := 0
	for item := range val {
		_ = nodeVal.Add(0, item) //这里的error理论上是不会出现的
		successNum++
	}
	return int64(successNum), nil
}

func (r *RBTreePriorityCache) LPop(ctx context.Context, key string) ecache.Value {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	var retVal ecache.Value

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据
		retVal.Err = errs.ErrKeyNotExist
		return retVal
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != rbTreeCacheNodeTypeList {
		retVal.Err = ErrOnlyListCanLPOP
		return retVal
	}
	nodeVal, _ := node.value.(*list.LinkedList[any])

	retVal.Val, retVal.Err = nodeVal.Delete(0)

	if nodeVal.Len() == 0 {
		r.cacheData.Delete(key) //如果列表为空，删除缓存结点
	}
	return retVal
}

func (r *RBTreePriorityCache) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据，要先新增缓存结点
		node = newSetRBTreeCacheNode(key, mapSetInitSize)
		_ = r.cacheData.Add(key, node) //这里的error理论上不会出现
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != rbTreeCacheNodeTypeSet {
		return 0, ErrOnlySetCanSAdd
	}
	nodeVal, _ := node.value.(*set.MapSet[any])

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

func (r *RBTreePriorityCache) SRem(ctx context.Context, key string, members ...any) ecache.Value {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	var retVal ecache.Value

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据
		retVal.Err = errs.ErrKeyNotExist
		return retVal
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != rbTreeCacheNodeTypeSet {
		retVal.Err = ErrOnlySetCanSRem
		return retVal
	}
	nodeVal, _ := node.value.(*set.MapSet[any])

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

func (r *RBTreePriorityCache) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据，要先新增缓存结点
		node = newIntRBTreeCacheNode(key)
		_ = r.cacheData.Add(key, node) //这里的error理论上不会出现
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != rbTreeCacheNodeTypeNum {
		return 0, ErrOnlyNumCanIncrBy
	}
	nodeVal, _ := node.value.(int64)

	// 修改值
	newVal := nodeVal + value
	node.value = newVal

	return newVal, nil
}

func (r *RBTreePriorityCache) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		//如果有err，证明没找到缓存数据，要先新增缓存结点
		node = newIntRBTreeCacheNode(key)
		_ = r.cacheData.Add(key, node) //这里的error理论上不会出现
	}
	//如果没有err，证明能找到缓存数据
	if node.unitType != rbTreeCacheNodeTypeNum {
		return 0, ErrOnlyNumCanDecrBy
	}
	nodeVal, _ := node.value.(int64)

	// 修改值
	newVal := nodeVal - value
	node.value = newVal

	return newVal, nil
}
