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

package priority

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/ecodeclub/ekit/queue"

	"github.com/ecodeclub/ecache"
	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit/bean/option"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/set"
	"github.com/ecodeclub/ekit/tree"
)

var (
	errOnlyListCanLPUSH = errors.New("ecache: 只有 list 类型的数据，才能执行 LPush")
	errOnlyListCanLPOP  = errors.New("ecache: 只有 list 类型的数据，才能执行 LPop")
	errOnlySetCanSAdd   = errors.New("ecache: 只有 set 类型的数据，才能执行 SAdd")
	errOnlySetCanSRem   = errors.New("ecache: 只有 set 类型的数据，才能执行 SRem")
	errOnlyNumCanIncrBy = errors.New("ecache: 只有数字类型的数据，才能执行 IncrBy")
	errOnlyNumCanDecrBy = errors.New("ecache: 只有数字类型的数据，才能执行 DecrBy")
)

type RBTreePriorityCache struct {
	globalLock      *sync.RWMutex                          //内部全局读写锁，保护缓存数据和优先级数据
	cacheData       *tree.RBTree[string, *rbTreeCacheNode] //缓存数据
	cacheNum        int                                    //缓存中总键值对数量
	cacheLimit      int                                    //键值对数量限制，默认MaxInt32，约等于没有限制
	priorityData    *queue.PriorityQueue[*rbTreeCacheNode] //优先级数据
	defaultPriority int                                    //默认优先级
	cleanInterval   time.Duration
	// 集合类型的值的初始化容量
	collectionCap int
}

func NewRBTreePriorityCache(opts ...option.Option[RBTreePriorityCache]) (*RBTreePriorityCache, error) {
	cache, _ := newRBTreePriorityCache(opts...)
	go cache.autoClean()
	return cache, nil
}

func newRBTreePriorityCache(opts ...option.Option[RBTreePriorityCache]) (*RBTreePriorityCache, error) {
	rbTree, _ := tree.NewRBTree[string, *rbTreeCacheNode](comparatorRBTreeCacheNodeByKey())

	const (
		priorityQueueDefaultSize = 8 //优先级队列的初始大小
		collectionDefaultCap     = 8 //缓存结点中set.MapSet的初始大小
	)
	priorityQueue := queue.NewPriorityQueue[*rbTreeCacheNode](priorityQueueDefaultSize, comparatorRBTreeCacheNodeByPriority())
	cache := &RBTreePriorityCache{
		globalLock:   &sync.RWMutex{},
		cacheData:    rbTree,
		cacheNum:     0,
		cacheLimit:   math.MaxInt32,
		priorityData: priorityQueue,
		// 暂时设置为一秒间隔
		cleanInterval: time.Second,
		collectionCap: collectionDefaultCap,
	}
	option.Apply(cache, opts...)

	return cache, nil
}

// WithCacheLimit 设置所允许的最大键值对数量
func WithCacheLimit(cacheLimit int) option.Option[RBTreePriorityCache] {
	return func(opt *RBTreePriorityCache) {
		opt.cacheLimit = cacheLimit
	}
}

func WithDefaultPriority(priority int) option.Option[RBTreePriorityCache] {
	return func(opt *RBTreePriorityCache) {
		opt.defaultPriority = priority
	}
}

func (r *RBTreePriorityCache) Set(_ context.Context, key string, val any, expiration time.Duration) error {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		if r.isFull() {
			r.deleteNodeByPriority()
		}
		node = newKVRBTreeCacheNode(key, val, expiration)
		r.addNode(node)
		return nil
	}
	node.replace(val, expiration)
	return nil
}

// addNode 把缓存结点添加到缓存结构中
func (r *RBTreePriorityCache) addNode(node *rbTreeCacheNode) {
	_ = r.cacheData.Add(node.key, node) //这里的error理论上不会出现
	r.cacheNum++
	r.addNodeToPriority(node)
}

// deleteNode 把缓存结点从缓存结构中移除
func (r *RBTreePriorityCache) deleteNode(node *rbTreeCacheNode) {
	r.cacheData.Delete(node.key)
	r.cacheNum--
	r.deleteNodeFromPriority(node)
}

func (r *RBTreePriorityCache) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		node = newKVRBTreeCacheNode(key, val, expiration)
		r.addNode(node)

		return true, nil
	}

	if !node.beforeDeadline(time.Now()) {
		node.replace(val, expiration) //过期的，key一样，直接覆盖

		return true, nil
	}

	return false, nil
}

func (r *RBTreePriorityCache) Get(ctx context.Context, key string) (val ecache.Value) {
	r.globalLock.RLock()
	node, cacheErr := r.cacheData.Find(key)
	r.globalLock.RUnlock()

	if cacheErr != nil {
		val.Err = errs.ErrKeyNotExist

		return
	}

	now := time.Now()
	if !node.beforeDeadline(now) {
		r.doubleCheckWhenExpire(node, now)
		val.Err = errs.ErrKeyNotExist // 缓存过期归类为找不到

		return
	}
	val.Val = node.value

	return
}

// doubleCheckWhenExpire 缓存过期时的二次校验，防止被抢先删除了
func (r *RBTreePriorityCache) doubleCheckWhenExpire(node *rbTreeCacheNode, now time.Time) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	checkNode, checkCacheErr := r.cacheData.Find(node.key)
	if checkCacheErr != nil {
		return //被抢先删除了
	}
	if !checkNode.beforeDeadline(now) {
		r.deleteNode(checkNode)
	}
}

func (r *RBTreePriorityCache) GetSet(ctx context.Context, key string, val string) ecache.Value {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	var retVal ecache.Value

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		retVal.Err = errs.ErrKeyNotExist
		if r.isFull() {
			r.deleteNodeByPriority()
		}
		node = newKVRBTreeCacheNode(key, val, 0)
		r.addNode(node)

		return retVal
	}

	//这里不需要判断缓存过期没有，取出旧值放入新值就完事了
	retVal.Val = node.value
	node.value = val

	return retVal
}

func (r *RBTreePriorityCache) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		if r.isFull() {
			r.deleteNodeByPriority()
		}
		node = newListRBTreeCacheNode(key)
		r.addNode(node)
	}

	nodeVal, ok := node.value.(*list.LinkedList[any])
	if !ok {
		return 0, errOnlyListCanLPUSH
	}

	var successNum int64
	for item := range val {
		_ = nodeVal.Add(0, item) //这里的error理论上是不会出现的
		successNum++
	}

	return successNum, nil
}

func (r *RBTreePriorityCache) LPop(ctx context.Context, key string) ecache.Value {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	var retVal ecache.Value

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		retVal.Err = errs.ErrKeyNotExist

		return retVal
	}

	nodeVal, ok := node.value.(*list.LinkedList[any])
	if !ok {
		retVal.Err = errOnlyListCanLPOP

		return retVal
	}

	retVal.Val, retVal.Err = nodeVal.Delete(0) //lpop就是删除并获取list的第一个元素

	if nodeVal.Len() == 0 {
		r.deleteNode(node) //如果列表为空就删除缓存结点
	}

	return retVal
}

func (r *RBTreePriorityCache) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		if r.isFull() {
			r.deleteNodeByPriority()
		}
		node = newSetRBTreeCacheNode(key, r.collectionCap)
		r.addNode(node)
	}

	nodeVal, ok := node.value.(*set.MapSet[any])
	if !ok {
		return 0, errOnlySetCanSAdd
	}

	var successNum int64
	for _, item := range members {
		isExist := nodeVal.Exist(item)
		if !isExist {
			nodeVal.Add(item)
			successNum++
		}
	}

	return successNum, nil
}

func (r *RBTreePriorityCache) SRem(_ context.Context, key string, members ...any) (int64, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		return 0, errs.ErrKeyNotExist
	}

	nodeVal, ok := node.value.(*set.MapSet[any])
	if !ok {
		return 0, errOnlySetCanSRem
	}

	var successNum int64
	for _, item := range members {
		isExist := nodeVal.Exist(item)
		if isExist {
			nodeVal.Delete(item)
			successNum++
		}
	}

	if len(nodeVal.Keys()) == 0 {
		r.deleteNode(node) //如果集合为空，删除缓存结点
	}
	return successNum, nil
}

func (r *RBTreePriorityCache) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		if r.isFull() {
			r.deleteNodeByPriority()
		}
		node = newIntRBTreeCacheNode(key)
		r.addNode(node)
	}

	nodeVal, ok := node.value.(int64)
	if !ok {
		return 0, errOnlyNumCanIncrBy
	}

	newVal := nodeVal + value
	node.value = newVal

	return newVal, nil
}

func (r *RBTreePriorityCache) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()

	node, cacheErr := r.cacheData.Find(key)
	if cacheErr != nil {
		if r.isFull() {
			r.deleteNodeByPriority()
		}
		node = newIntRBTreeCacheNode(key)
		r.addNode(node)
	}

	nodeVal, ok := node.value.(int64)
	if !ok {
		return 0, errOnlyNumCanDecrBy
	}

	newVal := nodeVal - value
	node.value = newVal

	return newVal, nil
}

// calculatePriority 获取缓存数据的优先级权重
func (r *RBTreePriorityCache) calculatePriority(node *rbTreeCacheNode) int {
	priority := r.defaultPriority

	//如果实现了Priority接口，那么就用接口的方法获取优先级权重
	val, ok := node.value.(Priority)
	if ok {
		priority = val.Priority()
	}

	return priority
}

// addNodeToPriority 把缓存结点添加到优先级数据中去
func (r *RBTreePriorityCache) addNodeToPriority(node *rbTreeCacheNode) {
	node.priority = r.calculatePriority(node)
	_ = r.priorityData.Enqueue(node)
}

// deleteNodeFromPriority 从优先级数据中移除缓存结点
func (r *RBTreePriorityCache) deleteNodeFromPriority(node *rbTreeCacheNode) {
	//优先级队列无法随机删除结点
	//这里的方案是把优先级数据中的缓存结点置空，并标记为已删除
	//等到触发淘汰的时候再处理
	node.truncate()
}

// isFull 键值对数量满了没有
func (r *RBTreePriorityCache) isFull() bool {
	return r.cacheNum >= r.cacheLimit
}

// deleteNodeByPriority 根据优先级淘汰缓存结点【调用该方法必须先获得锁】
func (r *RBTreePriorityCache) deleteNodeByPriority() {
	for {
		//这里需要循环，因为有的优先级结点是空的
		topNode, topErr := r.priorityData.Dequeue()
		if topErr != nil {
			return //走这里铁有bug，不可能缓存满了但是优先级队列是空的
		}
		if topNode.isDeleted {
			continue //空结点，直接回去，继续下一轮
		}
		// 结点非空，删除缓存
		r.cacheData.Delete(topNode.key)
		r.cacheNum--

		return
	}
}

// autoClean 自动清理过期缓存
func (r *RBTreePriorityCache) autoClean() {
	ticker := time.NewTicker(r.cleanInterval)
	defer ticker.Stop()
	for range ticker.C {
		r.globalLock.RLock()
		_, values := r.cacheData.KeyValues()
		r.globalLock.RUnlock()

		now := time.Now()
		for _, value := range values {
			if !value.beforeDeadline(now) {
				r.doubleCheckWhenExpire(value, now)
			}
		}
	}
}
