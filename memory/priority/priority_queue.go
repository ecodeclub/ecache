package priority

import (
	"context"
	"errors"
)

type Queue[T any] interface {
	Push(ctx context.Context, t *T) error
	Pop(ctx context.Context) (*T, error)
	Peek(ctx context.Context) (*T, error)
	Remove(ctx context.Context, t *T) error // 为了支持随机删除而引入的接口，如果不需要随机删除，可以不实现
}

type Comparator[T any] interface {
	Compare(src, dest *T) int
}

type Indexable interface {
	Index() int
	SetIndex(idx int)
}

func NewQueueWithHeap[T any](comparator Comparator[T]) Queue[T] {
	// 这里可以考虑给一个默认的堆容量
	return &QueueWithHeap[T]{
		heap:       make([]*T, 0),
		comparator: comparator,
		len:        0,
	}
}

type QueueWithHeap[T any] struct {
	heap       []*T
	comparator Comparator[T]
	len        int
}

func (q *QueueWithHeap[T]) Push(ctx context.Context, t *T) error {
	if len(q.heap) > q.len {
		q.heap[q.len] = t
	} else {
		q.heap = append(q.heap, t)
	}

	// 如果是可索引的，需要为这个类型设置索引
	if idx, ok := checkIndexable(t); ok {
		idx.SetIndex(q.len)
	}

	q.len++

	q.heapifyUp(q.len - 1)

	return nil
}

func (q *QueueWithHeap[T]) Pop(ctx context.Context) (*T, error) {
	if q.len == 0 {
		return nil, errors.New("队列为空")
	}
	res := q.heap[0]
	q.heap[0] = q.heap[q.len-1]
	q.heap[q.len-1] = nil // let GC do its work
	q.len--

	q.heapifyDown(0)
	return res, nil
}

func (q *QueueWithHeap[T]) Peek(ctx context.Context) (*T, error) {
	if q.len == 0 {
		return nil, errors.New("队列为空")
	}

	return q.heap[0], nil
}

// Remove 随机删除一个元素
// 但是要确保这个元素是在堆里的
func (q *QueueWithHeap[T]) Remove(ctx context.Context, t *T) error {
	idx, ok := checkIndexable(t)
	if !ok {
		return errors.New("只有实现Indexable的数据才能随机删除")
	}

	if idx.Index() >= q.len {
		return errors.New("这个元素不在堆里")
	}

	q.heap[idx.Index()] = q.heap[q.len-1]
	q.heap[q.len-1] = nil // let GC do its work
	q.len--
	q.heapifyDown(idx.Index())
	return nil
}

// heapifyDown 从上往下进行堆化
func (q *QueueWithHeap[T]) heapifyDown(cur int) {
	n := q.len

	// 如果满足 idx <= n - 2 / 2 说明有子节点，需要往下进行堆化
	for cur <= (n-2)>>1 {
		l, r := 2*cur+1, 2*cur+2
		min := l

		if r < n && q.comparator.Compare(q.heap[l], q.heap[r]) > 0 {
			min = r
		}

		// 说明已经满足堆化条件，直接返回
		if q.comparator.Compare(q.heap[cur], q.heap[min]) < 0 {
			return
		}

		// swap
		q.swap(cur, min)

		cur = min
	}
}

// heapifyUp 从下往上进行堆化
func (q *QueueWithHeap[T]) heapifyUp(cur int) {
	for p := (cur - 1) >> 1; cur > 0 && q.comparator.Compare(q.heap[cur], q.heap[p]) < 0; cur, p = p, (p-1)>>1 {
		q.swap(cur, p)
	}
}

// swap 交换下标值为src和dest位置的值，如果实现了Indexable接口，则更新以下索引
func (q *QueueWithHeap[T]) swap(src, dest int) {
	q.heap[src], q.heap[dest] = q.heap[dest], q.heap[src]

	if idx, ok := checkIndexable(q.heap[src]); ok {
		idx.SetIndex(src)
	}

	if idx, ok := checkIndexable(q.heap[dest]); ok {
		idx.SetIndex(dest)
	}
}

func checkIndexable(val any) (Indexable, bool) {
	idx, ok := val.(Indexable)
	return idx, ok
}
