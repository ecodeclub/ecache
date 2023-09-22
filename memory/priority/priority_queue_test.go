package priority

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueueWithHeap_Push(t *testing.T) {

	ctx := context.TODO()

	testCases := []struct {
		name string

		q Queue[testNode]

		t *testNode

		before func(q Queue[testNode])

		wantLen int
		wantRes []*testNode
	}{
		{
			// 队列为空，插入一个元素
			name: "insert one element, queue is empty",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			t:    &testNode{data: 1},
			before: func(q Queue[testNode]) {

			},
			wantLen: 1,
			wantRes: []*testNode{
				{data: 1, index: 0},
			},
		},
		{
			// 队列不为空，插入一个元素
			name: "insert one element, and no heapify",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			t:    &testNode{data: 5},
			before: func(q Queue[testNode]) {
				_ = q.Push(ctx, &testNode{data: 2})
				_ = q.Push(ctx, &testNode{data: 3})
				_ = q.Push(ctx, &testNode{data: 4})
				_ = q.Push(ctx, &testNode{data: 6})
				_ = q.Push(ctx, &testNode{data: 7})
			},
			wantLen: 6,
			wantRes: []*testNode{
				{data: 2, index: 0},
				{data: 3, index: 1},
				{data: 4, index: 2},
				{data: 6, index: 3},
				{data: 7, index: 4},
				{data: 5, index: 5},
			},
		},
		{
			// 队列不为空，插入一个元素
			name: "insert one element, and heapify",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			t:    &testNode{data: 1},
			before: func(q Queue[testNode]) {
				_ = q.Push(ctx, &testNode{data: 2})
				_ = q.Push(ctx, &testNode{data: 3})
				_ = q.Push(ctx, &testNode{data: 4})
				_ = q.Push(ctx, &testNode{data: 6})
				_ = q.Push(ctx, &testNode{data: 7})
			},
			wantLen: 6,
			wantRes: []*testNode{
				{data: 1, index: 0},
				{data: 3, index: 1},
				{data: 2, index: 2},
				{data: 6, index: 3},
				{data: 7, index: 4},
				{data: 4, index: 5},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(tc.q)

			err := tc.q.Push(ctx, tc.t)
			if err != nil {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.wantLen, tc.q.(*QueueWithHeap[testNode]).len)

			for i, v := range tc.wantRes {
				assert.Equal(t, v, tc.q.(*QueueWithHeap[testNode]).heap[i])
			}
		})
	}
}

func TestQueueWithHeap_Pop(t *testing.T) {

	ctx := context.TODO()

	testCases := []struct {
		name string

		q Queue[testNode]

		before func(q Queue[testNode])

		wantLen  int
		wantRes  *testNode
		wantHeap []*testNode
		wantErr  error
	}{
		{
			// 队列为空，弹出一个元素
			name: "pop one element, queue is empty",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			before: func(q Queue[testNode]) {

			},
			wantErr: errors.New("队列为空"),
		},
		{
			// 当队列只有一个元素，弹出一个元素
			name: "pop one element, queue has one element",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			before: func(q Queue[testNode]) {
				_ = q.Push(ctx, &testNode{data: 2})
			},
			wantLen:  0,
			wantRes:  &testNode{data: 2, index: 0},
			wantHeap: []*testNode{},
		},
		{
			// 堆里多个元素，弹出一个元素
			name: "pop one element, queue has many elements",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			before: func(q Queue[testNode]) {
				_ = q.Push(ctx, &testNode{data: 2})
				_ = q.Push(ctx, &testNode{data: 3})
				_ = q.Push(ctx, &testNode{data: 4})
				_ = q.Push(ctx, &testNode{data: 6})
				_ = q.Push(ctx, &testNode{data: 7})
			},
			wantLen: 4,
			wantRes: &testNode{data: 2, index: 0},
			wantHeap: []*testNode{
				{data: 3, index: 0},
				{data: 6, index: 1},
				{data: 4, index: 2},
				{data: 7, index: 3},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(tc.q)

			res, err := tc.q.Pop(ctx)
			assert.Equal(t, tc.wantErr, err)

			if err != nil {
				return
			}

			assert.Equal(t, tc.wantRes, res)

			assert.Equal(t, tc.wantLen, tc.q.(*QueueWithHeap[testNode]).len)

			for i, v := range tc.wantHeap {
				assert.Equal(t, v, tc.q.(*QueueWithHeap[testNode]).heap[i])
			}
		})
	}
}

func TestQueueWithHeap_Peek(t *testing.T) {

	ctx := context.TODO()

	testCases := []struct {
		name string

		q Queue[testNode]

		before func(q Queue[testNode])

		wantRes *testNode
		wantErr error
	}{
		{
			// 队列为空，peek一个元素
			name: "peek one element, queue is empty",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			before: func(q Queue[testNode]) {

			},
			wantErr: errors.New("队列为空"),
		},
		{
			// 堆里多个元素，peek一个元素
			name: "peek one element, queue has many elements",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			before: func(q Queue[testNode]) {
				_ = q.Push(ctx, &testNode{data: 2})
				_ = q.Push(ctx, &testNode{data: 3})
				_ = q.Push(ctx, &testNode{data: 4})
				_ = q.Push(ctx, &testNode{data: 6})
				_ = q.Push(ctx, &testNode{data: 7})
			},
			wantRes: &testNode{data: 2, index: 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(tc.q)

			res, err := tc.q.Peek(ctx)
			assert.Equal(t, tc.wantErr, err)

			if err != nil {
				return
			}

			assert.Equal(t, tc.wantRes, res)
		})
	}
}

func TestQueueWithHeap_Remove(t *testing.T) {

	ctx := context.TODO()

	testCases := []struct {
		name string

		q Queue[testNode]
		t *testNode

		before func(q Queue[testNode])

		wantLen  int
		wantHeap []*testNode
		wantErr  error
	}{
		{
			// 删除一个不在队列中的元素
			name: "remove one element, element not in queue",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			t:    &testNode{data: 2},
			before: func(q Queue[testNode]) {

			},
			wantErr: errors.New("这个元素不在堆里"),
		},
		{
			// 删除一个在队列中的元素
			name: "remove one element, element in queue",
			q:    NewQueueWithHeap[testNode](&testComparator{}),
			t:    &testNode{data: 3, index: 1},
			before: func(q Queue[testNode]) {
				_ = q.Push(ctx, &testNode{data: 2})
				_ = q.Push(ctx, &testNode{data: 3})
				_ = q.Push(ctx, &testNode{data: 4})
				_ = q.Push(ctx, &testNode{data: 6})
				_ = q.Push(ctx, &testNode{data: 7})
				_ = q.Push(ctx, &testNode{data: 9})
				_ = q.Push(ctx, &testNode{data: 10})
			},
			wantLen: 6,
			wantHeap: []*testNode{
				{data: 2, index: 0},
				{data: 6, index: 1},
				{data: 4, index: 2},
				{data: 10, index: 3},
				{data: 7, index: 4},
				{data: 9, index: 5},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(tc.q)

			err := tc.q.Remove(ctx, tc.t)
			assert.Equal(t, tc.wantErr, err)

			assert.Equal(t, tc.wantLen, tc.q.(*QueueWithHeap[testNode]).len)

			for i, v := range tc.wantHeap {
				assert.Equal(t, v, tc.q.(*QueueWithHeap[testNode]).heap[i])
			}
		})
	}
}

func TestQueueWithHeap_Remove_Not_Indexable(t *testing.T) {

	q := NewQueueWithHeap[int](&testIntComparator{})

	arg := 1

	err := q.Remove(context.Background(), &arg)

	assert.Equal(t, errors.New("只有实现Indexable的数据才能随机删除"), err)
}

type testIntComparator struct{}

func (t *testIntComparator) Compare(src, dest *int) int {
	if *src > *dest {
		return 1
	} else if *src < *dest {
		return -1
	} else {
		return 0
	}
}

type testComparator struct{}

func (t *testComparator) Compare(src, dest *testNode) int {
	if src.data > dest.data {
		return 1
	} else if src.data < dest.data {
		return -1
	} else {
		return 0
	}
}

type testNode struct {
	index int

	data int
}

func (t *testNode) Index() int {
	return t.index
}

func (t *testNode) SetIndex(idx int) {
	t.index = idx
}
