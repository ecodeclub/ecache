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

package lru

import (
	"fmt"
	"testing"
)

func Example() {
	// Create a new list and put some numbers in it.
	l := newLinkedList[int]()
	e4 := l.PushBack(4)
	e1 := l.PushFront(1)
	l.InsertBefore(3, e4)
	l.InsertAfter(2, e1)

	// Iterate through list and print its contents.
	for e := l.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}

	// Output:
	// 1
	// 2
	// 3
	// 4
}

func checkLinkedListLen[T any](t *testing.T, l *linkedList[T], len int) bool {
	if n := l.Len(); n != len {
		t.Errorf("l.Len() = %d, want %d", n, len)
		return false
	}
	return true
}

func checkLinkedListPointers[T any](t *testing.T, l *linkedList[T], es []*element[T]) {
	root := &l.root

	if !checkLinkedListLen[T](t, l, len(es)) {
		return
	}

	// zero length LinkLists must be the zero value or properly initialized (sentinel circle)
	if len(es) == 0 {
		if l.root.next != nil && l.root.next != root || l.root.prev != nil && l.root.prev != root {
			t.Errorf("l.root.next = %p, l.root.prev = %p; both should both be nil or %p", l.root.next, l.root.prev, root)
		}
		return
	}
	// len(es) > 0

	// check internal and external prev/next connections
	for i, e := range es {
		prev := root
		Prev := (*element[T])(nil)
		if i > 0 {
			prev = es[i-1]
			Prev = prev
		}
		if p := e.prev; p != prev {
			t.Errorf("elt[%d](%p).prev = %p, want %p", i, e, p, prev)
		}
		if p := e.Prev(); p != Prev {
			t.Errorf("elt[%d](%p).Prev() = %p, want %p", i, e, p, Prev)
		}

		next := root
		Next := (*element[T])(nil)
		if i < len(es)-1 {
			next = es[i+1]
			Next = next
		}
		if n := e.next; n != next {
			t.Errorf("elt[%d](%p).next = %p, want %p", i, e, n, next)
		}
		if n := e.Next(); n != Next {
			t.Errorf("elt[%d](%p).Next() = %p, want %p", i, e, n, Next)
		}
	}
}

func TestLinkedList(t *testing.T) {
	l := newLinkedList[any]()
	checkLinkedListPointers(t, l, []*element[any]{})
	// Single element LinkList
	e := l.PushFront("a")
	checkLinkedListPointers(t, l, []*element[any]{e})
	l.MoveToFront(e)
	checkLinkedListPointers(t, l, []*element[any]{e})
	l.MoveToBack(e)
	checkLinkedListPointers(t, l, []*element[any]{e})
	l.Remove(e)
	checkLinkedListPointers(t, l, []*element[any]{})

	// Bigger LinkList
	e2 := l.PushFront(2)
	e1 := l.PushFront(1)
	e3 := l.PushBack(3)
	e4 := l.PushBack("banana")
	checkLinkedListPointers(t, l, []*element[any]{e1, e2, e3, e4})

	l.Remove(e2)
	checkLinkedListPointers(t, l, []*element[any]{e1, e3, e4})

	l.MoveToFront(e3) // move from middle
	checkLinkedListPointers(t, l, []*element[any]{e3, e1, e4})

	l.MoveToFront(e1)
	l.MoveToBack(e3) // move from middle
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e3})

	l.MoveToFront(e3) // move from back
	checkLinkedListPointers(t, l, []*element[any]{e3, e1, e4})
	l.MoveToFront(e3) // should be no-op
	checkLinkedListPointers(t, l, []*element[any]{e3, e1, e4})

	l.MoveToBack(e3) // move from front
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e3})
	l.MoveToBack(e3) // should be no-op
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e3})

	e2 = l.InsertBefore(2, e1) // insert before front
	checkLinkedListPointers(t, l, []*element[any]{e2, e1, e4, e3})
	l.Remove(e2)
	e2 = l.InsertBefore(2, e4) // insert before middle
	checkLinkedListPointers(t, l, []*element[any]{e1, e2, e4, e3})
	l.Remove(e2)
	e2 = l.InsertBefore(2, e3) // insert before back
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e2, e3})
	l.Remove(e2)

	e2 = l.InsertAfter(2, e1) // insert after front
	checkLinkedListPointers(t, l, []*element[any]{e1, e2, e4, e3})
	l.Remove(e2)
	e2 = l.InsertAfter(2, e4) // insert after middle
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e2, e3})
	l.Remove(e2)
	e2 = l.InsertAfter(2, e3) // insert after back
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e3, e2})
	l.Remove(e2)

	// Check standard iteration.
	sum := 0
	for e := l.Front(); e != nil; e = e.Next() {
		if i, ok := e.Value.(int); ok {
			sum += i
		}
	}
	if sum != 4 {
		t.Errorf("sum over l = %d, want 4", sum)
	}

	// Clear all elements by iterating
	var next *element[any]
	for e := l.Front(); e != nil; e = next {
		next = e.Next()
		l.Remove(e)
	}
	checkLinkedListPointers(t, l, []*element[any]{})
}

func checkLinkedList[T int](t *testing.T, l *linkedList[T], es []any) {
	if !checkLinkedListLen[T](t, l, len(es)) {
		return
	}

	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		le := e.Value
		if le != es[i] {
			t.Errorf("elt[%d].Value = %v, want %v", i, le, es[i])
		}
		i++
	}
}

func TestExtendingEle(t *testing.T) {
	l1 := newLinkedList[int]()
	l2 := newLinkedList[int]()

	l1.PushBack(1)
	l1.PushBack(2)
	l1.PushBack(3)

	l2.PushBack(4)
	l2.PushBack(5)

	l3 := newLinkedList[int]()
	l3.PushBackList(l1)
	checkLinkedList(t, l3, []any{1, 2, 3})
	l3.PushBackList(l2)
	checkLinkedList(t, l3, []any{1, 2, 3, 4, 5})

	l3 = newLinkedList[int]()
	l3.PushFrontList(l2)
	checkLinkedList(t, l3, []any{4, 5})
	l3.PushFrontList(l1)
	checkLinkedList(t, l3, []any{1, 2, 3, 4, 5})

	checkLinkedList(t, l1, []any{1, 2, 3})
	checkLinkedList(t, l2, []any{4, 5})

	l3 = newLinkedList[int]()
	l3.PushBackList(l1)
	checkLinkedList(t, l3, []any{1, 2, 3})
	l3.PushBackList(l3)
	checkLinkedList(t, l3, []any{1, 2, 3, 1, 2, 3})

	l3 = newLinkedList[int]()
	l3.PushFrontList(l1)
	checkLinkedList(t, l3, []any{1, 2, 3})
	l3.PushFrontList(l3)
	checkLinkedList(t, l3, []any{1, 2, 3, 1, 2, 3})

	l3 = newLinkedList[int]()
	l1.PushBackList(l3)
	checkLinkedList(t, l1, []any{1, 2, 3})
	l1.PushFrontList(l3)
	checkLinkedList(t, l1, []any{1, 2, 3})
}

func TestRemoveEle(t *testing.T) {
	l := newLinkedList[int]()
	e1 := l.PushBack(1)
	e2 := l.PushBack(2)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2})
	e := l.Front()
	l.Remove(e)
	checkLinkedListPointers(t, l, []*element[int]{e2})
	l.Remove(e)
	checkLinkedListPointers(t, l, []*element[int]{e2})
}

func TestIssue4103Ele(t *testing.T) {
	l1 := newLinkedList[int]()
	l1.PushBack(1)
	l1.PushBack(2)

	l2 := newLinkedList[int]()
	l2.PushBack(3)
	l2.PushBack(4)

	e := l1.Front()
	l2.Remove(e) // l2 should not change because e is not an element of l2
	if n := l2.Len(); n != 2 {
		t.Errorf("l2.Len() = %d, want 2", n)
	}

	l1.InsertBefore(8, e)
	if n := l1.Len(); n != 3 {
		t.Errorf("l1.Len() = %d, want 3", n)
	}
}

func TestIssue6349Ele(t *testing.T) {
	l := newLinkedList[int]()
	l.PushBack(1)
	l.PushBack(2)

	e := l.Front()
	l.Remove(e)
	if e.Value != 1 {
		t.Errorf("e.value = %d, want 1", e.Value)
	}
	if e.Next() != nil {
		t.Errorf("e.Next() != nil")
	}
	if e.Prev() != nil {
		t.Errorf("e.Prev() != nil")
	}
}

func TestMoveEle(t *testing.T) {
	l := newLinkedList[int]()
	e1 := l.PushBack(1)
	e2 := l.PushBack(2)
	e3 := l.PushBack(3)
	e4 := l.PushBack(4)

	l.MoveAfter(e3, e3)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2, e3, e4})
	l.MoveBefore(e2, e2)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2, e3, e4})

	l.MoveAfter(e3, e2)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2, e3, e4})
	l.MoveBefore(e2, e3)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2, e3, e4})

	l.MoveBefore(e2, e4)
	checkLinkedListPointers(t, l, []*element[int]{e1, e3, e2, e4})
	e2, e3 = e3, e2

	l.MoveBefore(e4, e1)
	checkLinkedListPointers(t, l, []*element[int]{e4, e1, e2, e3})
	e1, e2, e3, e4 = e4, e1, e2, e3

	l.MoveAfter(e4, e1)
	checkLinkedListPointers(t, l, []*element[int]{e1, e4, e2, e3})
	e2, e3, e4 = e4, e2, e3

	l.MoveAfter(e2, e3)
	checkLinkedListPointers(t, l, []*element[int]{e1, e3, e2, e4})
}

func TestZeroLinkedList(t *testing.T) {
	var l1 = new(linkedList[int])
	l1.PushFront(1)
	checkLinkedList(t, l1, []any{1})

	var l2 = new(linkedList[int])
	l2.PushBack(1)
	checkLinkedList(t, l2, []any{1})

	var l3 = new(linkedList[int])
	l3.PushFrontList(l1)
	checkLinkedList(t, l3, []any{1})

	var l4 = new(linkedList[int])
	l4.PushBackList(l2)
	checkLinkedList(t, l4, []any{1})
}

func TestInsertBeforeUnknownMarkEle(t *testing.T) {
	var l linkedList[int]
	l.PushBack(1)
	l.PushBack(2)
	l.PushBack(3)
	l.InsertBefore(1, new(element[int]))
	checkLinkedList(t, &l, []any{1, 2, 3})
}

func TestInsertAfterUnknownMarkEle(t *testing.T) {
	var l linkedList[int]
	l.PushBack(1)
	l.PushBack(2)
	l.PushBack(3)
	l.InsertAfter(1, new(element[int]))
	checkLinkedList(t, &l, []any{1, 2, 3})
}

func TestMoveUnknownMarkEle(t *testing.T) {
	var l1 linkedList[int]
	e1 := l1.PushBack(1)

	var l2 linkedList[int]
	e2 := l2.PushBack(2)

	l1.MoveAfter(e1, e2)
	checkLinkedList(t, &l1, []any{1})
	checkLinkedList(t, &l2, []any{2})

	l1.MoveBefore(e1, e2)
	checkLinkedList(t, &l1, []any{1})
	checkLinkedList(t, &l2, []any{2})
}
