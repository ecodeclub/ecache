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
	l := newLinkedList[int]()
	e4 := l.pushBack(4)
	e1 := l.pushFront(1)
	l.insertBefore(3, e4)
	l.insertAfter(2, e1)
	for e, i := l.front(), 0; i < l.capacity; i++ {
		fmt.Println(e.Value)
		e = e.next
	}

	// Output:
	// 1
	// 2
	// 3
	// 4
}

func checkLinkedListLen[T any](t *testing.T, l *linkedList[T], len int) bool {
	if n := l.len(); n != len {
		t.Errorf("l.len() = %d, want %d", n, len)
		return false
	}
	return true
}

func checkLinkedListPointers[T any](t *testing.T, l *linkedList[T], es []*element[T]) {
	root := l.head

	if !checkLinkedListLen[T](t, l, len(es)) {
		return
	}

	if len(es) == 0 {
		if l.head.next != l.tail && l.head.next != root || l.tail.prev != root {
			t.Errorf("l.head.next = %p, l.tail.prev = %p; both should both be nil or %p", l.head.next, l.tail.prev, root)
		}
		return
	}

	for i, e := range es {
		prev := root
		Prev := (*element[T])(nil)
		if i > 0 {
			prev = es[i-1]
			Prev = prev
		}
		if p := e.prev; p != root && p != prev {
			t.Errorf("elt[%d](%p).prev = %p, want %p", i, e, p, prev)
		}
		if p := e.prev; p != root && p != Prev {
			t.Errorf("elt[%d](%p).prev = %p, want %p", i, e, p, Prev)
		}

		next := root
		Next := (*element[T])(nil)
		if i < len(es)-1 {
			next = es[i+1]
			Next = next
		}
		if n := e.next; n != l.tail && n != next {
			t.Errorf("elt[%d](%p).next = %p, want %p", i, e, n, next)
		}
		if n := e.next; n != l.tail && n != Next {
			t.Errorf("elt[%d](%p).next = %p, want %p", i, e, n, Next)
		}
	}
}

func TestLinkedList(t *testing.T) {
	l := newLinkedList[any]()
	checkLinkedListPointers(t, l, []*element[any]{})
	e := l.pushFront("a")
	checkLinkedListPointers(t, l, []*element[any]{e})
	l.moveToFront(e)
	checkLinkedListPointers(t, l, []*element[any]{e})
	l.moveToBack(e)
	checkLinkedListPointers(t, l, []*element[any]{e})
	l.removeElem(e)
	checkLinkedListPointers(t, l, []*element[any]{})

	e2 := l.pushFront(2)
	e1 := l.pushFront(1)
	e3 := l.pushBack(3)
	e4 := l.pushBack("banana")
	checkLinkedListPointers(t, l, []*element[any]{e1, e2, e3, e4})

	l.removeElem(e2)
	checkLinkedListPointers(t, l, []*element[any]{e1, e3, e4})

	l.moveToFront(e3)
	checkLinkedListPointers(t, l, []*element[any]{e3, e1, e4})

	l.moveToFront(e1)
	l.moveToBack(e3)
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e3})

	l.moveToFront(e3)
	checkLinkedListPointers(t, l, []*element[any]{e3, e1, e4})
	l.moveToFront(e3)
	checkLinkedListPointers(t, l, []*element[any]{e3, e1, e4})

	l.moveToBack(e3)
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e3})
	l.moveToBack(e3)
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e3})

	e2 = l.insertBefore(2, e1)
	checkLinkedListPointers(t, l, []*element[any]{e2, e1, e4, e3})
	l.removeElem(e2)
	e2 = l.insertBefore(2, e4)
	checkLinkedListPointers(t, l, []*element[any]{e1, e2, e4, e3})
	l.removeElem(e2)
	e2 = l.insertBefore(2, e3)
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e2, e3})
	l.removeElem(e2)

	e2 = l.insertAfter(2, e1)
	checkLinkedListPointers(t, l, []*element[any]{e1, e2, e4, e3})
	l.removeElem(e2)
	e2 = l.insertAfter(2, e4)
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e2, e3})
	l.removeElem(e2)
	e2 = l.insertAfter(2, e3)
	checkLinkedListPointers(t, l, []*element[any]{e1, e4, e3, e2})
	l.removeElem(e2)

	sum := 0
	for e, i := l.front(), 0; i < l.capacity; i++ {
		if i, ok := e.Value.(int); ok {
			sum += i
		}
		e = e.next
	}
	if sum != 4 {
		t.Errorf("sum over l = %d, want 4", sum)
	}

	//var next *element[any]
	capacity := l.capacity
	for e, i := l.front(), 0; i < capacity; i++ {
		next := e.next
		l.removeElem(e)
		e = next
	}
	checkLinkedListPointers(t, l, []*element[any]{})
}

func checkLinkedList[T int](t *testing.T, l *linkedList[T], es []any) {
	if !checkLinkedListLen[T](t, l, len(es)) {
		return
	}

	i := 0
	for e := l.front(); i < l.capacity; i++ {
		if e != l.tail {
			le := e.Value
			if le != es[i] {
				t.Errorf("elt[%d].Value = %v, want %v", i, le, es[i])
			}
			e = e.next
		}
	}
}

func TestExtendingEle(t *testing.T) {
	l1 := newLinkedList[int]()
	l2 := newLinkedList[int]()

	l1.pushBack(1)
	l1.pushBack(2)
	l1.pushBack(3)

	l2.pushBack(4)
	l2.pushBack(5)

	l3 := newLinkedList[int]()
	l3.pushBackList(l1)
	checkLinkedList(t, l3, []any{1, 2, 3})
	l3.pushBackList(l2)
	checkLinkedList(t, l3, []any{1, 2, 3, 4, 5})

	l3 = newLinkedList[int]()
	l3.pushFrontList(l2)
	checkLinkedList(t, l3, []any{4, 5})
	l3.pushFrontList(l1)
	checkLinkedList(t, l3, []any{1, 2, 3, 4, 5})

	checkLinkedList(t, l1, []any{1, 2, 3})
	checkLinkedList(t, l2, []any{4, 5})

	l3 = newLinkedList[int]()
	l3.pushBackList(l1)
	checkLinkedList(t, l3, []any{1, 2, 3})
	l3.pushBackList(l3)
	checkLinkedList(t, l3, []any{1, 2, 3, 1, 2, 3})

	l3 = newLinkedList[int]()
	l3.pushFrontList(l1)
	checkLinkedList(t, l3, []any{1, 2, 3})
	l3.pushFrontList(l3)
	checkLinkedList(t, l3, []any{1, 2, 3, 1, 2, 3})

	l3 = newLinkedList[int]()
	l1.pushBackList(l3)
	checkLinkedList(t, l1, []any{1, 2, 3})
	l1.pushFrontList(l3)
	checkLinkedList(t, l1, []any{1, 2, 3})
}

func TestRemoveEle(t *testing.T) {
	l := newLinkedList[int]()
	e1 := l.pushBack(1)
	e2 := l.pushBack(2)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2})
	e := l.front()
	l.removeElem(e)
	checkLinkedListPointers(t, l, []*element[int]{e2})
	e = l.front()
	l.removeElem(e)
	checkLinkedListPointers(t, l, []*element[int]{})
}

func TestIssue6349Ele(t *testing.T) {
	l := newLinkedList[int]()
	l.pushBack(1)
	l.pushBack(2)

	e := l.front()
	l.removeElem(e)
	if e.Value != 1 {
		t.Errorf("e.value = %d, want 1", e.Value)
	}
	if e.next != nil && e.next != l.tail {
		t.Errorf("e.nextElem() != nil")
	}
	if e.prev != nil && e.prev != l.head {
		t.Errorf("e.prevElem() != nil")
	}
}

func TestMoveEle(t *testing.T) {
	l := newLinkedList[int]()
	e1 := l.pushBack(1)
	e2 := l.pushBack(2)
	e3 := l.pushBack(3)
	e4 := l.pushBack(4)

	l.moveAfter(e3, e3)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2, e3, e4})
	l.moveBefore(e2, e2)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2, e3, e4})

	l.moveAfter(e3, e2)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2, e3, e4})
	l.moveBefore(e2, e3)
	checkLinkedListPointers(t, l, []*element[int]{e1, e2, e3, e4})

	l.moveBefore(e2, e4)
	checkLinkedListPointers(t, l, []*element[int]{e1, e3, e2, e4})
	e2, e3 = e3, e2

	l.moveBefore(e4, e1)
	checkLinkedListPointers(t, l, []*element[int]{e4, e1, e2, e3})
	e1, e2, e3, e4 = e4, e1, e2, e3

	l.moveAfter(e4, e1)
	checkLinkedListPointers(t, l, []*element[int]{e1, e4, e2, e3})
	e2, e3, e4 = e4, e2, e3

	l.moveAfter(e2, e3)
	checkLinkedListPointers(t, l, []*element[int]{e1, e3, e2, e4})
}
