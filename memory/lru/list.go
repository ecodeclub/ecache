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

type element[T any] struct {
	Value      T
	list       *linkedList[T]
	next, prev *element[T]
}

func (e *element[T]) nextElem() *element[T] {
	if n := e.next; e.list != nil && n != &e.list.root {
		return n
	}
	return nil
}

func (e *element[T]) prevElem() *element[T] {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

type linkedList[T any] struct {
	root     element[T]
	capacity int
}

func newLinkedList[T any]() *linkedList[T] {
	l := &linkedList[T]{}
	return l.init()
}

func (l *linkedList[T]) init() *linkedList[T] {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.capacity = 0
	return l
}

func (l *linkedList[T]) len() int {
	return l.capacity
}

func (l *linkedList[T]) front() *element[T] {
	if l.capacity == 0 {
		return nil
	}
	return l.root.next
}

func (l *linkedList[T]) back() *element[T] {
	if l.capacity == 0 {
		return nil
	}
	return l.root.prev
}

func (l *linkedList[T]) lazyInit() {
	if l.root.next == nil {
		l.init()
	}
}

func (l *linkedList[T]) insert(e, at *element[T]) *element[T] {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.capacity++
	return e
}

func (l *linkedList[T]) insertValue(v T, at *element[T]) *element[T] {
	return l.insert(&element[T]{Value: v}, at)
}

func (l *linkedList[T]) remove(e *element[T]) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil
	e.prev = nil
	e.list = nil
	l.capacity--
}

func (l *linkedList[T]) move(e, at *element[T]) {
	if e == at {
		return
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
}

func (l *linkedList[T]) removeElem(e *element[T]) any {
	if e.list == l {
		l.remove(e)
	}
	return e.Value
}

func (l *linkedList[T]) pushFront(v T) *element[T] {
	l.lazyInit()
	return l.insertValue(v, &l.root)
}

func (l *linkedList[T]) pushBack(v T) *element[T] {
	l.lazyInit()
	return l.insertValue(v, l.root.prev)
}

func (l *linkedList[T]) moveToFront(e *element[T]) {
	if e.list != l || l.root.next == e {
		return
	}
	l.move(e, &l.root)
}

func (l *linkedList[T]) moveToBack(e *element[T]) {
	if e.list != l || l.root.prev == e {
		return
	}
	l.move(e, l.root.prev)
}

func (l *linkedList[T]) moveBefore(e, mark *element[T]) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark.prev)
}

func (l *linkedList[T]) moveAfter(e, mark *element[T]) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark)
}

func (l *linkedList[T]) insertBefore(v T, mark *element[T]) *element[T] {
	if mark.list != l {
		return nil
	}
	return l.insertValue(v, mark.prev)
}

func (l *linkedList[T]) insertAfter(v T, mark *element[T]) *element[T] {
	if mark.list != l {
		return nil
	}
	return l.insertValue(v, mark)
}

func (l *linkedList[T]) pushBackList(other *linkedList[T]) {
	l.lazyInit()
	e := other.front()
	for i := other.len(); i > 0; i-- {
		l.insertValue(e.Value, l.root.prev)
		e = e.nextElem()
	}
}

func (l *linkedList[T]) pushFrontList(other *linkedList[T]) {
	l.lazyInit()
	for i, e := other.len(), other.back(); i > 0; i, e = i-1, e.prevElem() {
		l.insertValue(e.Value, &l.root)
	}
}
