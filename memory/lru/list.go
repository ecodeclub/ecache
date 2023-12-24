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

func (e *element[T]) Next() *element[T] {
	if n := e.next; e.list != nil && n != &e.list.root {
		return n
	}
	return nil
}

func (e *element[T]) Prev() *element[T] {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

type linkedList[T any] struct {
	root element[T]
	len  int
}

func newLinkedList[T any]() *linkedList[T] {
	l := &linkedList[T]{}
	return l.Init()
}

func (l *linkedList[T]) Init() *linkedList[T] {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	return l
}

func (l *linkedList[T]) Len() int {
	return l.len
}

func (l *linkedList[T]) Front() *element[T] {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

func (l *linkedList[T]) Back() *element[T] {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

func (l *linkedList[T]) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}

func (l *linkedList[T]) insert(e, at *element[T]) *element[T] {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.len++
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
	l.len--
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

func (l *linkedList[T]) Remove(e *element[T]) any {
	if e.list == l {
		l.remove(e)
	}
	return e.Value
}

func (l *linkedList[T]) PushFront(v T) *element[T] {
	l.lazyInit()
	return l.insertValue(v, &l.root)
}

func (l *linkedList[T]) PushBack(v T) *element[T] {
	l.lazyInit()
	return l.insertValue(v, l.root.prev)
}

func (l *linkedList[T]) MoveToFront(e *element[T]) {
	if e.list != l || l.root.next == e {
		return
	}
	l.move(e, &l.root)
}

func (l *linkedList[T]) MoveToBack(e *element[T]) {
	if e.list != l || l.root.prev == e {
		return
	}
	l.move(e, l.root.prev)
}

func (l *linkedList[T]) MoveBefore(e, mark *element[T]) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark.prev)
}

func (l *linkedList[T]) MoveAfter(e, mark *element[T]) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark)
}

func (l *linkedList[T]) InsertBefore(v T, mark *element[T]) *element[T] {
	if mark.list != l {
		return nil
	}
	return l.insertValue(v, mark.prev)
}

func (l *linkedList[T]) InsertAfter(v T, mark *element[T]) *element[T] {
	if mark.list != l {
		return nil
	}
	return l.insertValue(v, mark)
}

func (l *linkedList[T]) PushBackList(other *linkedList[T]) {
	l.lazyInit()
	e := other.Front()
	for i := other.Len(); i > 0; i-- {
		l.insertValue(e.Value, l.root.prev)
		e = e.Next()
	}
}

func (l *linkedList[T]) PushFrontList(other *linkedList[T]) {
	l.lazyInit()
	for i, e := other.Len(), other.Back(); i > 0; i, e = i-1, e.Prev() {
		l.insertValue(e.Value, &l.root)
	}
}
