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
	next, prev *element[T]
}

type linkedList[T any] struct {
	head     *element[T]
	tail     *element[T]
	capacity int
}

func newLinkedList[T any]() *linkedList[T] {
	head := &element[T]{}
	tail := &element[T]{next: head, prev: head}
	head.next, head.prev = tail, tail
	return &linkedList[T]{
		head: head,
		tail: tail,
	}
}

func (l *linkedList[T]) len() int {
	return l.capacity
}

func (l *linkedList[T]) front() *element[T] {
	if l.capacity == 0 {
		return nil
	}
	return l.head.next
}

func (l *linkedList[T]) back() *element[T] {
	if l.capacity == 0 {
		return nil
	}
	return l.tail.prev
}

func (l *linkedList[T]) insert(e, at *element[T]) *element[T] {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
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
	l.remove(e)
	return e.Value
}

func (l *linkedList[T]) pushFront(v T) *element[T] {
	return l.insertValue(v, l.head)
}

func (l *linkedList[T]) pushBack(v T) *element[T] {
	return l.insertValue(v, l.tail.prev)
}

func (l *linkedList[T]) moveToFront(e *element[T]) {
	l.move(e, l.head)
}

func (l *linkedList[T]) moveToBack(e *element[T]) {
	l.move(e, l.tail.prev)
}

func (l *linkedList[T]) moveBefore(e, mark *element[T]) {
	l.move(e, mark.prev)
}

func (l *linkedList[T]) moveAfter(e, mark *element[T]) {
	if e == mark {
		return
	}
	l.move(e, mark)
}

func (l *linkedList[T]) insertBefore(v T, mark *element[T]) *element[T] {
	return l.insertValue(v, mark.prev)
}

func (l *linkedList[T]) insertAfter(v T, mark *element[T]) *element[T] {
	return l.insertValue(v, mark)
}

func (l *linkedList[T]) pushBackList(other *linkedList[T]) {
	e := other.front()
	for i := other.len(); i > 0; i-- {
		l.insertValue(e.Value, l.tail.prev)
		e = e.next
	}
}

func (l *linkedList[T]) pushFrontList(other *linkedList[T]) {
	for i, e := other.len(), other.back(); i > 0; i-- {
		l.insertValue(e.Value, l.head)
		e = e.prev
	}
}
