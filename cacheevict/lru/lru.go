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
	"sync"
	"time"
)

const (
	defaultCapacity = 100
)

var defaultExpiresAt = time.Time{}

type Entry[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time
}

func (e Entry[K, V]) isExpired() bool {
	return e.expiresAt.Before(time.Now())
}

func (e Entry[K, V]) existExpiration() bool {
	return !e.expiresAt.Equal(defaultExpiresAt)
}

type EvictCallback[K comparable, V any] func(key K, value V)

type Option[K comparable, V any] func(l *LRU[K, V])

func WithCallback[K comparable, V any](callback func(k K, v V)) Option[K, V] {
	return func(l *LRU[K, V]) {
		l.callback = callback
	}
}

func WithCapacity[K comparable, V any](capacity int) Option[K, V] {
	return func(l *LRU[K, V]) {
		l.capacity = capacity
	}
}

type LRU[K comparable, V any] struct {
	lock     sync.RWMutex
	capacity int
	list     *LinkedList[Entry[K, V]]
	data     map[K]*Element[Entry[K, V]]
	callback EvictCallback[K, V]
}

func NewLRU[K comparable, V any](options ...Option[K, V]) *LRU[K, V] {
	res := &LRU[K, V]{
		list:     NewLinkedList[Entry[K, V]](),
		data:     make(map[K]*Element[Entry[K, V]], 16),
		capacity: defaultCapacity,
	}
	for _, opt := range options {
		opt(res)
	}
	return res
}

func (l *LRU[K, V]) Purge() {
	l.lock.Lock()
	defer l.lock.Unlock()
	for k, v := range l.data {
		if l.callback != nil {
			l.callback(v.Value.key, v.Value.value)
		}
		l.delete(k)
	}
	l.list.Init()
}

func (l *LRU[K, V]) pushEntry(key K, ent Entry[K, V]) (evicted bool) {
	if elem, ok := l.data[key]; ok {
		elem.Value = ent
		l.list.MoveToFront(elem)
		return false
	}
	elem := l.list.PushFront(ent)
	l.data[key] = elem
	evict := l.len() > l.capacity
	if evict {
		l.removeOldest()
	}
	return evict
}

func (l *LRU[K, V]) addTTL(key K, value V, expiration time.Duration) (evicted bool) {
	ent := Entry[K, V]{key: key, value: value,
		expiresAt: time.Now().Add(expiration)}
	return l.pushEntry(key, ent)
}

func (l *LRU[K, V]) add(key K, value V) (evicted bool) {
	ent := Entry[K, V]{key: key, value: value,
		expiresAt: defaultExpiresAt}
	return l.pushEntry(key, ent)
}

func (l *LRU[K, V]) AddTTL(key K, value V, expiration time.Duration) (evicted bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.addTTL(key, value, expiration)
}

func (l *LRU[K, V]) Add(key K, value V) (evicted bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.add(key, value)
}

func (l *LRU[K, V]) Get(key K) (value V, ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if elem, exist := l.data[key]; exist {
		entry := elem.Value
		if entry.existExpiration() && entry.isExpired() {
			l.removeElement(elem)
			return
		}
		l.list.MoveToFront(elem)
		return entry.value, true
	}
	return
}

func (l *LRU[K, V]) peek(key K) (value V, ok bool) {
	if elem, exist := l.data[key]; exist {
		entry := elem.Value
		if entry.existExpiration() && entry.isExpired() {
			l.removeElement(elem)
			return
		}
		return entry.value, true
	}
	return
}

func (l *LRU[K, V]) Peek(key K) (value V, ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.peek(key)
}

func (l *LRU[K, V]) GetOldest() (key K, value V, ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	elem := l.list.Back()
	for elem != nil {
		entry := elem.Value
		if !entry.existExpiration() || !entry.isExpired() {
			return entry.key, entry.value, true
		}
		l.removeElement(elem)
		elem = l.list.Back()
	}
	return
}

func (l *LRU[K, V]) RemoveOldest() (key K, value V, ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if elem := l.list.Back(); elem != nil {
		l.removeElement(elem)
		return elem.Value.key, elem.Value.value, true
	}
	return
}

func (l *LRU[K, V]) removeOldest() {
	if ent := l.list.Back(); ent != nil {
		l.removeElement(ent)
	}
}

func (l *LRU[K, V]) removeElement(elem *Element[Entry[K, V]]) {
	l.list.Remove(elem)
	entry := elem.Value
	l.delete(entry.key)
	if l.callback != nil {
		l.callback(entry.key, entry.value)
	}
}

func (l *LRU[K, V]) Remove(key K) (present bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if elem, ok := l.data[key]; ok {
		l.removeElement(elem)
		if elem.Value.existExpiration() && elem.Value.isExpired() {
			return false
		}
		return true
	}
	return false
}

func (l *LRU[K, V]) Resize(size int) (evicted int) {
	l.lock.Lock()
	defer l.lock.Unlock()
	diff := l.len() - size
	if diff < 0 {
		diff = 0
	}
	for i := 0; i < diff; i++ {
		l.removeOldest()
	}
	l.capacity = size
	return diff
}

func (l *LRU[K, V]) contains(key K) (ok bool) {
	elem, ok := l.data[key]
	if ok {
		if elem.Value.existExpiration() && elem.Value.isExpired() {
			l.removeElement(elem)
			return false
		}
	}
	return ok
}

func (l *LRU[K, V]) Contains(key K) (ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.contains(key)
}

func (l *LRU[K, V]) delete(key K) {
	delete(l.data, key)
}

func (l *LRU[K, V]) len() int {
	var length int
	for elem := l.list.Back(); elem != nil; elem = elem.Prev() {
		if elem.Value.existExpiration() && elem.Value.isExpired() {
			l.removeElement(elem)
			continue
		}
		length++
	}
	return length
}

func (l *LRU[K, V]) Len() int {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.len()
}

func (l *LRU[K, V]) Keys() []K {
	l.lock.Lock()
	defer l.lock.Unlock()
	keys := make([]K, l.list.Len())
	i := 0
	for elem := l.list.Back(); elem != nil; elem = elem.Prev() {
		if elem.Value.existExpiration() && elem.Value.isExpired() {
			l.removeElement(elem)
			continue
		}
		keys[i] = elem.Value.key
		i++
	}
	return keys
}

func (l *LRU[K, V]) Values() []V {
	l.lock.Lock()
	defer l.lock.Unlock()
	values := make([]V, l.list.Len())
	i := 0
	for elem := l.list.Back(); elem != nil; elem = elem.Prev() {
		if elem.Value.existExpiration() && elem.Value.isExpired() {
			l.removeElement(elem)
			continue
		}
		values[i] = elem.Value.value
		i++
	}
	return values
}
