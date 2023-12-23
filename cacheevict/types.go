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

package cacheevict

import (
	"time"

	"github.com/ecodeclub/ecache/cacheevict/lru"
)

var _ EvictStrategy[string, any] = (*lru.LRU[string, any])(nil)

type EvictStrategy[K comparable, V any] interface {
	AddTTL(key K, value V, expiration time.Duration) bool
	Add(key K, value V) bool
	Get(key K) (value V, ok bool)
	Contains(key K) (ok bool)
	Peek(key K) (value V, ok bool)
	Remove(key K) bool
	RemoveOldest() (K, V, bool)
	GetOldest() (K, V, bool)
	Keys() []K
	Values() []V
	Len() int
	Purge()
	Resize(int) int
}
