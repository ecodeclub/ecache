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

package ecache

import (
	"context"
	"time"
)

type NamespaceCache struct {
	C         Cache
	Namespace string
}

func (c *NamespaceCache) Set(ctx context.Context, key string, val any, expiration time.Duration) error {
	return c.C.Set(ctx, c.Namespace+key, val, expiration)
}

func (c *NamespaceCache) SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error) {
	return c.C.SetNX(ctx, c.Namespace+key, val, expiration)
}

func (c *NamespaceCache) GetSet(ctx context.Context, key string, val string) Value {
	return c.C.GetSet(ctx, c.Namespace+key, val)
}

func (c *NamespaceCache) Delete(ctx context.Context, key ...string) (int64, error) {
	if len(key) == 1 {
		return c.C.Delete(ctx, c.Namespace+key[0])
	}
	newkey := make([]string, len(key))
	for i, v := range key {
		newkey[i] = c.Namespace + v
	}
	return c.C.Delete(ctx, newkey...)
}

func (c *NamespaceCache) LPush(ctx context.Context, key string, val ...any) (int64, error) {
	return c.C.LPush(ctx, c.Namespace+key, val...)
}

func (c *NamespaceCache) LPop(ctx context.Context, key string) Value {
	return c.C.LPop(ctx, c.Namespace+key)
}

func (c *NamespaceCache) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	return c.C.SAdd(ctx, c.Namespace+key, members...)
}

func (c *NamespaceCache) SRem(ctx context.Context, key string, members ...any) (int64, error) {
	return c.C.SRem(ctx, c.Namespace+key, members...)
}

func (c *NamespaceCache) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.C.IncrBy(ctx, c.Namespace+key, value)
}

func (c *NamespaceCache) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.C.DecrBy(ctx, c.Namespace+key, value)
}

func (c *NamespaceCache) IncrByFloat(ctx context.Context, key string, value float64) (float64, error) {
	return c.C.IncrByFloat(ctx, c.Namespace+key, value)
}

func (c *NamespaceCache) Get(ctx context.Context, key string) Value {
	return c.C.Get(ctx, c.Namespace+key)
}
