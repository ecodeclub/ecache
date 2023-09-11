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
	"errors"
	"time"

	"github.com/ecodeclub/ecache/internal/errs"
	"github.com/ecodeclub/ekit"
)

type Cache interface {
	// Set 设置一个键值对，并且设置过期时间
	Set(ctx context.Context, key string, val any, expiration time.Duration) error
	// SetNX 设置一个键值对如果key不存在则写入反之失败，并且设置过期时间
	SetNX(ctx context.Context, key string, val any, expiration time.Duration) (bool, error)
	// Get 返回一个 Value
	// 如果你需要检测 Err，可以使用 Value.Err
	// 如果你需要知道 Key 是否存在，可以使用 Value.KeyNotFound
	Get(ctx context.Context, key string) Value
	// GetSet 设置一个新的值返回老的值 如果key没有老的值仍然设置成功，但是返回 errs.ErrKeyNotExist
	GetSet(ctx context.Context, key string, val string) Value
	// LPush 将所有指定值插入存储在 的列表的头部key。
	// 如果key不存在，则在执行推送操作之前将其创建为空列表。当key保存的值不是列表时，将返回错误
	// 默认返回列表的数量
	LPush(ctx context.Context, key string, val ...any) (int64, error)
	// LPop 命令用于移除并返回列表的第一个元素。
	LPop(ctx context.Context, key string) Value
	// SAdd 命令将一个或多个成员元素加入到集合中，已经存在于集合的成员元素将被忽略。
	SAdd(ctx context.Context, key string, members ...any) (int64, error)
	// SRem 移除集合中的一个或多个成员元素，不存在的成员元素会被忽略。
	SRem(ctx context.Context, key string, members ...any) Value
	// IncrBy 设置一个key并自增 1 或者指定的值
	IncrBy(ctx context.Context, key string, value int64) (int64, error)
	// DecrBy 将 key 中储存的数字值减一
	DecrBy(ctx context.Context, key string, value int64) (int64, error)
	// IncrByFloat 为 key 中所储存的值加上指定的浮点数增量值。
	IncrByFloat(ctx context.Context, key string, value float64) (float64, error)
}

// Value 代表一个从缓存中读取出来的值
type Value struct {
	ekit.AnyValue
}

func (v Value) KeyNotFound() bool {
	return errors.Is(v.Err, errs.ErrKeyNotExist)
}
