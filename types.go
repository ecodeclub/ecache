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
	// Get 返回一个 Value
	// 如果你需要检测 Err，可以使用 Value.Err
	// 如果你需要知道 Key 是否存在，可以使用 Value.KeyNotFound
	Get(ctx context.Context, key string) Value
}

// Value 代表一个从缓存中读取出来的值
type Value struct {
	ekit.AnyValue
}

func (v Value) KeyNotFound() bool {
	return errors.Is(v.Err, errs.ErrKeyNotExist)
}
