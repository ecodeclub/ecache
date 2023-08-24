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
