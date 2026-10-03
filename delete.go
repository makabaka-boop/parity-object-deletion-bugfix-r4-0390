package xorstore

import (
	"context"
	"errors"
	"os"
)

func (s *Store) Delete(ctx context.Context, key string, expectGen uint64) (uint64, error) {
	if key == "" {
		return 0, errors.New("empty key")
	}
	lock := s.lockKey(key)
	lock.Lock()
	defer lock.Unlock()
	cur, err := s.currentGen(ctx, key)
	if err != nil {
		return 0, err
	}
	if cur == nil {
		return 0, ErrNotFound
	}
	if cur.Gen != expectGen {
		return cur.Gen, ErrConflict
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if s.hooks.BeforeDeleteCommit != nil {
		if err = s.hooks.BeforeDeleteCommit(key, cur.Gen+1); err != nil {
			return 0, err
		}
	}
	for _, dir := range s.dirs {
		_ = os.Remove(manifestPath(dir, keyID(key)))
	}
	return cur.Gen + 1, nil
}
