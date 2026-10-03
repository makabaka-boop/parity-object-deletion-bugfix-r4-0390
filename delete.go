package xorstore

import (
	"context"
	"errors"
	"fmt"
)

// Delete performs a conditional generation delete of key: it succeeds
// only when the current live generation equals expectGen, and returns the
// delete generation (expectGen+1).
//
// Deletion is published as a tombstone manifest, never by removing the
// manifest copies:
//
//   - Get/Repair on the key return ErrNotFound while the tombstone stands,
//     and a manifest copy that was momentarily missing or stale can no
//     longer resurrect the old content (it re-heals to the tombstone).
//   - The same-name key can only be recreated by a conditional Put with
//     expectGen set to the returned delete generation, so the new content
//     gets a strictly higher generation and can never share or reuse an
//     old generation.
//   - Old-generation repairs in flight are rejected by the repair
//     generation guard, and restart GC removes only shard generations
//     below the tombstone.
//
// After the tombstone is committed, shards of the deleted generation are
// removed best-effort; anything left is reclaimed by the next restart
// recovery. A failure before the first tombstone rename changes nothing
// readable (the object stays live). Deleting a deleted key returns
// ErrNotFound.
func (s *Store) Delete(ctx context.Context, key string, expectGen uint64) (uint64, error) {
	if key == "" {
		return 0, errors.New("xorstore: empty key")
	}
	lock := s.lockKey(key)
	lock.Lock()
	defer lock.Unlock()

	cur, err := s.currentGen(ctx, key)
	if err != nil {
		return 0, err
	}
	if cur == nil {
		// No readable manifest. Refuse to fabricate a tombstone if shard
		// evidence of an older incarnation lingers: deleting on guessed
		// generations would make a later recreation indistinguishable from
		// that incarnation. Recovery reclaims the orphans first.
		if s.anyPublishedShard(keyID(key)) {
			return 0, fmt.Errorf("delete %q: %w", key, ErrGenerationAmbiguous)
		}
		return 0, ErrNotFound
	}
	if cur.Deleted {
		return cur.Gen, fmt.Errorf("delete %q already deleted at gen %d: %w",
			key, cur.Gen, ErrNotFound)
	}
	if cur.Gen != expectGen {
		return cur.Gen, fmt.Errorf("delete %q expected gen %d, current %d: %w",
			key, expectGen, cur.Gen, ErrConflict)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	deleteGen := cur.Gen + 1
	id := keyID(key)
	tomb := buildTombstone(key, deleteGen)

	// All tombstone copies are staged before the hook fires; an error
	// there aborts them with no rename, so a failed delete leaves the
	// object fully live and creates no readable half-state.
	if _, err := s.publishManifest(ctx, id, tomb, func() error {
		if s.hooks.BeforeDeleteCommit == nil {
			return nil
		}
		return s.hooks.BeforeDeleteCommit(key, deleteGen)
	}); err != nil {
		return 0, err
	}

	// The object is now unreadable. Remove the deleted generation's shard
	// files best-effort (and any older leftovers). Removal failure cannot
	// un-delete anything: the tombstone governs reads, and restart GC
	// reclaims whatever remains.
	for d := 0; d < NumShards; d++ {
		if _, err := s.gcDiskShards(s.dirs[d], id, tomb); err != nil {
			break
		}
	}
	return deleteGen, nil
}
