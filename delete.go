package xorstore

import (
	"context"
	"errors"
	"fmt"
)

// Delete conditionally deletes key, exactly like a conditional generation
// update: it succeeds only when the current generation equals expectGen.
//
// Deletion is published as a tombstone manifest at generation
// expectGen+1 ({"deleted": true}), atomically staged and renamed through
// the same publish path as ordinary writes. The tombstone is the new
// highest generation, so:
//
//   - Get/Repair return ErrNotFound as soon as the first copy lands and
//     stay unreadable across restarts even while stale manifest copies or
//     shard files linger;
//   - a same-name Put must use the generation returned here as its
//     expectation and is then published at a strictly higher generation,
//     never reusing the deleted generation;
//   - an in-flight repair of the old generation is rejected by its
//     re-read guard (generation mismatch) instead of writing old shards
//     back.
//
// The deleted generation's shard files are collected best-effort after
// the tombstone is live; anything left (a failed unlink, an absent disk)
// is reclaimed by restart recovery, which never removes shards a live
// manifest still references.
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
	if cur.Deleted {
		// Already deleted: there is no live generation to delete again.
		return cur.Gen, ErrNotFound
	}
	if cur.Gen != expectGen {
		return cur.Gen, fmt.Errorf("delete %q expected gen %d, current %d: %w",
			key, expectGen, cur.Gen, ErrConflict)
	}
	newGen := cur.Gen + 1
	id := keyID(key)

	tomb := &Manifest{
		Deleted: true,
		Schema:  1,
		Key:     key,
		Gen:     newGen,
	}

	// Fault point: staged on every disk but nothing renamed yet, so the
	// live generation is unchanged and remains readable until publication.
	if s.hooks.BeforeDeleteCommit != nil {
		if err := s.hooks.BeforeDeleteCommit(key, newGen); err != nil {
			return 0, err
		}
	}

	published, err := s.publishManifest(ctx, tomb)
	if err != nil && published == 0 {
		return 0, err // deletion is not live; the object stays readable
	}
	// If some tombstone copies failed to land, the first one already
	// makes the deletion authoritative; the missing copies heal later.

	// Collect the deleted generation's shards best-effort; the tombstone
	// already makes the object unreadable and recovery reclaims leftovers.
	for d := 0; d < NumShards; d++ {
		if !diskExists(s.dirs[d]) {
			continue
		}
		_, _ = s.gcDiskShards(s.dirs[d], id, tomb)
	}
	return newGen, nil
}
