package xorstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Put performs a conditional generation update of key.
//
// expectGen is the generation the caller based its update on: pass 0 to
// create the object, or the generation returned by Get for an update.
// The write is serialized per key; if a newer generation has been
// published in the meantime Put returns ErrConflict.
//
// Publication order is: stage all three shards as unreferenced temp
// files, verify them, (fault point), rename them into generation-scoped
// names, then atomically publish the new manifest to every disk. A crash
// before the manifest leaves only unreferenced files that restart
// recovery deletes; the previous published version stays readable.
// It returns the new generation.
func (s *Store) Put(ctx context.Context, key string, data []byte, expectGen uint64) (uint64, error) {
	if key == "" {
		return 0, errors.New("xorstore: empty key")
	}
	kl := s.lockKey(key)
	kl.Lock()
	defer kl.Unlock()

	cur, err := s.currentGen(ctx, key)
	if err != nil {
		return 0, err
	}
	curGen := uint64(0)
	if cur != nil {
		curGen = cur.Gen
	}
	if expectGen != curGen {
		return curGen, fmt.Errorf("put %q expected gen %d, current %d: %w",
			key, expectGen, curGen, ErrConflict)
	}
	newGen := curGen + 1
	id := keyID(key)

	// 1. Encode and stage every shard as an unpublished temp file.
	a, b, p := splitShards(data)
	raw := [NumShards][]byte{a, b, p}
	roles := [NumShards]string{"a", "b", "p"}

	staged := [NumShards]string{}
	for i := 0; i < NumShards; i++ {
		if !diskExists(s.dirs[i]) {
			continue // disk offline: its copy is rebuilt when it returns
		}
		path, err := stageFile(s.dirs[i], id, fmt.Sprintf("gen%d-%s", newGen, roles[i]), raw[i])
		if err != nil {
			s.abortStaged(staged[:])
			return 0, fmt.Errorf("stage shard %s: %w", roles[i], err)
		}
		staged[i] = path
	}

	// 2. Read every staged shard back and verify it against the bytes
	//    we intended to persist.
	for i := 0; i < NumShards; i++ {
		if staged[i] == "" {
			continue // offline disk: nothing staged to verify
		}
		got, err := readStaged(staged[i])
		if err != nil {
			s.abortStaged(staged[:])
			return 0, fmt.Errorf("verify shard %s: %w", roles[i], err)
		}
		if digestBytes(got) != digestBytes(raw[i]) || len(got) != len(raw[i]) {
			s.abortStaged(staged[:])
			return 0, fmt.Errorf("verify shard %s: digest mismatch", roles[i])
		}
	}

	// Fault point: a "crash" here leaves staged temp files but nothing
	// published. Recovery sweeps them on restart. A plain hook error
	// aborts cleanly; a simulated crash keeps the on-disk half-state.
	if s.hooks.BeforeManifestCommit != nil {
		if err := s.hooks.BeforeManifestCommit(key, newGen); err != nil {
			if !errors.Is(err, ErrSimulatedCrash) {
				s.abortStaged(staged[:])
			}
			return 0, err
		}
	}

	// 3. Publish the three shards under generation-scoped names.
	//    Nothing reads these until the manifest references gen N.
	final := [NumShards]string{}
	publishedPaths := []string{}
	for i := 0; i < NumShards; i++ {
		if staged[i] == "" {
			continue // disk was offline: no copy to publish
		}
		final[i] = shardPath(s.dirs[i], id, newGen, roles[i])
		if err := commitStaged(staged[i], final[i]); err != nil {
			// Roll back the shards that were already published so the
			// new generation cannot half-appear; the old generation is
			// untouched (shards are gen-scoped).
			removeBestEffort(publishedPaths...)
			for j := i + 1; j < NumShards; j++ {
				removeBestEffort(staged[j])
			}
			return 0, fmt.Errorf("publish shard %s: %w", roles[i], err)
		}
		publishedPaths = append(publishedPaths, final[i])
	}

	// 4. Publish the new manifest (all copies are staged first so a
	//    failure before renaming cannot corrupt any existing manifest).
	//    After the first rename the new generation is readable and the
	//    remaining copies are retried via manifest healing on future
	//    operations. If not even one rename lands, roll back the shards
	//    just published so the failed operation leaves no half-finished,
	//    unreferenced generation on disk.
	man := s.buildManifest(key, newGen, len(data), raw[:], roles[:])
	published, err := s.publishManifest(ctx, man)
	if err != nil {
		if published == 0 {
			for i := 0; i < NumShards; i++ {
				removeBestEffort(final[i])
			}
		}
		return 0, err
	}
	return newGen, nil
}

// publishManifest marshals man, stages a copy on every present disk and
// renames the copies into place one by one. Copies that fail to stage are
// skipped and healed after the first rename. It returns the number of
// copies that were renamed. A non-nil error means:
//
//   - no copy could be staged/renamed (published == 0): the generation is
//     not live and the caller must roll back its shards;
//   - some copies are live but healing the rest failed: the generation is
//     readable on at least one disk already.
func (s *Store) publishManifest(ctx context.Context, man *Manifest) (int, error) {
	id := keyID(man.Key)
	manRaw, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return 0, err
	}
	manStaged := [NumShards]string{}
	for d := 0; d < NumShards; d++ {
		if !diskExists(s.dirs[d]) {
			continue // disk removed: no copy to publish there
		}
		path, err := stageFile(s.dirs[d], id, "manifest", manRaw)
		if err != nil {
			continue // healed later once the generation is live
		}
		manStaged[d] = path
	}
	published := 0
	var publishErr error
	for d := 0; d < NumShards; d++ {
		if manStaged[d] == "" {
			continue
		}
		if err := commitStaged(manStaged[d], manifestPath(s.dirs[d], id)); err != nil {
			publishErr = err
			break
		}
		published++
	}
	s.abortStaged(manStaged[:])
	if published == 0 {
		return 0, fmt.Errorf("publish manifest: %w",
			firstErr(publishErr, errNoDiskAvailable))
	}
	if publishErr != nil {
		// At least one copy is published, so the generation is live.
		// Try to heal the remaining copies before reporting.
		if _, _, err := s.loadBestManifest(ctx, id, true); err != nil {
			return published, fmt.Errorf("manifest partially published, heal failed: %w", err)
		}
	}
	return published, nil
}

// errNoDiskAvailable marks that no present disk accepted a manifest
// staging file.
var errNoDiskAvailable = errors.New("xorstore: no disk available for manifest publish")

// firstErr returns err if non-nil, otherwise fallback.
func firstErr(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}

// abortStaged removes any non-empty staged paths.
func (s *Store) abortStaged(paths []string) {
	for _, p := range paths {
		if p != "" {
			removeBestEffort(p)
		}
	}
}

// buildManifest assembles the manifest for a new generation.
func (s *Store) buildManifest(key string, gen uint64, length int, shards [][]byte, roles []string) *Manifest {
	man := &Manifest{
		Schema: 1,
		Key:    key,
		Gen:    gen,
		Length: length,
	}
	for i := 0; i < NumShards; i++ {
		man.Shards[i] = ShardInfo{
			Role:   roles[i],
			Size:   len(shards[i]),
			Digest: digestBytes(shards[i]),
		}
	}
	return man
}
