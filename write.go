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
	committed := 0
	for i := 0; i < NumShards; i++ {
		final[i] = shardPath(s.dirs[i], id, newGen, roles[i])
		if err := commitStaged(staged[i], final[i]); err != nil {
			// Roll back the shards that were already published so the
			// new generation cannot half-appear; the old generation is
			// untouched (shards are gen-scoped).
			for j := 0; j < committed; j++ {
				removeBestEffort(final[j])
			}
			removeBestEffort(staged[i+1:]...)
			return 0, fmt.Errorf("publish shard %s: %w", roles[i], err)
		}
		committed++
	}

	// 4. Build and publish the new manifest. First stage all copies so
	//    a failure before renaming cannot corrupt any existing manifest;
	//    then rename one by one. After the first rename the new
	//    generation is readable and the remaining renames are retried
	//    via manifest healing on future operations.
	man := s.buildManifest(key, newGen, len(data), raw[:], roles[:])
	manRaw, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return 0, err
	}
	manStaged := [NumShards]string{}
	for d := 0; d < NumShards; d++ {
		path, err := stageFile(s.dirs[d], id, "manifest", manRaw)
		if err != nil {
			s.abortStaged(manStaged[:])
			return 0, fmt.Errorf("stage manifest: %w", err)
		}
		manStaged[d] = path
	}
	published := 0
	var publishErr error
	for d := 0; d < NumShards; d++ {
		if err := commitStaged(manStaged[d], manifestPath(s.dirs[d], id)); err != nil {
			publishErr = err
			break
		}
		published++
	}
	if publishErr != nil {
		s.abortStaged(manStaged[published:])
		if published == 0 {
			// No manifest made it: the new shards are unreferenced.
			// Leave them for recovery GC rather than risking a partial
			// rollback under error; nothing references them yet.
			return 0, fmt.Errorf("publish manifest: %w", publishErr)
		}
		// At least one copy is published, so the generation is live.
		// Try to heal the remaining copies before reporting.
		if _, _, err := s.loadBestManifest(ctx, id, true); err != nil {
			return newGen, fmt.Errorf("manifest partially published (%d/%d disks), heal failed: %w",
				published, NumShards, err)
		}
	}
	return newGen, nil
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
