package xorstore

import (
	"context"
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
	if cur == nil {
		// No manifest names a current generation. If published shard
		// files of this key still exist, every manifest copy of a prior
		// incarnation has been lost (e.g. delete evidence vanished).
		// Treating this as a first write would publish gen 1 again, so
		// new content could overwrite or be confused with the old
		// incarnation. Restart recovery GCs the orphans; until then the
		// recreation must be failed rather than silently reuse the gen.
		if s.anyPublishedShard(keyID(key)) {
			return 0, fmt.Errorf("put %q: %w", key, ErrGenerationAmbiguous)
		}
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

	// 4. Publish the new manifest to every disk. publishManifest stages
	//    all copies before renaming any of them, so a failure before the
	//    first rename cannot alter an existing manifest; after the first
	//    rename the new generation is readable and remaining copies are
	//    retried via manifest healing on future operations.
	man := s.buildManifest(key, newGen, len(data), raw[:], roles[:])
	present, perr := s.publishManifest(ctx, id, man, nil)
	if perr != nil {
		if present > 0 {
			// The generation is live on some disk; report success of the
			// data but surface the partial durability.
			return newGen, perr
		}
		// No manifest made it: the new shards are unreferenced. Leave
		// them for recovery GC rather than risking a partial rollback
		// under error; nothing references them yet.
		return 0, perr
	}
	if present < NumShards {
		// Manifest is live but not every disk was present; the gen-scoped
		// shards stay (recovery heals when the disk returns).
		return newGen, nil
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
