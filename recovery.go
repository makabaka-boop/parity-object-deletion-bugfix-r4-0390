package xorstore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// RecoveryStats reports what restart recovery did.
type RecoveryStats struct {
	// StagedRemoved is the number of unpublished temp files deleted.
	StagedRemoved int
	// OrphanShardsRemoved is the number of shard files not referenced
	// by any published manifest (half-finished writes) deleted.
	OrphanShardsRemoved int
	// ManifestCopiesHealed is the number of manifest copies repaired
	// from other disks (missing, older or garbled).
	ManifestCopiesHealed int
	// KeysScanned is the number of distinct objects seen.
	KeysScanned int
}

// recover performs restart-time recovery:
//
//  1. Remove every file in every disk's .stage directory: those files
//     were never published.
//  2. For every object key found in manifests, heal missing/stale/garbled
//     manifest copies and delete shard files not referenced by the best
//     published generation (half-finished writes of any generation).
//
// Shards referenced by a published manifest are always retained.
func (s *Store) recover(ctx context.Context) (*RecoveryStats, error) {
	stats := &RecoveryStats{}

	// 1. Sweep unpublished staged files.
	for _, d := range s.dirs {
		st := filepath.Join(d, stageDirName)
		entries, err := os.ReadDir(st)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return stats, err
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if err := os.Remove(filepath.Join(st, e.Name())); err == nil {
				stats.StagedRemoved++
			}
		}
		_ = syncDir(st)
	}

	// 2. Collect the set of object ids from all manifest directories.
	idSet := map[string]struct{}{}
	for _, d := range s.dirs {
		md := filepath.Join(d, metaDirName)
		entries, err := os.ReadDir(md)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return stats, err
		}
		for _, e := range entries {
			name := e.Name()
			if strings.HasSuffix(name, ".json") {
				idSet[strings.TrimSuffix(name, ".json")] = struct{}{}
			}
		}
	}
	stats.KeysScanned = len(idSet)

	// 3. Per key: load best manifest, heal copies, GC unreferenced shards.
	for id := range idSet {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		man, onBest, err := s.loadBestManifest(ctx, id, false)
		if err != nil {
			// No usable copy anywhere: leave files alone rather than
			// deleting data an operator might still salvage.
			continue
		}
		bestRaw, err := json.MarshalIndent(man, "", "  ")
		if err != nil {
			return stats, err
		}
		for d := 0; d < NumShards; d++ {
			if !diskExists(s.dirs[d]) {
				continue
			}
			if !onBest[d] {
				mp := manifestPath(s.dirs[d], id)
				// Re-check validity on this disk so healing is counted
				// only when the copy is actually missing/stale/garbled.
				needHeal := true
				if b, err := os.ReadFile(mp); err == nil {
					var m Manifest
					if json.Unmarshal(b, &m) == nil && m.Gen == man.Gen &&
						validateManifest(&m) == nil {
						needHeal = false
					}
				}
				if needHeal {
					if err := writeStagedAndCommit(s.dirs[d], id, "manifest", bestRaw, mp); err == nil {
						stats.ManifestCopiesHealed++
					}
				}
			}
			n, err := s.gcDiskShards(s.dirs[d], id, man)
			if err != nil {
				return stats, err
			}
			stats.OrphanShardsRemoved += n
		}
	}
	return stats, nil
}

// gcDiskShards removes shard files on disk dir for id that do not belong
// to the manifest's generation. Only files matching the strict published
// shard naming are eligible; anything else is left untouched.
func (s *Store) gcDiskShards(dir, id string, man *Manifest) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	prefix := id + "-gen"
	removed := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".shard") {
			continue
		}
		body := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".shard")
		// body is "<gen>-<role>".
		dash := strings.IndexByte(body, '-')
		if dash <= 0 {
			continue
		}
		genN, err := strconv.ParseUint(body[:dash], 10, 64)
		if err != nil {
			continue
		}
		role := body[dash+1:]
		if role != "a" && role != "b" && role != "p" {
			continue
		}
		if genN == man.Gen {
			continue // referenced by the published manifest: retain
		}
		if err := os.Remove(filepath.Join(dir, name)); err == nil {
			removed++
		}
	}
	if removed > 0 {
		_ = syncDir(dir)
	}
	return removed, nil
}
