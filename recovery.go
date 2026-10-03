package xorstore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// RecoveryStats reports what restart recovery did.
type RecoveryStats struct {
	// StagedRemoved is the number of unpublished temp files deleted.
	StagedRemoved int
	// OrphanShardsRemoved is the number of shard files deleted that were
	// not referenced by the current published manifest: half-finished
	// writes, generations superseded by a delete/recreate, or (when no
	// manifest at all names the id) shards of an incarnation whose
	// manifest evidence was lost.
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
//  2. Collect object ids from both manifest copies and published shard
//     files (so an incarnation whose manifests were all lost is still
//     visible to GC).
//  3. For every id with a usable best manifest: heal missing/stale/
//     garbled copies (a deleted key heals back to its tombstone, never to
//     old content) and delete shard files not referenced by the current
//     manifest. A live manifest retains exactly its own generation; a
//     tombstone retains none.
//  4. For an id with no manifest on any disk, remove all its shards:
//     without manifest evidence the generations cannot be distinguished,
//     and leaving them would let a same-name first write reuse an old
//     generation.
//
// Shards referenced by the current published manifest are always
// retained.
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

	// 2. Collect ids named by manifest copies and ids seen on shards.
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
	shardIDs := map[string]struct{}{}
	for _, d := range s.dirs {
		for _, id := range listShardIDs(d) {
			shardIDs[id] = struct{}{}
			idSet[id] = struct{}{}
		}
	}
	stats.KeysScanned = len(idSet)

	// 3/4. Per key: resolve the best manifest (if any), heal copies and
	// GC shards not referenced by the current generation.
	for id := range idSet {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		man, onBest, err := s.loadBestManifest(ctx, id, false)
		if err != nil {
			// No usable manifest anywhere.
			if _, hasShards := shardIDs[id]; hasShards && errors.Is(err, ErrManifestCorrupt) {
				// Garbled copies exist on some disks: leave everything
				// for operator salvage rather than deleting data.
				continue
			}
			// No manifest names this id (all copies lost): the shard
			// generations are unidentifiable. Remove them so a same-name
			// recreation cannot reuse an old generation.
			for d := 0; d < NumShards; d++ {
				n, gerr := s.gcDiskShards(s.dirs[d], id, nil)
				if gerr != nil {
					return stats, gerr
				}
				stats.OrphanShardsRemoved += n
			}
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
						m.Deleted == man.Deleted && validateManifest(&m) == nil {
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

// listShardIDs returns the ids of published shard files (strict
// "<id>-gen<N>-<role>.shard" naming) present directly in dir.
func listShardIDs(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var ids []string
	for _, e := range entries {
		_, sid, ok := parseShardName(e.Name())
		if !ok {
			continue
		}
		ids = append(ids, sid)
	}
	return ids
}

// parseShardName splits a strict published shard filename
// "<id>-gen<N>-<role>.shard" into role and id. Anything else (temp
// files, directories, malformed names) returns ok=false.
func parseShardName(name string) (role, id string, ok bool) {
	const prefixSep = "-gen"
	if strings.HasSuffix(name, ".shard.tmp") {
		return "", "", false
	}
	if !strings.HasSuffix(name, ".shard") {
		return "", "", false
	}
	base := strings.TrimSuffix(name, ".shard")
	dash := strings.Index(base, prefixSep)
	if dash <= 0 {
		return "", "", false
	}
	id = base[:dash]
	rest := base[dash+len(prefixSep):]
	r := strings.IndexByte(rest, '-')
	if r < 0 {
		return "", "", false
	}
	genPart := rest[:r]
	role = rest[r+1:]
	if role != "a" && role != "b" && role != "p" {
		return "", "", false
	}
	for _, c := range genPart {
		if c < '0' || c > '9' {
			return "", "", false
		}
	}
	if genPart == "" {
		return "", "", false
	}
	return role, id, true
}

// gcDiskShards removes shard files on disk dir for id that are not
// referenced by the current manifest:
//
//   - man == nil (no manifest names the id anywhere): every generation is
//     orphan and gets removed;
//   - man.Deleted (tombstone at G): every remaining generation gets
//     removed (the deleted content lived at G-1 or below);
//   - live manifest at G: only files named for exactly G are retained,
//     so shards a half-finished write or an older incarnation left
//     behind are collected while the current content is preserved.
//
// Only files matching the strict published shard naming are eligible;
// anything else is left untouched.
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
		role, fileID, ok := parseShardName(name)
		if !ok || fileID != id {
			continue
		}
		_ = role
		if man != nil && !man.Deleted {
			body := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".shard")
			dash := strings.IndexByte(body, '-')
			genN, perr := strconv.ParseUint(body[:dash], 10, 64)
			if perr != nil {
				continue
			}
			if genN == man.Gen {
				continue // referenced by the live manifest: retain
			}
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
