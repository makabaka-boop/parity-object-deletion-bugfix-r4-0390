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
	// OrphanShardsRemoved is the number of shard files not referenced
	// by any published manifest (half-finished writes, deleted
	// generations, externally erased objects) deleted.
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
//  2. For every object id found in manifests or in generation-scoped
//     shard file names, load the best published manifest, heal
//     missing/stale/garbled copies and delete shard files the best
//     generation does not reference.
//  3. An id with no manifest file on any disk (externally or legacy
//     remove-based deletion) can only have its shards treated as
//     unreferenced when every disk directory is present — otherwise the
//     manifest copies might live on the missing disk and the shards are
//     retained until it returns.
//  4. An id whose manifest files exist but are all unreadable is left
//     untouched: an operator may still salvage the data.
//
// Shards referenced by a published manifest (including a newer
// generation of a rebuilt same-name key) are always retained.
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

	// 2. Collect the set of object ids from both manifest directories and
	//    published shard file names, so an object whose manifests were
	//    removed (legacy delete, external erase) still gets its unreferenced
	//    shards collected instead of leaking forever.
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
		diskEntries, err := os.ReadDir(d)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return stats, err
		}
		for _, e := range diskEntries {
			if e.IsDir() {
				continue
			}
			if id, _, _, ok := parseAnyShardName(e.Name()); ok {
				idSet[id] = struct{}{}
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
			if !errors.Is(err, ErrNotFound) {
				// Corrupt-but-present manifests: leave everything alone
				// rather than deleting data an operator might salvage.
				continue
			}
			// No manifest on any disk. Collect the shards only when all
			// disk directories are present; with a disk missing its
			// manifest copy may still exist there and the shards must
			// survive until it comes back.
			allDisks := true
			for _, d := range s.dirs {
				if !diskExists(d) {
					allDisks = false
					break
				}
			}
			if !allDisks {
				continue
			}
			for _, d := range s.dirs {
				n, gerr := s.removeAllShardsForID(d, id)
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

// parseShardName reports whether name is a generation-scoped published
// shard file for id and, when it is, returns its generation and role.
// The strict naming is "<id>-gen<N>-<role>.shard" with role a/b/p.
func parseShardName(id, name string) (gen uint64, role string, ok bool) {
	gid, g, role, ok := parseAnyShardName(name)
	if !ok || gid != id {
		return 0, "", false
	}
	return g, role, true
}

// parseAnyShardName parses any generation-scoped shard file name,
// returning the id encoded in the name.
func parseAnyShardName(name string) (id string, gen uint64, role string, ok bool) {
	const suffix = ".shard"
	const mid = "-gen"
	if !strings.HasSuffix(name, suffix) {
		return "", 0, "", false
	}
	body := name[:len(name)-len(suffix)]
	gIdx := strings.Index(body, mid)
	if gIdx <= 0 {
		return "", 0, "", false
	}
	id = body[:gIdx]
	rest := body[gIdx+len(mid):]
	dash := strings.IndexByte(rest, '-')
	if dash <= 0 {
		return "", 0, "", false
	}
	g, err := strconv.ParseUint(rest[:dash], 10, 64)
	if err != nil || g == 0 {
		return "", 0, "", false
	}
	role = rest[dash+1:]
	if role != "a" && role != "b" && role != "p" {
		return "", 0, "", false
	}
	return id, g, role, true
}

// gcDiskShards removes shard files on disk dir for id that do not belong
// to the manifest's generation. A deletion tombstone references no
// generation, so all of the id's shards are collected. Only files
// matching the strict published shard naming are eligible; anything else
// is left untouched.
func (s *Store) gcDiskShards(dir, id string, man *Manifest) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	removed := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		genN, _, ok := parseShardName(id, name)
		if !ok {
			continue
		}
		if !man.Deleted && genN == man.Gen {
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

// removeAllShardsForID unlinks every generation-scoped shard file for id
// on dir. It is used only when no manifest for the id exists on any
// present disk, i.e. the shards cannot be referenced by a live
// generation.
func (s *Store) removeAllShardsForID(dir, id string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	removed := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, _, ok := parseShardName(id, e.Name()); !ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err == nil {
			removed++
		}
	}
	if removed > 0 {
		_ = syncDir(dir)
	}
	return removed, nil
}
