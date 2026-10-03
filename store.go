package xorstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Hooks injects faults/observers into the store, primarily for tests.
// Every hook is optional; returning an error aborts the operation at the
// hook boundary as if a crash had prevented further progress (the
// partially staged files stay on disk until the next recovery).
type Hooks struct {
	BeforeDeleteCommit func(key string, gen uint64) error
	// BeforeManifestCommit runs after all three new shards have been
	// staged and verified, before any of them (or the new manifest) is
	// published. Returning an error aborts the write with no readable
	// version change.
	BeforeManifestCommit func(key string, gen uint64) error
	// BeforeRepairCommit runs during a Repair after the replacement
	// shard has been staged, but before it is published. It lets a test
	// interleave a newer conditional write with an in-flight old-gen
	// repair.
	BeforeRepairCommit func(key string, gen uint64) error
}

// Store is the three-disk object store.
type Store struct {
	dirs  [NumShards]string
	hooks Hooks

	mu   sync.Mutex
	keys map[string]*keyLock
}

// keyLock serializes all state-changing operations for one object, which
// makes generation compare-and-publish atomic.
type keyLock struct{ sync.Mutex }

// Option customizes Open.
type Option func(*openConfig)

type openConfig struct {
	skipRecovery bool
}

// SkipRecoveryOnOpen skips the restart-recovery pass during Open. It is
// used when several processes/handlers share one set of directories and
// one of them must not sweep another's in-flight staged files (the
// primary opener should run recovery).
func SkipRecoveryOnOpen() Option {
	return func(c *openConfig) { c.skipRecovery = true }
}

// Open opens (creating if needed) a store backed by the three given
// directories and immediately runs restart recovery: unpublished staged
// files are swept, while shards referenced by any published manifest are
// retained.
func Open(ctx context.Context, d0, d1, d2 string, hooks *Hooks, opts ...Option) (*Store, error) {
	cfg := openConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	s := &Store{
		dirs: [NumShards]string{d0, d1, d2},
		keys: make(map[string]*keyLock),
	}
	if hooks != nil {
		s.hooks = *hooks
	}
	for _, d := range s.dirs {
		if err := os.MkdirAll(filepath.Join(d, metaDirName), dirPerm); err != nil {
			return nil, fmt.Errorf("xorstore: init disk %q: %w", d, err)
		}
		if err := os.MkdirAll(filepath.Join(d, stageDirName), dirPerm); err != nil {
			return nil, fmt.Errorf("xorstore: init stage %q: %w", d, err)
		}
	}
	if !cfg.skipRecovery {
		if _, err := s.recover(ctx); err != nil {
			return nil, fmt.Errorf("xorstore: recovery: %w", err)
		}
	}
	return s, nil
}

// lockKey returns the per-key mutex, creating it on first use.
func (s *Store) lockKey(key string) *keyLock {
	s.mu.Lock()
	kl, ok := s.keys[key]
	if !ok {
		kl = &keyLock{}
		s.keys[key] = kl
	}
	s.mu.Unlock()
	return kl
}

// currentGen returns the generation of the best readable manifest (nil if
// none) and ensures all disks that can hold a copy do hold the best copy.
// Caller must hold the key lock.
//
// When no manifest is readable but published shard files for the id exist
// on disk, the object has a partially erased history whose highest
// generation cannot be determined; the error wraps ErrGenerationEvidence
// so callers never mistake that for a never-written object and reuse a
// stale generation.
func (s *Store) currentGen(ctx context.Context, key string) (*Manifest, error) {
	id := keyID(key)
	man, _, err := s.loadBestManifest(ctx, id, true)
	if errors.Is(err, ErrNotFound) {
		if s.publishedShardEvidence(id) {
			return nil, fmt.Errorf("key %q: shards present without manifest: %w",
				key, ErrGenerationEvidence)
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return man, nil
}

// publishedShardEvidence reports whether any disk carries a
// generation-scoped published shard file for id. Stage directories are
// not consulted (their files are unpublished and swept on restart).
func (s *Store) publishedShardEvidence(id string) bool {
	for _, d := range s.dirs {
		if !diskExists(d) {
			continue
		}
		entries, err := os.ReadDir(d)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if _, _, ok := parseShardName(id, e.Name()); ok {
				return true
			}
		}
	}
	return false
}

// loadBestManifest reads all manifest copies, validates them and returns
// the one with the highest generation. When heal is true, disks with no
// manifest or with an older/garbled copy are refreshed. The returned
// bool slice marks which disks held the returned manifest bytes.
func (s *Store) loadBestManifest(ctx context.Context, id string, heal bool) (*Manifest, []bool, error) {
	type cand struct {
		man  *Manifest
		raw  []byte
		disk int
	}
	var cands []cand
	validAny := false
	for d := 0; d < NumShards; d++ {
		if !diskExists(s.dirs[d]) {
			continue
		}
		raw, err := os.ReadFile(manifestPath(s.dirs[d], id))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			continue
		}
		man := &Manifest{}
		if err := json.Unmarshal(raw, man); err != nil {
			continue // garbled manifest copy
		}
		if err := validateManifest(man); err != nil {
			continue
		}
		validAny = true
		cands = append(cands, cand{man: man, raw: raw, disk: d})
	}
	if !validAny {
		// Distinguish "never written" from "copies exist but are garbage":
		// if no file exists on any disk it is NotFound, otherwise corrupt.
		anyFile := false
		for d := 0; d < NumShards; d++ {
			if fileExists(manifestPath(s.dirs[d], id)) {
				anyFile = true
				break
			}
		}
		if anyFile {
			return nil, nil, ErrManifestCorrupt
		}
		return nil, nil, ErrNotFound
	}

	best := cands[0]
	for _, c := range cands[1:] {
		if c.man.Gen > best.man.Gen {
			best = c
		}
	}
	bestRaw, err := json.MarshalIndent(best.man, "", "  ")
	if err != nil {
		return nil, nil, err
	}

	onBest := make([]bool, NumShards)
	for _, c := range cands {
		if c.man.Gen == best.man.Gen {
			onBest[c.disk] = true
		}
	}

	if heal {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		for d := 0; d < NumShards; d++ {
			if onBest[d] || !diskExists(s.dirs[d]) {
				continue
			}
			// Missing, older or garbled copy: re-publish best. Failure to
			// heal one disk is not fatal to the read/write.
			_ = writeStagedAndCommit(s.dirs[d], id, "manifest", bestRaw, manifestPath(s.dirs[d], id))
			onBest[d] = true
		}
	}
	return best.man, onBest, nil
}

// validateManifest checks the structural invariants a manifest must meet.
// A deletion record ("tombstone") carries only the key and the generation
// past which the object must stay unreadable; it has no shards.
func validateManifest(m *Manifest) error {
	if m.Schema != 1 {
		return errors.New("bad schema")
	}
	if m.Key == "" {
		return errors.New("empty key")
	}
	if m.Gen == 0 {
		return errors.New("zero generation")
	}
	if m.Length < 0 {
		return errors.New("negative length")
	}
	if m.Deleted {
		if m.Length != 0 {
			return errors.New("tombstone with nonzero length")
		}
		for i := 0; i < NumShards; i++ {
			if m.Shards[i] != (ShardInfo{}) {
				return errors.New("tombstone with shard metadata")
			}
		}
		return nil
	}
	roles := map[string]int{}
	for i := 0; i < NumShards; i++ {
		sh := m.Shards[i]
		if sh.Size < 0 || len(sh.Digest) != 64 {
			return errors.New("bad shard metadata")
		}
		roles[sh.Role]++
	}
	if roles["a"] != 1 || roles["b"] != 1 || roles["p"] != 1 {
		return errors.New("bad shard roles")
	}
	return nil
}
