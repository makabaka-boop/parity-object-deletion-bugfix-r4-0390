package xorstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Hooks injects faults/observers into the store, primarily for tests.
// Every hook is optional; returning an error aborts the operation at the
// hook boundary as if a crash had prevented further progress (the
// partially staged files stay on disk until the next recovery).
type Hooks struct {
	// BeforeDeleteCommit runs after the delete condition (current
	// generation == expectGen) has been checked and the tombstone manifest
	// staged on every available disk, but before any tombstone copy is
	// published. gen is the generation Delete will return. Returning an
	// error aborts the delete; ErrSimulatedCrash keeps the staged files as
	// a modelled crash (recovery sweeps them and the object stays live).
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

// currentGen returns the generation of the best readable manifest (0 if
// none) and ensures all disks that can hold a copy do hold the best copy.
// Caller must hold the key lock.
func (s *Store) currentGen(ctx context.Context, key string) (*Manifest, error) {
	id := keyID(key)
	man, _, err := s.loadBestManifest(ctx, id, true)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return man, nil
}

// Generation returns the current generation of key, 0 if the key has no
// manifest (never seen) and ErrNotFound if the key is currently deleted
// (its best manifest is a tombstone). It lets a caller learn the
// conditional generation for a Put after a Delete: recreating a deleted
// key requires the delete generation returned by Delete, never zero.
func (s *Store) Generation(ctx context.Context, key string) (uint64, error) {
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
	if cur == nil {
		return 0, nil
	}
	if cur.Deleted {
		return cur.Gen, fmt.Errorf("generation %q deleted at gen %d: %w", key, cur.Gen, ErrNotFound)
	}
	return cur.Gen, nil
}

// anyPublishedShard reports whether any disk holds a published shard file
// for id (generation-scoped name, any generation). It is used to detect
// "generation evidence without a manifest" — the state left when every
// manifest copy of a key has been lost. A first-time Put is refused in
// that state rather than reusing an old generation.
func (s *Store) anyPublishedShard(id string) bool {
	for _, d := range s.dirs {
		if !diskExists(d) {
			continue
		}
		entries, err := os.ReadDir(d)
		if err != nil {
			continue
		}
		prefix := id + "-gen"
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".shard") {
				continue
			}
			_, fileID, ok := parseShardName(name)
			if ok && fileID == id {
				return true
			}
		}
	}
	return false
}

// publishManifest atomically publishes man to every present disk: all
// copies are first staged and verified (an error before the first rename
// leaves every existing manifest untouched), then renamed one by one.
// After the first rename the manifest is readable and remaining copies
// are healed. beforeCommit, when non-nil, runs after every copy is staged
// but before any rename; an error from it (other than ErrSimulatedCrash)
// aborts the staged copies and nothing is published. It returns the
// number of disks on which publication was attempted (present disks).
func (s *Store) publishManifest(ctx context.Context, id string, man *Manifest,
	beforeCommit func() error) (int, error) {
	raw, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return 0, err
	}
	staged := [NumShards]string{}
	present := 0
	for d := 0; d < NumShards; d++ {
		if !diskExists(s.dirs[d]) {
			continue
		}
		path, err := stageFile(s.dirs[d], id, "manifest", raw)
		if err != nil {
			s.abortStaged(staged[:])
			return present, fmt.Errorf("stage manifest: %w", err)
		}
		staged[d] = path
		present++
	}
	if present == 0 {
		return 0, errors.New("xorstore: no disk available")
	}

	if beforeCommit != nil {
		if err := beforeCommit(); err != nil {
			if !errors.Is(err, ErrSimulatedCrash) {
				s.abortStaged(staged[:])
			}
			return present, err
		}
	}

	published := 0
	var publishErr error
	for d := 0; d < NumShards; d++ {
		if staged[d] == "" {
			continue
		}
		if err := commitStaged(staged[d], manifestPath(s.dirs[d], id)); err != nil {
			publishErr = err
			break
		}
		published++
	}
	if publishErr == nil {
		return present, nil
	}
	if published == 0 {
		s.abortStaged(staged[:])
		return present, fmt.Errorf("publish manifest: %w", publishErr)
	}
	// At least one copy is live, so the new manifest is the readable one.
	// Heal the remaining copies before reporting.
	if _, _, herr := s.loadBestManifest(ctx, id, true); herr != nil {
		return present, fmt.Errorf("manifest partially published (%d disks), heal failed: %w",
			published, herr)
	}
	return present, nil
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
		// A tombstone references no shards. It must not masquerade as a
		// live manifest: the shard table is required to be empty.
		for i := 0; i < NumShards; i++ {
			if m.Shards[i] != (ShardInfo{}) {
				return errors.New("tombstone carries shard metadata")
			}
		}
		if m.Length != 0 {
			return errors.New("tombstone carries length")
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

// buildTombstone assembles the delete marker for key at deleteGen (the
// generation Delete returns: the deleted content's generation plus one).
func buildTombstone(key string, deleteGen uint64) *Manifest {
	return &Manifest{
		Deleted: true,
		Schema:  1,
		Key:     key,
		Gen:     deleteGen,
	}
}
