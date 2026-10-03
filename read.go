package xorstore

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// repairPlan is the outcome of inspecting one object: its manifest, the
// shard bytes that verified and the indexes of bad/missing shards plus
// reconstructed replacement bytes for them.
type repairPlan struct {
	id   string
	man  *Manifest
	good [NumShards][]byte
	bad  []int
	repl [NumShards][]byte // reconstructed bytes for bad shards
}

// Get returns the object's data, its generation and the indexes of any
// shards that had to be rebuilt and rewritten during this read.
//
// Exactly one bad/missing shard is transparently reconstructed (each
// shard is the XOR of the other two), verified against the manifest
// digest, and repaired on its disk. Two bad shards yield ErrUnrecoverable.
func (s *Store) Get(ctx context.Context, key string) (data []byte, gen uint64, repaired []int, err error) {
	kl := s.lockKey(key)
	kl.Lock()
	defer kl.Unlock()

	plan, err := s.planRepair(ctx, key)
	if err != nil {
		return nil, 0, nil, err
	}
	if len(plan.bad) == 1 {
		installed, ierr := s.commitRepair(ctx, plan)
		repaired = installed
		if ierr != nil && !errors.Is(ierr, ErrConflict) && !errors.Is(ierr, errDiskUnavailable) {
			// The data is reconstructable even if it could not be
			// written back; a stale-repair conflict is also benign here.
			return nil, 0, nil, ierr
		}
	} else if len(plan.bad) >= 2 {
		return nil, plan.man.Gen, nil,
			fmt.Errorf("get %q: %d shards bad: %w", key, len(plan.bad), ErrUnrecoverable)
	}

	a := plan.shard(ShardA)
	b := plan.shard(ShardB)
	return joinShards(a, b, plan.man.Length), plan.man.Gen, repaired, nil
}

// shard returns verified shard bytes from the plan, using the
// reconstructed replacement for a bad shard.
func (p *repairPlan) shard(i int) []byte {
	if p.good[i] != nil {
		return p.good[i]
	}
	return p.repl[i]
}

// Repair runs the same single-shard rebuild/repair path as Get but
// without returning data. It is the entry point used by the background
// repair loop. Repairing a generation that has been superseded while the
// repair was staged fails with ErrConflict and never touches the newer
// generation's files.
func (s *Store) Repair(ctx context.Context, key string) (gen uint64, repaired []int, err error) {
	kl := s.lockKey(key)
	kl.Lock()
	defer kl.Unlock()

	plan, err := s.planRepair(ctx, key)
	if err != nil {
		return 0, nil, err
	}
	if len(plan.bad) >= 2 {
		return plan.man.Gen, nil,
			fmt.Errorf("repair %q: %d shards bad: %w", key, len(plan.bad), ErrUnrecoverable)
	}
	if len(plan.bad) == 0 {
		return plan.man.Gen, nil, nil
	}
	installed, err := s.commitRepair(ctx, plan)
	return plan.man.Gen, installed, err
}

// planRepair reads the best manifest and all shards, verifies them and
// constructs replacements for bad shards. Caller must hold the key lock.
func (s *Store) planRepair(ctx context.Context, key string) (*repairPlan, error) {
	id := keyID(key)
	man, _, err := s.loadBestManifest(ctx, id, true)
	if err != nil {
		return nil, err
	}
	plan := &repairPlan{id: id, man: man}

	for i := 0; i < NumShards; i++ {
		b, ok := s.readShard(i, man)
		if ok {
			plan.good[i] = b
		} else {
			plan.bad = append(plan.bad, i)
		}
	}

	switch len(plan.bad) {
	case 0, 1:
		// One missing shard is XOR-reconstructable from the other two
		// (zero bad: nothing to do).
	default:
		return plan, nil // Get/Repair turn this into ErrUnrecoverable
	}
	for _, bad := range plan.bad {
		var x, y int
		switch bad {
		case ShardA:
			x, y = ShardB, ShardP
		case ShardB:
			x, y = ShardA, ShardP
		case ShardP:
			x, y = ShardA, ShardB
		}
		if len(plan.good[x]) != len(plan.good[y]) {
			// The two surviving shards disagree in length, so their
			// XOR cannot be the manifest shard.
			return plan, fmt.Errorf("repair %q: surviving shards inconsistent: %w",
				key, ErrUnrecoverable)
		}
		z := reconstruct(plan.good[x], plan.good[y])
		meta := man.Shards[bad]
		if len(z) != meta.Size || digestBytes(z) != meta.Digest {
			// Rebuilt bytes do not match the manifest: the surviving
			// shards are not the ones the manifest describes.
			return plan, fmt.Errorf("repair %q: reconstructed shard %d fails digest check: %w",
				key, bad, ErrUnrecoverable)
		}
		plan.repl[bad] = z
	}
	return plan, nil
}

// readShard reads and verifies shard i. ok is false for a missing disk,
// missing file, read error or digest/length mismatch (a "corrupt" shard).
func (s *Store) readShard(i int, man *Manifest) ([]byte, bool) {
	if !diskExists(s.dirs[i]) {
		return nil, false
	}
	role := man.Shards[i].Role
	path := shardPath(s.dirs[i], keyID(man.Key), man.Gen, role)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	meta := man.Shards[i]
	if len(b) != meta.Size || digestBytes(b) != meta.Digest {
		return nil, false
	}
	return b, true
}

// errDiskUnavailable marks a bad shard whose disk directory is gone and
// therefore cannot be rewritten in place; the shard is still served from
// reconstruction.
var errDiskUnavailable = errors.New("xorstore: disk unavailable for rewrite")

// repairStaged is one staged replacement shard awaiting install.
type repairStaged struct {
	disk  int
	path  string
	final string
}

// commitRepair stages and installs replacement shards. Caller must hold
// the key lock. Just before installing, it re-reads the published
// manifest: if the generation advanced while the repair was being
// prepared it aborts with ErrConflict without writing anything, so an
// old-generation repair can never clobber a newer generation.
func (s *Store) commitRepair(ctx context.Context, plan *repairPlan) ([]int, error) {
	var items []repairStaged
	for _, bad := range plan.bad {
		if !diskExists(s.dirs[bad]) {
			continue // cannot rewrite onto an absent disk
		}
		// Another repair may have landed first; skip if already valid.
		if b, ok := s.readShard(bad, plan.man); ok && b != nil {
			continue
		}
		path, err := stageFile(s.dirs[bad], plan.id,
			fmt.Sprintf("gen%d-repair-%s", plan.man.Gen, plan.man.Shards[bad].Role),
			plan.repl[bad])
		if err != nil {
			abortRepairItems(items)
			return nil, fmt.Errorf("stage repair shard: %w", err)
		}
		items = append(items, repairStaged{
			disk:  bad,
			path:  path,
			final: shardPath(s.dirs[bad], plan.id, plan.man.Gen, plan.man.Shards[bad].Role),
		})
	}
	if len(items) == 0 {
		return nil, errDiskUnavailable
	}

	// Fault/observer point: a test interleaves a newer conditional
	// write at exactly this moment.
	if s.hooks.BeforeRepairCommit != nil {
		if err := s.hooks.BeforeRepairCommit(plan.man.Key, plan.man.Gen); err != nil {
			if !errors.Is(err, ErrSimulatedCrash) {
				abortRepairItems(items)
			}
			return nil, err
		}
	}

	// Generation guard: re-read under the same key lock. If the
	// published generation moved, drop every staged replacement.
	cur, _, err := s.loadBestManifest(ctx, plan.id, false)
	if err != nil {
		abortRepairItems(items)
		return nil, err
	}
	if cur.Gen != plan.man.Gen {
		abortRepairItems(items)
		return nil, fmt.Errorf("repair gen %d superseded by gen %d: %w",
			plan.man.Gen, cur.Gen, ErrConflict)
	}

	installed := make([]int, 0, len(items))
	for i, it := range items {
		if err := ctx.Err(); err != nil {
			abortRepairItems(items[i:])
			return installed, err
		}
		if err := commitStaged(it.path, it.final); err != nil {
			abortRepairItems(items[i+1:])
			return installed, fmt.Errorf("install repair shard on disk %d: %w", it.disk, err)
		}
		installed = append(installed, it.disk)
	}
	return installed, nil
}

func abortRepairItems(items []repairStaged) {
	for _, it := range items {
		removeBestEffort(it.path)
	}
}
