package xorstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func manifestRaw(t *testing.T, e *testEnv, key string, disk int) []byte {
	t.Helper()
	b, err := os.ReadFile(manifestPath(e.dirs[disk], keyID(key)))
	if err != nil {
		t.Fatalf("read manifest disk%d: %v", disk, err)
	}
	return b
}

func manifestOnDisk(e *testEnv, key string, disk int) bool {
	return fileExists(manifestPath(e.dirs[disk], keyID(key)))
}

// TestDeleteIsAtomicAndDurable covers the core deletion contract:
//   - a successful Delete makes Get fail with ErrNotFound immediately;
//   - the deletion is published as a higher-generation tombstone, so it
//     survives restart and stale manifest copies never resurrect data;
//   - Delete is conditional: a wrong expectation returns ErrConflict and
//     leaves the object readable; deleting a missing key is ErrNotFound;
//   - after the delete, same-name recreation must use the generation
//     Delete returned and lands at a strictly higher generation.
func TestDeleteIsAtomicAndDurable(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old-content"), 0)

	// Wrong expectation: conflict, object stays readable.
	g, err := e.s.Delete(context.Background(), key, gen1+9)
	if !errors.Is(err, ErrConflict) || g != gen1 {
		t.Fatalf("wrong-gen Delete gen=%d err=%v, want %d/ErrConflict", g, err, gen1)
	}
	mustGet(t, e.s, key, []byte("old-content"))

	// Successful conditional delete returns the post-delete generation.
	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if delGen != gen1+1 {
		t.Fatalf("Delete returned gen %d, want %d", delGen, gen1+1)
	}

	// Unreadable immediately, via Get and Repair.
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete err=%v, want ErrNotFound", err)
	}
	if _, _, err := e.s.Repair(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Repair after delete err=%v, want ErrNotFound", err)
	}
	// Deleting again is not found.
	if _, err := e.s.Delete(context.Background(), key, delGen); !errors.Is(err, ErrNotFound) {
		t.Fatalf("re-Delete err=%v, want ErrNotFound", err)
	}

	// Deleting a key that never existed is NotFound.
	if _, err := e.s.Delete(context.Background(), "never", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete missing err=%v, want ErrNotFound", err)
	}

	// Recreate with the same name using the post-delete generation: it
	// must land ABOVE the deleted generation, never reuse gen1.
	gen3 := mustPut(t, e.s, key, []byte("new-content"), delGen)
	if gen3 != delGen+1 {
		t.Fatalf("recreate gen=%d, want %d", gen3, delGen+1)
	}
	got, g, _, err := e.s.Get(context.Background(), key)
	if err != nil || g != gen3 || string(got) != "new-content" {
		t.Fatalf("Get recreated = %q gen %d err %v", got, g, err)
	}
	// A first-write expectation (0) and the deleted generation both conflict.
	if _, err := e.s.Put(context.Background(), key, []byte("x"), 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("Put expect0 over recreated err=%v, want ErrConflict", err)
	}
	if _, err := e.s.Put(context.Background(), key, []byte("x"), delGen); !errors.Is(err, ErrConflict) {
		t.Fatalf("Put stale after recreate err=%v, want ErrConflict", err)
	}

	// Restart: tombstone-healed state keeps the recreated content; the
	// old generation must never reappear.
	s2 := e.reopen(nil)
	got, g, _, err = s2.Get(context.Background(), key)
	if err != nil || g != gen3 || string(got) != "new-content" {
		t.Fatalf("Get after restart = %q gen %d err %v, want new-content/%d", got, g, err, gen3)
	}

	// Delete the recreated object too: again unreadable, durably.
	gen4, err := s2.Delete(context.Background(), key, gen3)
	if err != nil || gen4 != gen3+1 {
		t.Fatalf("second Delete gen=%d err=%v", gen4, err)
	}
	_ = e.reopen(nil)
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete+restart err=%v, want ErrNotFound", err)
	}
}

// TestDeleteTombstoneHealsStaleCopies simulates the "manifest copies
// temporarily unavailable" history: after delete, force one disk's copy
// back to the old live manifest (as if it had been offline) and reopen.
// Recovery must pick the higher-generation tombstone, heal the stale
// copy, and keep the object unreadable.
func TestDeleteTombstoneHealsStaleCopies(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old"), 0)
	oldCopy := manifestRaw(t, e, key, ShardA)

	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil || delGen != gen1+1 {
		t.Fatalf("Delete: gen=%d err=%v", delGen, err)
	}
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("post-delete Get: %v", err)
	}

	// Regress disk0's manifest to the gen1 live copy (offline-disk replay).
	if err := os.WriteFile(manifestPath(e.dirs[ShardA], keyID(key)), oldCopy, filePerm); err != nil {
		t.Fatalf("regress manifest: %v", err)
	}
	// Restart must heal the stale copy from the tombstone, not serve it.
	s2 := e.reopen(nil)
	if _, _, _, err := s2.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale copy resurrected deleted object: %v", err)
	}
	var healed Manifest
	if err := json.Unmarshal(manifestRaw(t, e, key, ShardA), &healed); err != nil {
		t.Fatalf("healed copy unreadable: %v", err)
	}
	if !healed.Deleted || healed.Gen != delGen {
		t.Fatalf("disk0 manifest = gen %d deleted=%v, want tombstone gen %d",
			healed.Gen, healed.Deleted, delGen)
	}
}

// TestDeleteFailureLeavesObjectReadable injects a fault at the delete
// commit boundary: the deletion must not be visible, the object stays
// readable, staged files are swept, and retrying the delete afterwards
// succeeds.
func TestDeleteFailureLeavesObjectReadable(t *testing.T) {
	key := "obj"
	var sawGen uint64
	hooks := &Hooks{
		BeforeDeleteCommit: func(k string, gen uint64) error {
			sawGen = gen
			return ErrSimulatedCrash
		},
	}
	e := newTestEnv(t, hooks)
	gen1 := mustPut(t, e.s, key, []byte("still-here"), 0)

	if _, err := e.s.Delete(context.Background(), key, gen1); !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("Delete err=%v, want ErrSimulatedCrash", err)
	}
	if sawGen != gen1+1 {
		t.Fatalf("hook saw gen %d, want %d", sawGen, gen1+1)
	}
	// Deletion not live: object fully readable, no conflict state. The
	// hook fires before any tombstone copy is staged, so nothing is
	// half-published either.
	mustGet(t, e.s, key, []byte("still-here"))
	for d := 0; d < NumShards; d++ {
		if !manifestOnDisk(e, key, d) {
			t.Fatalf("manifest copy %d vanished after failed delete", d)
		}
	}
	if n := e.stageFileCount(); n != 0 {
		t.Fatalf("pre-stage delete failure left staged files: %d", n)
	}

	// Restart leaves the live object untouched.
	s2 := e.reopen(nil)
	if e.stageFileCount() != 0 {
		t.Fatalf("staged files after recovery = %d, want 0", e.stageFileCount())
	}
	mustGet(t, s2, key, []byte("still-here"))

	// Retry succeeds and the object stays deleted.
	delGen, err := s2.Delete(context.Background(), key, gen1)
	if err != nil || delGen != gen1+1 {
		t.Fatalf("retry Delete: gen=%d err=%v", delGen, err)
	}
	if _, _, _, err := s2.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after retry delete: %v", err)
	}
}

// TestRecreateDoesNotReuseGenerationWhenEvidenceRemains models the
// dangerous sequence: manifests are gone but old published shards remain
// on disk (legacy/externally removed object). A first write must be
// refused with ErrGenerationEvidence instead of reusing generation 1,
// because a reused generation would make old and new content
// indistinguishable. After recovery collects the unreferenced shards the
// first write is accepted again.
func TestRecreateDoesNotReuseGenerationWhenEvidenceRemains(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old"), 0)

	// External erase of every manifest copy, exactly like the previous
	// remove-based Delete (best effort, one copy could fail).
	id := keyID(key)
	for d := 0; d < NumShards; d++ {
		if err := os.Remove(manifestPath(e.dirs[d], id)); err != nil {
			t.Fatalf("remove manifest %d: %v", d, err)
		}
	}
	// Old gen1 shards are still on all disks.

	// No manifest is readable, but evidence of a past generation exists.
	_, err := e.s.Put(context.Background(), key, []byte("new"), 0)
	if !errors.Is(err, ErrGenerationEvidence) {
		t.Fatalf("Put over evidence err=%v, want ErrGenerationEvidence", err)
	}
	// Nothing readable half-appeared and no gen was reused: retrying
	// immediately reports the same evidence error. Reads themselves stay
	// NotFound (they must never resurrect the old bytes from stray shards);
	// it is only a first write that is refused.
	if _, _, _, gerr := e.s.Get(context.Background(), key); !errors.Is(gerr, ErrNotFound) {
		t.Fatalf("Get over evidence err=%v, want ErrNotFound", gerr)
	}
	if _, err := e.s.Put(context.Background(), key, []byte("new2"), 0); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatalf("retry Put over evidence err=%v, want ErrGenerationEvidence", err)
	}

	// Reopen runs recovery: shards unreferenced by ANY manifest are
	// collected with all disks present.
	s2 := e.reopen(nil)
	for d := 0; d < NumShards; d++ {
		if e.shardExistsOnDisk(key, gen1, d) {
			t.Fatalf("unreferenced shard %d survived recovery", d)
		}
	}
	// Fresh first write is accepted at gen1 — there is truly no history now.
	g := mustPut(t, s2, key, []byte("new"), 0)
	if g != 1 {
		t.Fatalf("Put after recovery gen=%d, want 1", g)
	}
	mustGet(t, s2, key, []byte("new"))
}

// TestLegacyRemovedObjectRecoveryReclaimsShards plants exactly the
// on-disk state of the old remove-based Delete (no manifests, shards
// present) and checks recovery statistics plus content neutrality.
func TestLegacyRemovedObjectRecoveryReclaimsShards(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old"), 0)
	id := keyID(key)
	for d := 0; d < NumShards; d++ {
		if err := os.Remove(manifestPath(e.dirs[d], id)); err != nil {
			t.Fatalf("remove manifest: %v", err)
		}
	}
	stats, err := e.s.recover(context.Background())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if stats.OrphanShardsRemoved < NumShards {
		t.Fatalf("OrphanShardsRemoved = %d, want >= %d", stats.OrphanShardsRemoved, NumShards)
	}
	for d := 0; d < NumShards; d++ {
		if e.shardExistsOnDisk(key, gen1, d) {
			t.Fatalf("deleted-gen shard %d not collected", d)
		}
	}
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get legacy-erased err=%v, want ErrNotFound", err)
	}
}

// TestDeleteRebuildWithOfflineDiskStaleCopy returns a disk that was
// absent across delete AND same-name recreate, still holding gen1 data.
// On its return the stale manifest copy must heal to the current
// generation, the stale gen1 shards must be collected, and single-shard
// tolerance for the current generation must be intact.
func TestDeleteRebuildWithOfflineDiskStaleCopy(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old"), 0)

	// disk2 goes offline before the delete.
	e.removeDisk(ShardP)
	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil || delGen != gen1+1 {
		t.Fatalf("delete with disk gone: gen=%d err=%v", delGen, err)
	}
	gen3 := mustPut(t, e.s, key, []byte("fresh"), delGen)

	// A peer-style open with the disk still gone serves the new content
	// degraded; disk2 still carries the old gen1 manifest and shards.
	if _, g, _, err := e.s.Get(context.Background(), key); err != nil || g != gen3 {
		t.Fatalf("degraded Get gen=%d err=%v", g, err)
	}

	// Corrupt one of the two present current-gen shards while disk2 is
	// still gone: reconstruction from the single survivor is impossible,
	// so two-bad is reported and nothing is guessed at.
	e.corruptShard(key, gen3, ShardA)
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrUnrecoverable) {
		t.Fatalf("two-bad with offline disk err=%v, want ErrUnrecoverable", err)
	}
	// Restore the bad shard so the environment is "one stale disk only"
	// when disk2 comes back.
	aShard, _, _ := splitShards([]byte("fresh"))
	if err := os.WriteFile(shardPath(e.dirs[ShardA], keyID(key), gen3, "a"), aShard, filePerm); err != nil {
		t.Fatalf("restore a: %v", err)
	}

	// disk2 returns. Reopen: it must heal disk2's gen1 manifest to gen3
	// and GC the stale gen1 shards there; its current-gen shard is
	// missing, so the first read reconstructs and rewrites it.
	s2 := e.reopen(nil)
	var m Manifest
	if err := json.Unmarshal(manifestRaw(t, e, key, ShardP), &m); err != nil {
		t.Fatalf("disk2 manifest after return: %v", err)
	}
	if m.Gen != gen3 || m.Deleted {
		t.Fatalf("disk2 healed to gen=%d deleted=%v, want gen %d live", m.Gen, m.Deleted, gen3)
	}
	for d := 0; d < NumShards; d++ {
		if e.shardExistsOnDisk(key, gen1, d) {
			t.Fatalf("stale gen1 shard on disk %d not collected", d)
		}
	}
	// First read with the returned-but-empty disk rebuilds its shard.
	got, g, repaired, err := s2.Get(context.Background(), key)
	if err != nil || g != gen3 || string(got) != "fresh" || len(repaired) != 1 || repaired[0] != ShardP {
		t.Fatalf("healing read on disk return = %q gen=%d repaired=%v err=%v", got, g, repaired, err)
	}
	for d := 0; d < NumShards; d++ {
		if !e.shardExistsOnDisk(key, gen3, d) {
			t.Fatalf("current gen3 shard missing on disk %d after rebuild", d)
		}
	}
	// Pre-existing single-shard recovery capability still holds.
	e.corruptShard(key, gen3, ShardB)
	got, g, repaired, err = s2.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("single-shard rebuild after disk return: %v", err)
	}
	if g != gen3 || string(got) != "fresh" || len(repaired) != 1 || repaired[0] != ShardB {
		t.Fatalf("rebuild = %q gen=%d repaired=%v", got, g, repaired)
	}
}

// TestOldRepairCannotResurrectDeleted interleaves a delete with a repair
// that has already staged its replacement shard: the repair's
// re-read guard must see the tombstone and reject the install with
// ErrConflict, and the deleted object stays unreadable.
func TestOldRepairCannotResurrectDeleted(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("generation-one"), 0)
	e.corruptShard(key, gen1, ShardP)

	deleterReady := make(chan struct{})
	releaseDelete := make(chan struct{})
	deleterDone := make(chan struct{})
	var deleter *Store
	var delGen uint64
	var delErr error

	hooks := &Hooks{
		BeforeRepairCommit: func(k string, gen uint64) error {
			if k != key || gen != gen1 {
				t.Fatalf("unexpected hook args %q/%d", k, gen)
			}
			go func() {
				close(deleterReady)
				<-releaseDelete
				delGen, delErr = deleter.Delete(context.Background(), k, gen1)
				close(deleterDone)
			}()
			<-deleterReady
			time.Sleep(20 * time.Millisecond)
			close(releaseDelete)
			// Block until the delete has landed so the guard re-read
			// observes the tombstone (channel sync, no shared-state poll).
			<-deleterDone
			return nil
		},
	}
	repairer := e.openPeer(hooks)
	deleter = e.openPeer(nil)

	_, repaired, err := repairer.Repair(context.Background(), key)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("repair vs delete err=%v repaired=%v, want ErrConflict", err, repaired)
	}
	if len(repaired) != 0 {
		t.Fatalf("old repair installed shards after delete: %v", repaired)
	}
	if delErr != nil || delGen != gen1+1 {
		t.Fatalf("interleaved Delete gen=%d err=%v, want %d/nil", delGen, delErr, gen1+1)
	}
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted object resurrected by stale repair: %v", err)
	}
	// The corrupt old parity shard must not have been rewritten with
	// old-generation bytes.
	path := shardPath(e.dirs[ShardP], keyID(key), gen1, "p")
	b, rerr := os.ReadFile(path)
	if rerr == nil && !bytes.Contains(b, []byte("garbage")) && len(b) > 0 {
		// Best-effort GC after the tombstone may have removed it instead;
		// a repaired valid shard is the only forbidden outcome.
		t.Fatalf("stale repair wrote old parity bytes back: %q", b)
	}
	if e.stageFileCount() != 0 {
		t.Fatalf("staged leftovers: %d", e.stageFileCount())
	}
}

// TestOldRepairCannotClobberRebuiltContent interleaves a repair of gen1
// with a delete followed by a same-name recreate. The stale repair must
// be rejected by its guard and the recreated content must remain intact
// across a restart; recovery must never delete shards the recreated
// generation uses.
func TestOldRepairCannotClobberRebuiltContent(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old-one"), 0)
	e.corruptShard(key, gen1, ShardA)

	proceed := make(chan struct{})
	var peer *Store
	hooks := &Hooks{
		BeforeRepairCommit: func(k string, gen uint64) error {
			if gen != gen1 {
				t.Fatalf("repair targets gen %d, want %d", gen, gen1)
			}
			go func() {
				// Delete then immediately recreate under the same name.
				dg, derr := peer.Delete(context.Background(), k, gen1)
				if derr != nil {
					t.Errorf("delete: %v", derr)
					close(proceed)
					return
				}
				if _, perr := peer.Put(context.Background(), k, []byte("brand-new"), dg); perr != nil {
					t.Errorf("recreate: %v", perr)
				}
				close(proceed)
			}()
			<-proceed
			return nil
		},
	}
	repairer := e.openPeer(hooks)
	peer = e.openPeer(nil)

	_, _, err := repairer.Repair(context.Background(), key)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("repair vs rebuild err=%v, want ErrConflict", err)
	}
	gen3 := uint64(gen1 + 2)
	mustGet(t, e.s, key, []byte("brand-new"))
	if _, g, _, err := e.s.Get(context.Background(), key); err != nil || g != gen3 {
		t.Fatalf("rebuilt Get gen=%d err=%v, want %d", g, err, gen3)
	}
	// Recreated shards must all be present and healthy.
	for d := 0; d < NumShards; d++ {
		if !e.shardExistsOnDisk(key, gen3, d) {
			t.Fatalf("current-gen shard %d missing before recovery", d)
		}
	}
	// Restart recovery keeps the current generation's shards (GC must
	// not mistake them for garbage).
	s2 := e.reopen(nil)
	for d := 0; d < NumShards; d++ {
		if !e.shardExistsOnDisk(key, gen3, d) {
			t.Fatalf("recovery removed current-gen shard %d", d)
		}
	}
	got, g, repaired, err := s2.Get(context.Background(), key)
	if err != nil || g != gen3 || len(repaired) != 0 || string(got) != "brand-new" {
		t.Fatalf("post-recovery rebuilt Get = %q gen=%d repaired=%v err=%v", got, g, repaired, err)
	}
}

// TestFailedPutLeavesNoReadableHalfProduct verifies that a Put which
// fails after shards are staged but before publication changes nothing
// readable, and that even a modeled failure at the manifest-rename phase
// rolls its unpublished shards back instead of leaking an unreferenced
// generation that later confuses recreation.
func TestFailedPutLeavesNoReadableHalfProduct(t *testing.T) {
	key := "obj"
	var failOnce sync.Once
	hooks := &Hooks{
		BeforeManifestCommit: func(k string, gen uint64) error {
			if gen != 2 {
				return nil
			}
			var ret error
			failOnce.Do(func() { ret = errors.New("boom") })
			return ret
		},
	}
	e := newTestEnv(t, hooks)
	gen1 := mustPut(t, e.s, key, []byte("v1"), 0)

	// Failing gen2 update.
	if _, err := e.s.Put(context.Background(), key, []byte("v2"), gen1); err == nil {
		t.Fatalf("expected injected Put failure")
	}
	// Old version still the one readable; no gen2 shard files leaked.
	mustGet(t, e.s, key, []byte("v1"))
	for d := 0; d < NumShards; d++ {
		if e.shardExistsOnDisk(key, gen1+1, d) {
			t.Fatalf("failed Put left published gen2 shard on disk %d", d)
		}
	}
	if e.stageFileCount() != 0 {
		t.Fatalf("clean Put failure left staged files: %d", e.stageFileCount())
	}

	// Evidence of a failed update must not block retrying the same update.
	gen2 := mustPut(t, e.s, key, []byte("v2-real"), gen1)
	if gen2 != gen1+1 {
		t.Fatalf("retry gen = %d, want %d", gen2, gen1+1)
	}
	mustGet(t, e.s, key, []byte("v2-real"))

	// Simulated crash at the same boundary keeps on-disk half-state for
	// recovery to sweep; after restart the retry still works.
	crashHooks := &Hooks{
		BeforeManifestCommit: func(k string, gen uint64) error {
			if gen == gen2+1 {
				return ErrSimulatedCrash
			}
			return nil
		},
	}
	s3, err := Open(context.Background(), e.dirs[0], e.dirs[1], e.dirs[2], crashHooks)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s3.Put(context.Background(), key, []byte("v3"), gen2); !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("want simulated crash, got %v", err)
	}
	mustGet(t, s3, key, []byte("v2-real"))
	s4 := e.reopen(nil)
	if e.stageFileCount() != 0 {
		t.Fatalf("staged files after recovery = %d", e.stageFileCount())
	}
	mustGet(t, s4, key, []byte("v2-real"))
	_ = mustPut(t, s4, key, []byte("v3-real"), gen2)
	mustGet(t, s4, key, []byte("v3-real"))
}

// TestRecoveryPreservesLiveShardsAndSingleShardRepair is the
// delete/rebuild regression combined with the pre-existing
// single-shard recovery capability:
//
//   - delete obj, recreate same key at the post-delete generation;
//   - corrupt one shard of the CURRENT generation and restart: recovery
//     must keep the current shards and a read reconstructs+repairs;
//   - an unrelated second object keeps its single-shard reconstruction
//     ability through the restart as well.
func TestRecoveryPreservesLiveShardsAndSingleShardRepair(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "rebuilt"
	other := "untouched"
	g1 := mustPut(t, e.s, key, []byte("old"), 0)
	og1 := mustPut(t, e.s, other, []byte("other-payload"), 0)

	dg, err := e.s.Delete(context.Background(), key, g1)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	g3 := mustPut(t, e.s, key, []byte("fresh"), dg)
	if g3 != dg+1 {
		t.Fatalf("recreate gen = %d", g3)
	}

	// Single-shard fault on the recreated (current) generation.
	e.corruptShard(key, g3, ShardP)
	// Single-shard fault on the unrelated object.
	e.corruptShard(other, og1, ShardB)

	s2 := e.reopen(nil)
	got, g, repaired, err := s2.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get rebuilt after fault: %v", err)
	}
	if g != g3 || string(got) != "fresh" || len(repaired) != 1 || repaired[0] != ShardP {
		t.Fatalf("rebuilt read = %q gen=%d repaired=%v", got, g, repaired)
	}
	got, g, repaired, err = s2.Get(context.Background(), other)
	if err != nil {
		t.Fatalf("Get other after fault: %v", err)
	}
	if g != og1 || string(got) != "other-payload" || len(repaired) != 1 || repaired[0] != ShardB {
		t.Fatalf("other read = %q gen=%d repaired=%v", got, g, repaired)
	}
	// Second read is clean on both, proving on-disk repair completed.
	if _, _, rep, err := s2.Get(context.Background(), key); err != nil || len(rep) != 0 {
		t.Fatalf("second rebuilt read repaired=%v err=%v", rep, err)
	}
	if _, _, rep, err := s2.Get(context.Background(), other); err != nil || len(rep) != 0 {
		t.Fatalf("second other read repaired=%v err=%v", rep, err)
	}
	// Old deleted generation shards were collected; current kept.
	for d := 0; d < NumShards; d++ {
		if e.shardExistsOnDisk(key, g1, d) {
			t.Fatalf("deleted gen %d shard %d survived", g1, d)
		}
		if !e.shardExistsOnDisk(key, g3, d) {
			t.Fatalf("current gen %d shard %d missing", g3, d)
		}
	}
}

// TestBackgroundRepairSkipsDeleted corrupts an object, deletes it, then
// runs the repair loop: the pass must not resurrect the object and must
// return no error for the deleted key; a live object in the same pass is
// still healed.
func TestBackgroundRepairSkipsDeleted(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "gone"
	other := "alive"
	g1 := mustPut(t, e.s, key, []byte("old"), 0)
	og := mustPut(t, e.s, other, []byte("keep-me"), 0)
	e.corruptShard(key, g1, ShardP)
	e.corruptShard(other, og, ShardA)

	if _, err := e.s.Delete(context.Background(), key, g1); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Immediate pass: no error for the deleted key; the live one heals.
	if err := e.s.RunRepairPass(context.Background()); err != nil {
		t.Fatalf("RunRepairPass: %v", err)
	}
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted object served after repair pass: %v", err)
	}
	// The pass must itself have rewritten the corrupt shard on disk; a
	// later Get only confirms the result, it must not be what healed it.
	path := shardPath(e.dirs[ShardA], keyID(other), og, "a")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("healed shard missing: %v", err)
	}
	if bytes.Contains(raw, []byte("garbage")) {
		t.Fatalf("repair pass did not rewrite corrupt shard")
	}
	got, g, _, err := e.s.Get(context.Background(), other)
	if err != nil || string(got) != "keep-me" || g != og {
		t.Fatalf("live object wrong after pass: %q gen=%d err=%v", got, g, err)
	}

	// And the periodic loop over a restart as well.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loop := e.s.StartRepairLoop(ctx, 10*time.Millisecond)
	defer loop.Stop()
	time.Sleep(50 * time.Millisecond)
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("loop resurrected deleted object: %v", err)
	}
}

// TestConcurrentDeleteRecreateAndRepair stress-tests the per-key
// serialization with -race: readers must observe either the old value,
// the new value, or not-found, never garbage or a reused-generation mix.
func TestConcurrentDeleteRecreateAndRepair(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "race"
	gen := mustPut(t, e.s, key, []byte("v0"), 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	// Readers/repairers.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				got, g, _, err := e.s.Get(ctx, key)
				switch {
				case errors.Is(err, ErrNotFound):
				case err != nil:
					return
				default:
					s := string(got)
					if s != "v0" && s != "new" {
						t.Errorf("unexpected value %q at gen %d", got, g)
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			_, _, _ = e.s.Repair(ctx, key)
			time.Sleep(time.Millisecond)
		}
	}()

	// Writer: delete/recreate cycles using the returned generations.
	wg.Add(1)
	go func() {
		defer wg.Done()
		cur := gen
		for j := 0; j < 20; j++ {
			dg, err := e.s.Delete(context.Background(), key, cur)
			if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
				cur = resyncGen(t, e.s, key)
				if cur == 0 {
					continue
				}
				dg, err = e.s.Delete(context.Background(), key, cur)
			}
			if err != nil {
				return
			}
			cur, err = e.s.Put(context.Background(), key, []byte("new"), dg)
			if err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
}

func resyncGen(t *testing.T, s *Store, key string) uint64 {
	t.Helper()
	_, g, _, err := s.Get(context.Background(), key)
	if err != nil {
		return 0
	}
	return g
}

// TestDeleteCLIEndToEnd drives the full lifecycle via the on-disk layout:
// delete -> reopen -> recreate, asserting generations on the manifest
// files themselves.
func TestDeleteTombstoneManifestShape(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("data"), 0)
	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	for d := 0; d < NumShards; d++ {
		if !manifestOnDisk(e, key, d) {
			t.Fatalf("tombstone copy missing on disk %d", d)
		}
		raw := manifestRaw(t, e, key, d)
		var m Manifest
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("tombstone %d: %v", d, err)
		}
		if !m.Deleted || m.Gen != delGen || m.Length != 0 || m.Key != key {
			t.Fatalf("disk %d tombstone = %+v", d, m)
		}
		for i := 0; i < NumShards; i++ {
			if m.Shards[i] != (ShardInfo{}) {
				t.Fatalf("tombstone carries shard info: %+v", m.Shards[i])
			}
		}
	}
	// Deleted gen shards were best-effort collected.
	for d := 0; d < NumShards; d++ {
		if e.shardExistsOnDisk(key, gen1, d) {
			t.Fatalf("deleted shard %d still present", d)
		}
	}
}
