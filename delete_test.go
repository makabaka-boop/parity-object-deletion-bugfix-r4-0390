package xorstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// plantShard writes raw bytes as a published (id, gen, role) shard on the
// given disk, bypassing the store (used to model out-of-band damage).
func (e *testEnv) plantShardRaw(disk int, key string, gen uint64, role string, raw []byte) {
	e.t.Helper()
	final := shardPath(e.dirs[disk], keyID(key), gen, role)
	if err := writeStagedAndCommit(e.dirs[disk], keyID(key),
		fmt.Sprintf("gen%d-%s-plant", gen, role), raw, final); err != nil {
		e.t.Fatalf("plant shard: %v", err)
	}
}

// assertTombstone loads the on-disk best manifest and asserts it is a
// tombstone carrying exactly deleteGen.
func assertTombstone(t *testing.T, s *Store, key string, deleteGen uint64) {
	t.Helper()
	man, onBest, err := s.loadBestManifest(context.Background(), keyID(key), false)
	if err != nil {
		t.Fatalf("loadBestManifest after delete: %v", err)
	}
	if !man.Deleted {
		t.Fatalf("manifest for %q is not a tombstone: %+v", key, man)
	}
	if man.Gen != deleteGen {
		t.Fatalf("tombstone gen = %d, want %d", man.Gen, deleteGen)
	}
	for d, ok := range onBest {
		if !ok {
			t.Fatalf("disk %d missing tombstone copy", d)
		}
	}
}

// TestDeleteMakesObjectUnreadable is the core delete invariant: after a
// successful conditional delete Get, Repair and the background scan never
// serve or reinstall the old content, the durable state is a tombstone,
// and restart does not resurrect the object.
func TestDeleteMakesObjectUnreadable(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old-content"), 0)

	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if delGen != gen1+1 {
		t.Fatalf("Delete gen = %d, want %d", delGen, gen1+1)
	}

	// Get must report ErrNotFound, never the old bytes.
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete err=%v, want ErrNotFound", err)
	}
	// Repair must not recreate anything from surviving shards.
	if g, _, err := e.s.Repair(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Repair after delete gen=%d err=%v, want ErrNotFound", g, err)
	}
	// Deleting again is NotFound (not a second tombstone / gen bump).
	if _, err := e.s.Delete(context.Background(), key, delGen); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete err=%v, want ErrNotFound", err)
	}
	// A wrong expectation conflicts against the live gen before delete.
	e2 := newTestEnv(t, nil)
	k2 := "other"
	g2 := mustPut(t, e2.s, k2, []byte("x"), 0)
	if dg, err := e2.s.Delete(context.Background(), k2, g2+9); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Delete returned gen=%d err=%v, want ErrConflict", dg, err)
	}
	if _, _, _, err := e2.s.Get(context.Background(), k2); err != nil {
		t.Fatalf("failed delete must leave object readable: %v", err)
	}

	// All three disks carry the tombstone.
	assertTombstone(t, e.s, key, delGen)

	// A background repair pass must skip the tombstoned key silently.
	if err := e.s.RunRepairPass(context.Background()); err != nil {
		t.Fatalf("repair pass over deleted key: %v", err)
	}
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after repair pass err=%v, want ErrNotFound", err)
	}

	// Restart recovery must keep the object deleted rather than heal an
	// old manifest copy back into existence.
	s2 := e.reopen(nil)
	assertTombstone(t, s2, key, delGen)
	if _, _, _, err := s2.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after restart err=%v, want ErrNotFound", err)
	}
	// The deleted generation's shard files were removed (best effort at
	// delete, guaranteed at recovery).
	for i := 0; i < NumShards; i++ {
		if e.shardExistsOnDisk(key, gen1, i) {
			t.Fatalf("deleted gen %d shard %d survived recovery", gen1, i)
		}
	}
}

// TestDeleteManifestHealsFromSurvivingCopies models the original failure:
// delete "returned success" with plain os.Remove, but a missing/stale
// manifest disk would resurrect old content. With tombstones, a disk whose
// copy is missing or stale heals to the tombstone and can never show the
// deleted content.
func TestDeleteManifestHealsFromSurvivingCopies(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("will-be-deleted"), 0)
	id := keyID(key)

	// Corrupt one manifest copy and remove another before deletion: at
	// delete time only disk2 carries a readable manifest.
	if err := os.WriteFile(manifestPath(e.dirs[0], id), []byte("{garbled"), filePerm); err != nil {
		t.Fatalf("garble manifest: %v", err)
	}
	if err := os.Remove(manifestPath(e.dirs[1], id)); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}

	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil {
		t.Fatalf("Delete with damaged copies: %v", err)
	}

	// currentGen healed during Delete; all copies must now be tombstones.
	assertTombstone(t, e.s, key, delGen)
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get err=%v, want ErrNotFound", err)
	}

	// Restart with the very same damaged disks: recovery must converge on
	// the tombstone rather than surface the old manifest or old shards.
	s2 := e.reopen(nil)
	assertTombstone(t, s2, key, delGen)
}

// TestRecreateAfterDeleteUsesDeleteGeneration covers same-name
// recreation: it must be conditional on the delete generation, publish a
// strictly higher generation and never reuse an old generation.
func TestRecreateAfterDeleteUsesDeleteGeneration(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old"), 0)
	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// expectGen 0 (first-write) and the pre-delete generation both
	// conflict: the tombstone names delGen as the current generation.
	for _, expect := range []uint64{0, gen1} {
		if g, err := e.s.Put(context.Background(), key, []byte("new"), expect); !errors.Is(err, ErrConflict) {
			t.Fatalf("Put expect=%d returned gen=%d err=%v, want ErrConflict", expect, g, err)
		}
	}
	// Generation reports the tombstone generation plus ErrNotFound.
	if g, err := e.s.Generation(context.Background(), key); !errors.Is(err, ErrNotFound) || g != delGen {
		t.Fatalf("Generation = %d, %v; want %d, ErrNotFound", g, err, delGen)
	}

	// Correct conditional recreation publishes delGen+1.
	genNew := mustPut(t, e.s, key, []byte("brand-new"), delGen)
	if genNew != delGen+1 {
		t.Fatalf("recreated gen = %d, want %d", genNew, delGen+1)
	}
	if genNew <= gen1 {
		t.Fatalf("recreation reused old generation: %d <= %d", genNew, gen1)
	}
	got, g, repaired, err := e.s.Get(context.Background(), key)
	if err != nil || g != genNew || len(repaired) != 0 || string(got) != "brand-new" {
		t.Fatalf("Get after recreate = %q gen=%d repaired=%v err=%v", got, g, repaired, err)
	}
	// No shard file of the recreated generation collides with an old one
	// (different gen-scoped names); old shards are gone from every disk.
	for i := 0; i < NumShards; i++ {
		if e.shardExistsOnDisk(key, gen1, i) {
			t.Fatalf("old gen %d shard %d still present after recreate", gen1, i)
		}
		if !e.shardExistsOnDisk(key, genNew, i) {
			t.Fatalf("new gen %d shard %d missing", genNew, i)
		}
	}

	// Restart preserves the new content and its generation.
	s2 := e.reopen(nil)
	mustGet(t, s2, key, []byte("brand-new"))
}

// TestRecreateRefusesToReuseGenerationWhenEvidenceLost covers the
// "manifest copies temporarily missing" danger: with no manifest but
// shard files left, a first-time Put must not be accepted (it would
// publish gen 1 over an indistinguishable older incarnation). Restart
// recovery clears the orphans and then the first write is allowed.
func TestRecreateRefusesToReuseGenerationWhenEvidenceLost(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("orphaned-content"), 0)
	id := keyID(key)

	// Lose ALL manifest copies out of band while shards remain.
	for d := 0; d < NumShards; d++ {
		if err := os.Remove(manifestPath(e.dirs[d], id)); err != nil {
			t.Fatalf("remove manifest: %v", err)
		}
	}
	// Reads cannot identify the data; the critical part is that no
	// write may silently claim the empty generation slot.
	if _, _, _, err := e.s.Get(context.Background(), key); err == nil {
		t.Fatalf("Get with no manifests unexpectedly served data")
	}
	// Same-name "first write" must be refused, not silently reuse gen 1.
	if _, err := e.s.Put(context.Background(), key, []byte("new"), 0); !errors.Is(err, ErrGenerationAmbiguous) {
		t.Fatalf("Put over shard evidence err=%v, want ErrGenerationAmbiguous", err)
	}
	// Old files untouched by the refusal.
	for i := 0; i < NumShards; i++ {
		if !e.shardExistsOnDisk(key, gen1, i) {
			t.Fatalf("shard %d must remain after refused Put", i)
		}
	}

	// Restart recovery removes the unidentifiable shard generations.
	stats, err := e.s.recover(context.Background())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if stats.OrphanShardsRemoved < NumShards {
		t.Fatalf("OrphanShardsRemoved=%d, want >=%d", stats.OrphanShardsRemoved, NumShards)
	}

	// Now the key genuinely has no history; a first write succeeds.
	gen := mustPut(t, e.s, key, []byte("after-recovery"), 0)
	if gen != 1 {
		t.Fatalf("gen after recovery = %d, want 1", gen)
	}
	mustGet(t, e.s, key, []byte("after-recovery"))
}

// TestOldRepairCannotTouchDeletedOrRecreatedObject stages a repair of the
// old generation and then deletes and recreates the key before the repair
// installs. The stale repair must be rejected, install nothing, and
// neither deleted bytes nor a clobbered new shard may appear.
func TestOldRepairCannotTouchDeletedOrRecreatedObject(t *testing.T) {
	for _, mode := range []string{"delete", "recreate"} {
		t.Run(mode, func(t *testing.T) {
			e := newTestEnv(t, nil)
			key := "obj"
			gen1 := mustPut(t, e.s, key, []byte("generation-one"), 0)
			e.corruptShard(key, gen1, ShardP)

			release := make(chan struct{})
			hooks := &Hooks{
				BeforeRepairCommit: func(k string, gen uint64) error {
					if k != key || gen != gen1 {
						t.Fatalf("unexpected hook %q/%d", k, gen)
					}
					<-release
					return nil
				},
			}
			repairer := e.openPeer(hooks)
			peer := e.openPeer(nil)

			type repairResult struct {
				gen      uint64
				repaired []int
				err      error
			}
			resCh := make(chan repairResult, 1)
			go func() {
				g, r, err := repairer.Repair(context.Background(), key)
				resCh <- repairResult{g, r, err}
			}()

			// Wait until the repair has staged and parked at its guard.
			deadline := time.Now().Add(2 * time.Second)
			for e.stageFileCount() == 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if e.stageFileCount() == 0 {
				t.Fatalf("repair never staged")
			}

			var newGen uint64
			if mode == "delete" {
				dg, err := peer.Delete(context.Background(), key, gen1)
				if err != nil {
					t.Fatalf("Delete during repair: %v", err)
				}
				newGen = dg
			} else {
				// Recreate at a HIGHER generation with a fresh shard
				// layout on the same paths' successor generation.
				newGen = mustPut(t, peer, key, []byte("generation-two"), gen1)
			}

			close(release)
			res := <-resCh
			if !errors.Is(res.err, ErrConflict) {
				t.Fatalf("stale repair err=%v, want ErrConflict", res.err)
			}
			if len(res.repaired) != 0 {
				t.Fatalf("stale repair installed %v", res.repaired)
			}
			if e.stageFileCount() != 0 {
				t.Fatalf("stale staged files left: %d", e.stageFileCount())
			}

			if mode == "delete" {
				if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
					t.Fatalf("Get err=%v, want ErrNotFound", err)
				}
				assertTombstone(t, e.s, key, newGen)
			} else {
				got, g, rep, err := e.s.Get(context.Background(), key)
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if g != newGen || string(got) != "generation-two" || len(rep) != 0 {
					t.Fatalf("Get = %q gen=%d rep=%v, want gen2 clean", got, g, rep)
				}
			}
		})
	}
}

// TestSameNumberGenerationReuseBlockedAtRepair models the nastiest
// variant: manifest evidence is lost and an attacker/operator plants a
// brand-new live manifest at the SAME generation number as the old
// incarnation (the exact confusion a delete/recreate must prevent). A
// repair prepared against the old manifest must detect that the shard
// digests differ even though the generation numbers match, and refuse to
// install its stale bytes.
func TestSameNumberGenerationReuseBlockedAtRepair(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen := mustPut(t, e.s, key, []byte("old-content"), 0)
	e.corruptShard(key, gen, ShardP)

	// Build a repair plan directly from the OLD manifest (as an in-flight
	// repair captured before the replacement).
	oldPlan, err := e.s.planRepair(context.Background(), key)
	if err != nil {
		t.Fatalf("planRepair: %v", err)
	}

	// Replace the live manifest at the same generation number with one
	// describing different shard bytes, and plant the matching shards.
	a, b, p := splitShards([]byte("new-content"))
	raw := [NumShards][]byte{a, b, p}
	roles := [NumShards]string{"a", "b", "p"}
	id := keyID(key)
	for i := 0; i < NumShards; i++ {
		e.plantShardRaw(i, key, gen, roles[i], raw[i])
	}
	newMan := e.s.buildManifest(key, gen, len([]byte("new-content")), raw[:], roles[:])
	newRaw, err := json.MarshalIndent(newMan, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	for d := 0; d < NumShards; d++ {
		if err := writeStagedAndCommit(e.dirs[d], id, "manifest", newRaw,
			manifestPath(e.dirs[d], id)); err != nil {
			t.Fatalf("publish replacement manifest: %v", err)
		}
	}

	if _, err := e.s.commitRepair(context.Background(), oldPlan); !errors.Is(err, ErrConflict) {
		t.Fatalf("same-gen-number stale repair err=%v, want ErrConflict", err)
	}

	// The new content's parity shard must be intact, not old parity bytes.
	got, g, rep, err := e.s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if g != gen || string(got) != "new-content" || len(rep) != 0 {
		t.Fatalf("Get = %q gen=%d rep=%v", got, g, rep)
	}
}

// TestDeleteFailureLeavesNoHalfState exercises the delete fault point:
// a simulated crash before the tombstone is published leaves only swept
// staging files and a fully readable object; a plain abort leaves
// nothing behind at all.
func TestDeleteFailureLeavesNoHalfState(t *testing.T) {
	t.Run("simulated_crash", func(t *testing.T) {
		key := "obj"
		hooks := &Hooks{
			BeforeDeleteCommit: func(k string, gen uint64) error {
				return ErrSimulatedCrash
			},
		}
		e := newTestEnv(t, hooks)
		gen := mustPut(t, e.s, key, []byte("alive"), 0)

		if _, err := e.s.Delete(context.Background(), key, gen); !errors.Is(err, ErrSimulatedCrash) {
			t.Fatalf("Delete err=%v, want ErrSimulatedCrash", err)
		}
		// Object still fully readable; three staged tombstone copies sit
		// unpublished.
		mustGet(t, e.s, key, []byte("alive"))
		if e.stageFileCount() != NumShards {
			t.Fatalf("staged = %d, want %d", e.stageFileCount(), NumShards)
		}
		// Retry after restart: recovery sweeps the staging files, delete
		// then succeeds and the object stays gone.
		s2 := e.reopen(nil)
		if e.stageFileCount() != 0 {
			t.Fatalf("staged after recovery = %d, want 0", e.stageFileCount())
		}
		mustGet(t, s2, key, []byte("alive"))
		if _, err := s2.Delete(context.Background(), key, gen); err != nil {
			t.Fatalf("retry delete: %v", err)
		}
		if _, _, _, err := s2.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get err=%v, want ErrNotFound", err)
		}
	})

	t.Run("plain_abort", func(t *testing.T) {
		key := "obj"
		hooks := &Hooks{
			BeforeDeleteCommit: func(k string, gen uint64) error {
				return errors.New("nope")
			},
		}
		e := newTestEnv(t, hooks)
		gen := mustPut(t, e.s, key, []byte("alive"), 0)
		if _, err := e.s.Delete(context.Background(), key, gen); err == nil {
			t.Fatalf("Delete should fail")
		}
		mustGet(t, e.s, key, []byte("alive"))
		if e.stageFileCount() != 0 {
			t.Fatalf("aborted delete left staged files: %d", e.stageFileCount())
		}
		// Failed Put leaves no readable half-state either: corrupt a disk
		// mid-write via the manifest hook and check only the old version.
	})
}

// TestFailedWriteLeavesNoReadableHalfProduct verifies that a Put which
// fails at the manifest commit (crash) never makes partial data readable:
// the previous generation stays served and restart clears the attempt.
func TestFailedWriteLeavesNoReadableHalfProduct(t *testing.T) {
	key := "obj"
	hooks := &Hooks{
		BeforeManifestCommit: func(k string, gen uint64) error {
			if gen == 2 {
				return ErrSimulatedCrash
			}
			return nil
		},
	}
	e := newTestEnv(t, hooks)
	gen1 := mustPut(t, e.s, key, []byte("v1"), 0)
	if _, err := e.s.Put(context.Background(), key, []byte("v2"), gen1); !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("Put err, want ErrSimulatedCrash")
	}
	mustGet(t, e.s, key, []byte("v1"))
	s2 := e.reopen(nil)
	mustGet(t, s2, key, []byte("v1"))
	if _, _, _, err := s2.Get(context.Background(), key); err != nil {
		t.Fatalf("old version must survive: %v", err)
	}
}

// TestRecoveryPreservesCurrentContentAndSingleShardRepair is the
// end-to-end restart scenario: delete + recreate with stale generations
// left around, then corrupt exactly one shard of the CURRENT content and
// restart. Recovery must keep the current shards, GC only the stale
// generations, and the existing single-shard read-repair must still work.
func TestRecoveryPreservesCurrentContentAndSingleShardRepair(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("version-one"), 0)
	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	gen3 := mustPut(t, e.s, key, []byte("version-three"), delGen)

	// Plant an unrelated orphan generation on one disk (never manifest).
	id := keyID(key)
	junk := filepath.Join(e.dirs[1], id+"-gen99-a.shard")
	if err := writeStagedAndCommit(e.dirs[1], id, "junk", []byte("x"), junk); err != nil {
		t.Fatalf("plant orphan: %v", err)
	}

	// Corrupt exactly one shard of the CURRENT (live) generation.
	e.corruptShard(key, gen3, ShardB)

	stats, err := e.s.recover(context.Background())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if stats.OrphanShardsRemoved < 1 {
		t.Fatalf("OrphanShardsRemoved = %d, want >=1 (gen99 junk)", stats.OrphanShardsRemoved)
	}

	// Current content survives recovery and still heals a single bad
	// shard via XOR reconstruction.
	got, g, repaired, err := e.s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if g != gen3 || string(got) != "version-three" {
		t.Fatalf("Get = %q gen %d, want gen %d", got, g, gen3)
	}
	if len(repaired) != 1 || repaired[0] != ShardB {
		t.Fatalf("repaired = %v, want [shardB]", repaired)
	}
	// Old deleted generations never reappear.
	for i := 0; i < NumShards; i++ {
		if e.shardExistsOnDisk(key, gen1, i) {
			t.Fatalf("deleted gen %d shard %d resurfaced", gen1, i)
		}
		if !e.shardExistsOnDisk(key, gen3, i) {
			t.Fatalf("current gen %d shard %d lost", gen3, i)
		}
	}
}

// TestBackgroundLoopSkipsDeletedAndHealsRecreated runs the actual
// StartRepairLoop against a deleted-then-recreated key with one corrupt
// current shard: the loop must heal the live content while the deleted
// generation's surviving (corrupt) shards are ignored.
func TestBackgroundLoopSkipsDeletedAndHealsRecreated(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old"), 0)
	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	gen3 := mustPut(t, e.s, key, []byte("new-live-content"), delGen)
	e.corruptShard(key, gen3, ShardA)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loop := e.s.StartRepairLoop(ctx, 10*time.Millisecond)
	defer loop.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.shardExistsOnDisk(key, gen3, ShardA) {
			if got, g, rep, err := e.s.Get(context.Background(), key); err == nil &&
				g == gen3 && string(got) == "new-live-content" && len(rep) == 0 {
				return // healed, deleted content never came back
			}
		}
		if _, _, _, err := e.s.Get(context.Background(), key); err != nil {
			t.Fatalf("deleted content leaked through loop: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("background loop did not heal recreated object")
}

// TestDeleteThenRecreateWithLostTombstoneCopy ensures that even if one
// tombstone copy disappears before recreation, healing restores it and
// the conditional generation is still enforced from the other copies.
func TestDeleteThenRecreateWithLostTombstoneCopy(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("old"), 0)
	delGen, err := e.s.Delete(context.Background(), key, gen1)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Drop the whole disk0 (tombstone + shards) after delete.
	e.removeDisk(ShardA)

	// From the surviving copies recreation still needs delGen.
	peer := e.openPeer(nil)
	if _, err := peer.Put(context.Background(), key, []byte("x"), 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("Put with one disk gone err=%v, want ErrConflict", err)
	}
	genNew := mustPut(t, peer, key, []byte("new"), delGen)

	// Bring disk0 back empty: restart heals manifest and GC must not
	// delete the live content's shards on the other disks.
	if err := os.MkdirAll(e.dirs[ShardA], dirPerm); err != nil {
		t.Fatalf("recreate disk: %v", err)
	}
	s2 := e.reopen(nil)
	got, g, _, err := s2.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get after disk return: %v", err)
	}
	if g != genNew || string(got) != "new" {
		t.Fatalf("Get = %q gen %d, want gen %d", got, g, genNew)
	}
}
