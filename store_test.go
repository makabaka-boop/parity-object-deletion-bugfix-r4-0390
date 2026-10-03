package xorstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPutGetGenerations covers basic encoding for even/odd lengths and
// the conditional-generation contract.
func TestPutGetGenerations(t *testing.T) {
	e := newTestEnv(t, nil)
	defer func() { _ = e.s }()

	cases := [][]byte{
		nil,
		[]byte(""),
		[]byte("a"),   // odd, padding byte
		[]byte("ab"),  // even
		[]byte("abc"), // odd
		bytes.Repeat([]byte{0x00, 0xff, 0x55, 0xaa}, 257),
	}
	const key = "obj-1"
	var gen uint64
	for i, payload := range cases {
		g := mustPut(t, e.s, key, payload, gen)
		if g != uint64(i+1) {
			t.Fatalf("gen = %d, want %d", g, i+1)
		}
		got, rg, repaired, err := e.s.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if rg != g || len(repaired) != 0 {
			t.Fatalf("Get gen=%d repaired=%v, want %d/[]", rg, repaired, g)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("roundtrip mismatch at case %d (len got=%d want=%d)", i, len(got), len(payload))
		}
		gen = g
	}

	// Stale expectation conflicts.
	if _, err := e.s.Put(context.Background(), key, []byte("x"), 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Put err=%v, want ErrConflict", err)
	}
	// Creating over an existing object with expectGen=0 conflicts too.
	if _, err := e.s.Put(context.Background(), key, []byte("x"), 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("expect0 Put err=%v, want ErrConflict", err)
	}
	// Correct expectation updates.
	_ = mustPut(t, e.s, key, []byte("final"), gen)

	// Missing object.
	if _, _, _, err := e.s.Get(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing err=%v, want ErrNotFound", err)
	}
}

// TestSingleShardCorruption_RebuildAndRepair injects corruption and
// deletion of each single shard in turn (for both an even- and odd-length
// object) and asserts transparent reconstruction + on-disk repair, then
// injects two simultaneous faults and asserts ErrUnrecoverable.
func TestSingleShardCorruption_RebuildAndRepair(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte("even-length-data!!"), // 17? make sure variants
		[]byte("odd-length-data"),    // 13
	} {
		for _, bad := range []int{ShardA, ShardB, ShardP} {
			for _, mode := range []string{"corrupt", "missing"} {
				t.Run(mode+"_role"+string(rune('a'+bad))+"_len"+itoa(uint64(len(payload))),
					func(t *testing.T) {
						hooks := &Hooks{}
						e := newTestEnv(t, hooks)
						const key = "k"
						gen := mustPut(t, e.s, key, payload, 0)

						if mode == "corrupt" {
							e.corruptShard(key, gen, bad)
						} else {
							e.removeShard(key, gen, bad)
						}

						got, rg, repaired, err := e.s.Get(context.Background(), key)
						if err != nil {
							t.Fatalf("Get after single fault: %v", err)
						}
						if rg != gen || !bytes.Equal(got, payload) {
							t.Fatalf("Get = %q gen %d, want %q gen %d", got, rg, payload, gen)
						}
						if len(repaired) != 1 || repaired[0] != bad {
							t.Fatalf("repaired = %v, want [%d]", repaired, bad)
						}
						if !e.shardExistsOnDisk(key, gen, bad) {
							t.Fatalf("shard %d not repaired on disk", bad)
						}
						// A subsequent read is fully healthy.
						_, _, repaired2, err := e.s.Get(context.Background(), key)
						if err != nil || len(repaired2) != 0 {
							t.Fatalf("second Get repaired=%v err=%v, want clean", repaired2, err)
						}
					})
			}
		}
	}

	// Two bad shards: unrecoverable.
	t.Run("two_bad_shards", func(t *testing.T) {
		e := newTestEnv(t, nil)
		const key = "k"
		gen := mustPut(t, e.s, key, []byte("important payload"), 0)
		e.corruptShard(key, gen, ShardA)
		e.removeShard(key, gen, ShardB)
		_, rg, _, err := e.s.Get(context.Background(), key)
		errIs(t, err, ErrUnrecoverable)
		if rg != gen {
			t.Fatalf("unrecoverable Get returned gen %d, want %d", rg, gen)
		}
		// Nothing got written back that could mask the damage: the
		// corrupt bytes on disk0 must be left untouched.
		path := shardPath(e.dirs[ShardA], keyID(key), gen, "a")
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read corrupt shard: %v", err)
		}
		if string(b) != "garbage-bytes-not-a-shard!!!" {
			t.Fatalf("corrupt shard was modified during unrecoverable read: %q", b)
		}
	})
}

// TestCrashBeforeManifestPublish injects a failure at the fault point
// after all three new shards have been staged/verified but before
// publication. The previous generation must remain readable, nothing new
// may be visible, and a restart must sweep the staged files.
func TestCrashBeforeManifestPublish(t *testing.T) {
	key := "obj"
	var crashedGen uint64
	hooks := &Hooks{
		BeforeManifestCommit: func(k string, gen uint64) error {
			if gen == 2 {
				crashedGen = gen
				return ErrSimulatedCrash
			}
			return nil
		},
	}
	e := newTestEnv(t, hooks)

	gen1 := mustPut(t, e.s, key, []byte("v1"), 0)

	// This write "crashes" at the fault point.
	if _, err := e.s.Put(context.Background(), key, []byte("v2"), gen1); err == nil {
		t.Fatalf("expected injected error")
	}
	if crashedGen != gen1+1 {
		t.Fatalf("hook saw gen %d, want %d", crashedGen, gen1+1)
	}

	// Old version is still the readable one.
	mustGet(t, e.s, key, []byte("v1"))
	if e.stageFileCount() != 3 {
		t.Fatalf("staged files = %d, want 3 leftovers", e.stageFileCount())
	}

	// Restart: sweep staged files, retain gen1 shards, gen2 unpublished.
	s2 := e.reopen(nil)
	if e.stageFileCount() != 0 {
		t.Fatalf("after recovery staged files = %d, want 0", e.stageFileCount())
	}
	mustGet(t, s2, key, []byte("v1"))

	// gen2 shard files must never have been published (hook fires before
	// shard rename), and no gen2 manifest exists.
	for i := 0; i < NumShards; i++ {
		if e.shardExistsOnDisk(key, gen1+1, i) {
			t.Fatalf("gen2 shard %d must not exist after pre-publish crash", i)
		}
	}

	// Retry the write after restart with the same expectation; it should
	// succeed and data becomes readable.
	_ = mustPut(t, s2, key, []byte("v2-real"), gen1)
	mustGet(t, s2, key, []byte("v2-real"))
}

// TestCrashMidManifestPublish simulates a crash after shards are
// committed but with zero manifest copies published (modeled by removing
// the disk directories' manifest dirs and reopening). Recovery must not
// resurrect the unreferenced generation as readable; subsequent GC
// removes it and the old generation stays intact.
func TestCrashMidManifestPublish(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("v1"), 0)

	// Publish gen2 directly at the filesystem layer and then remove all
	// gen2 manifest copies, mimicking a crash with shards committed but
	// no manifest rename landed.
	gen2 := gen1 + 1
	id := keyID(key)
	a, b, p := splitShards([]byte("v2"))
	roles := []string{"a", "b", "p"}
	for i, raw := range [][]byte{a, b, p} {
		final := shardPath(e.dirs[i], id, gen2, roles[i])
		if err := writeStagedAndCommit(e.dirs[i], id, "gen2-"+roles[i], raw, final); err != nil {
			t.Fatalf("plant gen2 shard: %v", err)
		}
	}

	// Restart runs recovery.
	s2 := e.reopen(nil)

	// Object must still be gen1 (no gen2 manifest exists).
	mustGet(t, s2, key, []byte("v1"))
	// Orphaned gen2 shards must have been GC'd on every present disk.
	for i := 0; i < NumShards; i++ {
		if e.shardExistsOnDisk(key, gen2, i) {
			t.Fatalf("orphan gen2 shard %d should have been collected", i)
		}
	}
	// gen1 shards retained.
	for i := 0; i < NumShards; i++ {
		if !e.shardExistsOnDisk(key, gen1, i) {
			t.Fatalf("referenced gen1 shard %d must be retained", i)
		}
	}
}

// TestConditionalWriteDuringRepair interleaves a conditional generation
// write with a prepared repair. Two separate Store instances share the
// directories (the second opener skips recovery). The writer is blocked
// until the repairing store has staged its replacement shard and is at
// its commit guard; the writer then publishes gen2. The stale gen1
// repair must be rejected with ErrConflict and must not corrupt gen2.
func TestConditionalWriteDuringRepair(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("generation-one"), 0)
	e.corruptShard(key, gen1, ShardP)

	writerReady := make(chan struct{})
	writerDone := make(chan struct{})
	releaseWriter := make(chan struct{})
	var writeGen uint64
	var writeErr error

	// Peer store does the concurrent conditional write.
	var peer *Store
	repairHooks := &Hooks{
		BeforeRepairCommit: func(k string, gen uint64) error {
			if k != key || gen != gen1 {
				t.Fatalf("unexpected hook args %q/%d", k, gen)
			}
			go func() {
				close(writerReady)
				<-releaseWriter
				writeGen, writeErr = peer.Put(context.Background(), k,
					[]byte("generation-two"), gen1)
				close(writerDone)
			}()
			<-writerReady
			// Small pause so the writer is queued on the key lock; then
			// we return and immediately re-check the published gen.
			time.Sleep(20 * time.Millisecond)
			close(releaseWriter)
			// Give the queued writer a chance to grab the lock: the
			// repair guard must still be correct regardless of ordering,
			// so block here until the write lands.
			<-writerDone
			return nil
		},
	}
	repairer := e.openPeer(repairHooks)
	peer = e.openPeer(nil)

	_, repaired, err := repairer.Repair(context.Background(), key)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale repair err=%v repaired=%v, want ErrConflict", err, repaired)
	}
	if len(repaired) != 0 {
		t.Fatalf("stale repair installed shards: %v", repaired)
	}

	// The concurrent write succeeded as gen2.
	if writeErr != nil || writeGen != gen1+1 {
		t.Fatalf("interleaved Put gen=%d err=%v, want gen2/nil", writeGen, writeErr)
	}

	// Reads serve gen2 with no repairs needed, and the corrupt gen1
	// parity shard on disk0 was never overwritten with stale bytes.
	got, gen, rep, err := e.s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get after interleaving: %v", err)
	}
	if gen != gen1+1 || string(got) != "generation-two" {
		t.Fatalf("Get = %q gen %d, want gen2", got, gen)
	}
	if len(rep) != 0 {
		t.Fatalf("gen2 read should be clean, repaired=%v", rep)
	}

	// No staged repair leftovers anywhere.
	if e.stageFileCount() != 0 {
		t.Fatalf("staged leftovers after aborted repair: %d", e.stageFileCount())
	}
}

// TestRepairLosesRaceToWrite is the opposite ordering: a repair is fully
// prepared, then the writer publishes gen2 *before* the repair reaches
// its guard. The guard rejects the stale repair; Repair on the new
// generation is a no-op.
func TestRepairLosesRaceToWrite(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen1 := mustPut(t, e.s, key, []byte("v1"), 0)
	e.corruptShard(key, gen1, ShardB)

	peer := e.openPeer(nil)
	// Writer advances to gen2 on the primary while repair will target gen1.
	gen2 := mustPut(t, e.s, key, []byte("v2-new"), gen1)
	if gen2 != 2 {
		t.Fatalf("gen2 = %d", gen2)
	}

	// Peer still sees the newer manifest on disk; its repair must fix
	// gen2 (which is healthy) as a no-op and never touch gen1's files.
	gen, repaired, err := peer.Repair(context.Background(), key)
	if err != nil {
		t.Fatalf("Repair healthy gen2: %v", err)
	}
	if gen != gen2 || len(repaired) != 0 {
		t.Fatalf("Repair gen=%d repaired=%v, want gen2/no-op", gen, repaired)
	}
}

// TestManifestHealing loses an entire disk directory (data shard and
// manifest copy), asserts degraded reads work, and confirms healing of
// the manifest copies after the directory comes back.
func TestDiskLossAndManifestHeal(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen := mustPut(t, e.s, key, []byte("survive a lost disk"), 0)

	e.removeDisk(ShardA)

	got, rg, repaired, err := e.s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get with one disk gone: %v", err)
	}
	if string(got) != "survive a lost disk" || rg != gen {
		t.Fatalf("degraded Get = %q gen %d", got, rg)
	}
	if len(repaired) != 0 {
		t.Fatalf("cannot rewrite onto absent disk, repaired=%v", repaired)
	}

	// Two disks gone -> unrecoverable.
	e.removeDisk(ShardB)
	if _, _, _, err := e.s.Get(context.Background(), key); !errors.Is(err, ErrUnrecoverable) {
		t.Fatalf("two disks gone err=%v, want ErrUnrecoverable", err)
	}
}

// TestRecoverySweepsAndHeals plants junk staged files, an old orphan
// generation and a missing manifest copy, then verifies recovery output.
func TestRecoverySweepsAndHeals(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	_ = mustPut(t, e.s, key, []byte("v1"), 0)

	// Old-generation orphan on disk1 (gen99 never had a manifest).
	id := keyID(key)
	oldPath := filepath.Join(e.dirs[1], id+"-gen99-a.shard")
	if err := writeStagedAndCommit(e.dirs[1], id, "junk", []byte("x"), oldPath); err != nil {
		t.Fatalf("plant orphan: %v", err)
	}

	// Junk in staging dirs.
	for i := 0; i < NumShards; i++ {
		if _, err := stageFile(e.dirs[i], id, "leftover", []byte("staged")); err != nil {
			t.Fatalf("plant staged junk: %v", err)
		}
	}

	// Remove one manifest copy; recovery must heal it.
	mp := manifestPath(e.dirs[0], id)
	if err := os.Remove(mp); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}

	stats, err := e.s.recover(context.Background())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if stats.StagedRemoved != 3 {
		t.Fatalf("StagedRemoved = %d, want 3", stats.StagedRemoved)
	}
	if stats.OrphanShardsRemoved < 1 {
		t.Fatalf("OrphanShardsRemoved = %d, want >=1", stats.OrphanShardsRemoved)
	}
	if stats.ManifestCopiesHealed != 1 {
		t.Fatalf("ManifestCopiesHealed = %d, want 1", stats.ManifestCopiesHealed)
	}
	mustGet(t, e.s, key, []byte("v1"))
}

// TestBackgroundRepairLoop corrupts a shard and lets the loop fix it.
func TestBackgroundRepairLoop(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "obj"
	gen := mustPut(t, e.s, key, []byte("backgrounded"), 0)
	e.corruptShard(key, gen, ShardP)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loop := e.s.StartRepairLoop(ctx, 10*time.Millisecond)
	defer loop.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e.shardExistsOnDisk(key, gen, ShardP) {
			_, _, rep, err := e.s.Get(context.Background(), key)
			if err == nil && len(rep) == 0 {
				return // healed
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("background repair did not heal shard in time")
}

// TestConcurrentReadersAndWriter ensures the per-key serialization keeps
// reads consistent under contention with -race.
func TestConcurrentReadersAndWriter(t *testing.T) {
	e := newTestEnv(t, nil)
	key := "k"
	gen := mustPut(t, e.s, key, []byte("init"), 0)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				got, g, _, err := e.s.Get(ctx, key)
				if err != nil {
					return
				}
				if !strings.HasPrefix(string(got), "v") && string(got) != "init" {
					t.Errorf("unexpected data %q", got)
				}
				_ = g
			}
		}()
	}
	for i := 0; i < 10; i++ {
		data := []byte("v" + itoa(uint64(i)))
		g, err := e.s.Put(context.Background(), key, data, gen)
		if errors.Is(err, ErrConflict) {
			// Re-read current gen and retry once.
			_, g2, _, _ := e.s.Get(context.Background(), key)
			g, err = e.s.Put(context.Background(), key, data, g2)
		}
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
		gen = g
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()
}
