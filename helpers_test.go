package xorstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// testEnv is a store backed by three temp directories.
type testEnv struct {
	t       *testing.T
	dirs    [NumShards]string
	s       *Store
	rootTmp string
}

func newTestEnv(t *testing.T, hooks *Hooks) *testEnv {
	t.Helper()
	root := t.TempDir()
	e := &testEnv{
		t:       t,
		rootTmp: root,
		dirs: [NumShards]string{
			filepath.Join(root, "disk0"),
			filepath.Join(root, "disk1"),
			filepath.Join(root, "disk2"),
		},
	}
	s, err := Open(context.Background(), e.dirs[0], e.dirs[1], e.dirs[2], hooks)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	e.s = s
	return e
}

// reopen simulates a process restart: a new Store opens the same
// directories and runs recovery.
func (e *testEnv) reopen(hooks *Hooks) *Store {
	e.t.Helper()
	s, err := Open(context.Background(), e.dirs[0], e.dirs[1], e.dirs[2], hooks)
	if err != nil {
		e.t.Fatalf("reopen: %v", err)
	}
	e.s = s
	return s
}

// openPeer opens a second Store sharing the same directories, without
// running recovery (so it cannot sweep the peer's staged files).
func (e *testEnv) openPeer(hooks *Hooks) *Store {
	e.t.Helper()
	s, err := Open(context.Background(), e.dirs[0], e.dirs[1], e.dirs[2],
		hooks, SkipRecoveryOnOpen())
	if err != nil {
		e.t.Fatalf("openPeer: %v", err)
	}
	return s
}

// corruptShard overwrites one published shard's bytes on disk.
func (e *testEnv) corruptShard(key string, gen uint64, shard int) {
	e.t.Helper()
	role := []string{"a", "b", "p"}[shard]
	path := shardPath(e.dirs[shard], keyID(key), gen, role)
	if err := os.WriteFile(path, []byte("garbage-bytes-not-a-shard!!!"), filePerm); err != nil {
		e.t.Fatalf("corruptShard: %v", err)
	}
}

// removeShard deletes one published shard file (or the whole disk dir
// when removeDisk is used separately).
func (e *testEnv) removeShard(key string, gen uint64, shard int) {
	e.t.Helper()
	role := []string{"a", "b", "p"}[shard]
	path := shardPath(e.dirs[shard], keyID(key), gen, role)
	if err := os.Remove(path); err != nil {
		e.t.Fatalf("removeShard: %v", err)
	}
}

func (e *testEnv) removeDisk(shard int) {
	e.t.Helper()
	if err := os.RemoveAll(e.dirs[shard]); err != nil {
		e.t.Fatalf("removeDisk: %v", err)
	}
}

// stageFileCount returns the total number of unpublished files left in
// any staging directory.
func (e *testEnv) stageFileCount() int {
	e.t.Helper()
	n := 0
	for _, d := range e.dirs {
		entries, err := os.ReadDir(filepath.Join(d, stageDirName))
		if err != nil {
			continue
		}
		for _, en := range entries {
			if !en.IsDir() {
				n++
			}
		}
	}
	return n
}

// shardExistsOnDisk reports whether the published shard file is present.
func (e *testEnv) shardExistsOnDisk(key string, gen uint64, shard int) bool {
	role := []string{"a", "b", "p"}[shard]
	return fileExists(shardPath(e.dirs[shard], keyID(key), gen, role))
}

func mustPut(t *testing.T, s *Store, key string, data []byte, expect uint64) uint64 {
	t.Helper()
	gen, err := s.Put(context.Background(), key, data, expect)
	if err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	return gen
}

func mustGet(t *testing.T, s *Store, key string, want []byte) uint64 {
	t.Helper()
	got, gen, _, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if string(got) != string(want) {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
	return gen
}

func errIs(t *testing.T, err error, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("err = %v, want %v", err, target)
	}
}

var _ = errIs
