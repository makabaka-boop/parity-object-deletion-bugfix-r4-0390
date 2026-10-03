package xorstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RepairLoop periodically scans every manifest and repairs objects with
// a single bad shard. The scan is generation-oblivious: it always repairs
// whatever generation is currently published, and stale repairs are
// rejected by the generation guard in commitRepair.
type RepairLoop struct {
	s    *Store
	stop context.CancelFunc
	done chan struct{}
	wg   sync.WaitGroup
}

// StartRepairLoop starts a background repair loop that does an immediate
// pass and then another pass at least every interval.
func (s *Store) StartRepairLoop(ctx context.Context, interval time.Duration) *RepairLoop {
	ctx, cancel := context.WithCancel(ctx)
	l := &RepairLoop{s: s, stop: cancel, done: make(chan struct{})}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer close(l.done)
		_ = l.s.RunRepairPass(ctx)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = l.s.RunRepairPass(ctx)
			}
		}
	}()
	return l
}

// Stop halts the loop and waits for the in-flight pass to finish.
func (l *RepairLoop) Stop() {
	l.stop()
	l.wg.Wait()
}

// RunRepairPass scans all manifest files and attempts to repair each
// distinct object once. Unrecoverable objects are skipped (they need
// operator attention); the first context/IO error encountered is
// returned after completing the scan where possible.
func (s *Store) RunRepairPass(ctx context.Context) error {
	ids := s.listObjectIDs()
	var firstErr error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, err := s.keyFromID(id)
		if err != nil {
			continue // manifest unreadable; leave for later
		}
		_, _, err = s.Repair(ctx, key)
		if err != nil {
			if firstErr == nil &&
				!isError(err, ErrUnrecoverable) &&
				!isError(err, ErrManifestCorrupt) {
				firstErr = err
			}
		}
	}
	return firstErr
}

// listObjectIDs returns the distinct object ids referenced by manifest
// files on any disk.
func (s *Store) listObjectIDs() []string {
	seen := map[string]struct{}{}
	for _, d := range s.dirs {
		entries, err := os.ReadDir(filepath.Join(d, metaDirName))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if n := e.Name(); strings.HasSuffix(n, ".json") {
				seen[strings.TrimSuffix(n, ".json")] = struct{}{}
			}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

// keyFromID recovers the object key by loading its manifest.
func (s *Store) keyFromID(id string) (string, error) {
	man, _, err := s.loadBestManifest(context.Background(), id, false)
	if err != nil {
		return "", err
	}
	return man.Key, nil
}

func isError(err, target error) bool {
	return errors.Is(err, target)
}
