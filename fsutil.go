package xorstore

import (
	"os"
	"path/filepath"
)

const (
	stageDirName = ".stage"
	metaDirName  = ".manifests"
	dirPerm      = 0o755
	filePerm     = 0o644
)

// shardPath returns the published path of a (key, generation, role) shard
// on disk d.
func shardPath(d, id string, gen uint64, role string) string {
	return filepath.Join(d, id+"-gen"+itoa(gen)+"-"+role+".shard")
}

// manifestPath returns the published manifest path of key id on disk d.
func manifestPath(d, id string) string {
	return filepath.Join(d, metaDirName, id+".json")
}

// stagePath returns the path of an unpublished staged file.
func stagePath(d, id, name string) string {
	return filepath.Join(d, stageDirName, id+"-"+name+"-"+tmpSuffix()+".tmp")
}

// itoa formats a uint64 without pulling in strconv at call sites.
func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// stageFile writes data to a fresh temp file in the disk's staging
// directory, fsyncs it, and returns its path. Nothing here is readable as
// a shard or manifest.
func stageFile(d, id, name string, data []byte) (string, error) {
	path := stagePath(d, id, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(path)
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

// readStaged reads a staged file back (used to verify staged content).
func readStaged(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// commitStaged atomically renames a staged file into its final published
// location and fsyncs the containing directory so the rename is durable.
func commitStaged(staged, final string) error {
	if err := os.MkdirAll(filepath.Dir(final), dirPerm); err != nil {
		return err
	}
	if err := os.Rename(staged, final); err != nil {
		return err
	}
	return syncDir(filepath.Dir(final))
}

// syncDir fsyncs a directory so that recent renames/unlinks inside it are
// persisted.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// writeStagedAndCommit stages data then atomically publishes it at final.
func writeStagedAndCommit(d, id, name string, data []byte, final string) error {
	staged, err := stageFile(d, id, name, data)
	if err != nil {
		return err
	}
	return commitStaged(staged, final)
}

// removeBestEffort deletes files whose removal failure should not abort
// the surrounding operation.
func removeBestEffort(paths ...string) {
	for _, p := range paths {
		_ = os.Remove(p)
	}
}

// diskExists reports whether the top-level disk directory is present.
// A removed disk directory simulates a failed/unmounted local disk.
func diskExists(d string) bool {
	info, err := os.Stat(d)
	return err == nil && info.IsDir()
}
