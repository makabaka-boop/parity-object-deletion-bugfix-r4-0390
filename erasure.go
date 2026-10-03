// Package xorstore implements a small object store that stripes every
// object across three local directories ("disks") as two equal-length
// data shards plus one byte-wise XOR parity shard.
//
// A manifest per object records the original length, per-shard SHA-256
// digests and the object's generation. New generations are published with
// a conditional (expected-generation) update: all three new shards must be
// staged and verified before the new manifest becomes readable. Deletion
// is published the same way, as a higher-generation tombstone manifest.
// Reads reconstruct from, and repair, a single bad/missing shard; two bad
// shards are reported unrecoverable. Replays of older-generation repairs
// can never overwrite newer generations or deleted objects because shards
// and manifests are generation scoped and installation is re-checked
// (generation plus shard digests) under the key lock.
package xorstore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
)

// NumShards is the fixed number of shards (two data, one parity).
const NumShards = 3

// Shard indexes.
const (
	ShardA = 0 // data shard 0
	ShardB = 1 // data shard 1
	ShardP = 2 // XOR parity shard
)

// Errors returned by the store. Callers should test with errors.Is.
var (
	// ErrConflict means a conditional write found a newer generation
	// than the caller expected.
	ErrConflict = errors.New("xorstore: generation conflict")
	// ErrNotFound means the object has no readable manifest.
	ErrNotFound = errors.New("xorstore: object not found")
	// ErrUnrecoverable means two or more shards are missing/corrupt.
	ErrUnrecoverable = errors.New("xorstore: object unrecoverable")
	// ErrManifestCorrupt means no structurally valid manifest exists.
	ErrManifestCorrupt = errors.New("xorstore: manifest corrupt")
	// ErrGenerationAmbiguous means no manifest determines the object's
	// current generation while shard files of an older incarnation are
	// still present on disk (for example every manifest copy was deleted
	// out of band). A first-time Put (expectGen 0) is refused in that
	// state because it could reuse an old generation and make new content
	// indistinguishable from the old one; restart recovery clears the
	// orphan shards, after which the Put is accepted.
	ErrGenerationAmbiguous = fmt.Errorf("xorstore: generation evidence insufficient: %w", ErrManifestCorrupt)
	// ErrSimulatedCrash is returned by test hooks to model a process
	// crash at a hook boundary: the operation stops immediately, leaving
	// staged files on disk for restart recovery to sweep.
	ErrSimulatedCrash = errors.New("xorstore: simulated crash")
)

// ShardInfo is one shard entry of a manifest.
type ShardInfo struct {
	// Role is "a", "b" or "p".
	Role string `json:"role,omitempty"`
	// Size is the on-disk shard length in bytes (both data shards and
	// parity have identical size).
	Size int `json:"size,omitempty"`
	// Digest is the lowercase hex SHA-256 of the shard bytes.
	Digest string `json:"digest,omitempty"`
}

// Manifest is the per-object metadata published atomically.
type Manifest struct {
	// Deleted marks a tombstone manifest: it records that the object was
	// conditionally deleted at Gen. A tombstone is the authoritative
	// "current generation" for its key: reads and repairs report
	// ErrNotFound, the background scan skips the key, and a same-name
	// recreation must conditional-write at Gen (which publishes Gen+1
	// rather than reusing any older generation). Tombstones carry no
	// shard metadata; all shard generations below Gen are garbage.
	Deleted bool   `json:"deleted,omitempty"`
	Schema  int    `json:"schema"`
	Key     string `json:"key"`
	// Gen is the monotonically increasing object generation, starting at 1.
	// For a tombstone it is the delete generation (the deleted content's
	// generation plus one).
	Gen uint64 `json:"gen"`
	// Length is the original object length; for odd lengths the second
	// data shard carries one zero padding byte that is not part of it.
	// It is zero (and unused) on a tombstone.
	Length int                  `json:"length"`
	Shards [NumShards]ShardInfo `json:"shards"`
}

// splitShards divides data into two equal-length shards (zero padding the
// second one when len(data) is odd) and computes the XOR parity shard.
// All three returned shards have the same length.
func splitShards(data []byte) (a, b, p []byte) {
	n := (len(data) + 1) / 2
	a = make([]byte, n)
	b = make([]byte, n)
	copy(a, data[:len(a)])
	copy(b, data[len(a):]) // tail stays zero for odd lengths
	p = make([]byte, n)
	xorInto(p, a, b)
	return a, b, p
}

// joinShards reassembles the original object from its two data shards,
// stripping the possible single padding byte.
func joinShards(a, b []byte, length int) []byte {
	out := make([]byte, length)
	copy(out, a)
	second := length - len(a)
	if second < 0 {
		second = 0
	}
	copy(out[len(a):], b[:second])
	return out
}

// reconstruct computes the missing shard at bad from the other two:
// each shard is the XOR of the other two.
func reconstruct(x, y []byte) []byte {
	z := make([]byte, len(x))
	xorInto(z, x, y)
	return z
}

// xorInto sets dst[i] = x[i] ^ y[i]; lengths are assumed equal.
func xorInto(dst, x, y []byte) {
	for i := range x {
		dst[i] = x[i] ^ y[i]
	}
}

// digestBytes returns the lowercase hex SHA-256 of data.
func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// keyID derives the filesystem-safe identifier of a key (hex SHA-256).
func keyID(key string) string {
	return digestBytes([]byte(key))
}

// tmpSuffix returns a random suffix used by unpublished staged files.
func tmpSuffix() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// rand.Read failing is not recoverable in any meaningful way;
		// fall back to a constant suffix and let rename collisions fail.
		return "0000000000000000"
	}
	return hex.EncodeToString(buf[:])
}

// fileExists reports whether path exists (any file type).
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
