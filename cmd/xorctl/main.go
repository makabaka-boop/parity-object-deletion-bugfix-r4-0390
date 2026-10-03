// Command xorctl is a tiny demo CLI for the three-disk XOR object store.
//
// Usage:
//
//	xorctl -root DIR put    KEY FILE/STRING [EXPECTGEN]
//	xorctl -root DIR get    KEY
//	xorctl -root DIR delete KEY EXPECTGEN
//	xorctl -root DIR repair KEY
//	xorctl -root DIR doctor       # run one background-repair pass
//
// delete returns the post-delete generation; recreate the same key with
// "put KEY CONTENT <that generation>". A first write refuses to reuse an
// old generation when unreferenced shards of an erased object remain
// (reopen once to let recovery collect them).
//
// The root directory holds disk0/disk1/disk2. Disks may be corrupted by
// hand between invocations to observe read repair:
//
//	echo garbage > $root/disk1/<id>-gen1-b.shard
//	rm            $root/disk2/<id>-gen1-p.shard
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"xorstore"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "xorctl:", err)
		switch {
		case errors.Is(err, xorstore.ErrConflict):
			os.Exit(3)
		case errors.Is(err, xorstore.ErrUnrecoverable):
			os.Exit(4)
		case errors.Is(err, xorstore.ErrNotFound):
			os.Exit(5)
		default:
			os.Exit(1)
		}
	}
}

func run(args []string) error {
	root := "./xordata"
	rest := args
	if len(args) >= 2 && args[0] == "-root" {
		root = args[1]
		rest = args[2:]
	}
	if len(rest) == 0 {
		return errors.New("need a command: put|get|repair|doctor")
	}
	cmd := rest[0]

	ctx := context.Background()
	s, err := xorstore.Open(ctx,
		filepath.Join(root, "disk0"),
		filepath.Join(root, "disk1"),
		filepath.Join(root, "disk2"),
		nil)
	if err != nil {
		return err
	}

	switch cmd {
	case "put":
		if len(rest) < 3 {
			return errors.New("usage: put KEY CONTENT")
		}
		key, content := rest[1], []byte(rest[2])
		expect := uint64(0)
		if data, gen, _, gerr := s.Get(ctx, key); gerr == nil {
			expect = gen
			_ = data
		} else if !errors.Is(gerr, xorstore.ErrNotFound) {
			return gerr
		}
		if len(rest) > 3 {
			expect, err = strconv.ParseUint(rest[3], 10, 64)
			if err != nil {
				return err
			}
		}
		gen, err := s.Put(ctx, key, content, expect)
		if err != nil {
			return err
		}
		fmt.Printf("published %q generation %d (%d bytes)\n", key, gen, len(content))
		return nil

	case "get":
		if len(rest) < 2 {
			return errors.New("usage: get KEY")
		}
		data, gen, repaired, err := s.Get(ctx, rest[1])
		if err != nil {
			return err
		}
		if len(repaired) > 0 {
			fmt.Fprintf(os.Stderr, "note: rebuilt shards %v from parity\n", repaired)
		}
		fmt.Printf("generation %d: %s\n", gen, string(data))
		return nil

	case "delete":
		if len(rest) != 3 {
			return errors.New("usage: delete KEY EXPECTGEN")
		}
		expect, err := strconv.ParseUint(rest[2], 10, 64)
		if err != nil {
			return err
		}
		gen, err := s.Delete(ctx, rest[1], expect)
		if err != nil {
			return err
		}
		fmt.Printf("deleted generation %d\n", gen)
		return nil
	case "repair":
		if len(rest) < 2 {
			return errors.New("usage: repair KEY")
		}
		gen, repaired, err := s.Repair(ctx, rest[1])
		if err != nil {
			return err
		}
		fmt.Printf("generation %d healthy; repaired shards %v\n", gen, repaired)
		return nil

	case "doctor":
		if err := s.RunRepairPass(ctx); err != nil {
			return err
		}
		fmt.Println("repair pass complete")
		return nil

	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}
