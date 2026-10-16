// Package checkpoint saves and restores a whole DB directory on disk.
//
// It is deliberately stop-the-world: the caller must guarantee that nothing is
// writing to the DB while Save runs (e.g. the load generator is stopped and no
// Put/Delete is in flight), and that the DB is closed (or about to be replaced
// and reopened) around Restore. No engine locks are involved — every WAL batch
// is fsynced before Put returns, so a quiescent directory is consistent and a
// plain recursive copy captures it exactly.
//
// This is unrelated to the metadata snapshot streamed to the visualizer.
package checkpoint

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

var validName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// Info describes a saved checkpoint.
type Info struct {
	Name    string    `json:"name"`
	Created time.Time `json:"created"`
	Files   int       `json:"files"`
	Bytes   int64     `json:"bytes"`
}

// Save copies dbDir to root/name. It fails if that checkpoint already exists.
// The copy is staged under a temporary name and renamed into place, so a
// failed save never leaves a partial checkpoint behind.
func Save(dbDir, root, name string) (Info, error) {
	if err := checkName(name); err != nil {
		return Info{}, err
	}
	dst := filepath.Join(root, name)
	if _, err := os.Stat(dst); err == nil {
		return Info{}, fmt.Errorf("checkpoint %q already exists", name)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return Info{}, err
	}

	staging := dst + ".saving"
	if err := os.RemoveAll(staging); err != nil {
		return Info{}, err
	}
	if err := copyDir(dbDir, staging); err != nil {
		os.RemoveAll(staging)
		return Info{}, fmt.Errorf("copy %s: %w", dbDir, err)
	}
	if err := os.Rename(staging, dst); err != nil {
		os.RemoveAll(staging)
		return Info{}, err
	}
	return stat(root, name)
}

// Restore replaces dbDir with a copy of root/name. The checkpoint itself is
// left untouched, so it can be restored any number of times.
//
// The copy is staged next to dbDir first; dbDir is only removed once the
// staged copy is complete, so a failed restore leaves the current DB intact.
func Restore(root, name, dbDir string) error {
	if err := checkName(name); err != nil {
		return err
	}
	src := filepath.Join(root, name)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("checkpoint %q: %w", name, err)
	}

	staging := filepath.Clean(dbDir) + ".restoring"
	if err := os.RemoveAll(staging); err != nil {
		return err
	}
	if err := copyDir(src, staging); err != nil {
		os.RemoveAll(staging)
		return fmt.Errorf("copy %s: %w", src, err)
	}
	if err := os.RemoveAll(dbDir); err != nil {
		os.RemoveAll(staging)
		return err
	}
	return os.Rename(staging, dbDir)
}

// List returns all checkpoints under root, oldest first.
func List(root string) ([]Info, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var infos []Info
	for _, e := range entries {
		if !e.IsDir() || !validName.MatchString(e.Name()) || filepath.Ext(e.Name()) == ".saving" {
			continue
		}
		info, err := stat(root, e.Name())
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Created.Before(infos[j].Created) })
	return infos, nil
}

// Remove deletes root/name.
func Remove(root, name string) error {
	if err := checkName(name); err != nil {
		return err
	}
	dir := filepath.Join(root, name)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("checkpoint %q: %w", name, err)
	}
	return os.RemoveAll(dir)
}

func checkName(name string) error {
	if !validName.MatchString(name) {
		return fmt.Errorf("invalid checkpoint name %q (letters, digits, . _ -)", name)
	}
	return nil
}

func stat(root, name string) (Info, error) {
	dir := filepath.Join(root, name)
	fi, err := os.Stat(dir)
	if err != nil {
		return Info{}, err
	}
	info := Info{Name: name, Created: fi.ModTime()}
	err = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		info.Files++
		info.Bytes += fi.Size()
		return nil
	})
	return info, err
}

// copyDir recursively copies regular files and directories from src to dst.
// Files are real copies, never hard links: the restored WAL is appended to,
// and that must not mutate the checkpoint.
func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)

		switch {
		case d.IsDir():
			return os.MkdirAll(target, 0o755)
		case d.Type().IsRegular():
			return copyFile(path, target)
		default:
			return fmt.Errorf("unexpected non-regular file %s", path)
		}
	})
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
