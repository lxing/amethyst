package loadgen

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// stateFile lives inside the DB directory so checkpoints capture it with the data.
const stateFile = "LOADGEN_STATE"

// StatePath returns where the key space for the DB at dbDir is persisted.
func StatePath(dbDir string) string {
	return filepath.Join(dbDir, stateFile)
}

// LoadKeySpace reads the persisted key space. A missing file means a fresh DB.
func LoadKeySpace(dbDir string) (KeySpace, error) {
	var ks KeySpace
	data, err := os.ReadFile(StatePath(dbDir))
	if errors.Is(err, fs.ErrNotExist) {
		return ks, nil
	}
	if err != nil {
		return ks, err
	}
	err = json.Unmarshal(data, &ks)
	return ks, err
}

// SaveKeySpace persists the key space atomically (temp file + rename).
func SaveKeySpace(dbDir string, ks KeySpace) error {
	data, err := json.Marshal(ks)
	if err != nil {
		return err
	}
	path := StatePath(dbDir)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
