package common

import (
	"fmt"
	"path/filepath"
)

type PathManager struct {
	BasePath string
}

func NewPathManager(basePath string) *PathManager {
	return &PathManager{BasePath: basePath}
}

func (pm *PathManager) WALPath(fileNo FileNo) string {
	return filepath.Join(pm.BasePath, "wal", fmt.Sprintf("%d.log", fileNo))
}

func (pm *PathManager) SSTablePath(level int, fileNo FileNo) string {
	return filepath.Join(pm.BasePath, "sstable", fmt.Sprintf("%d/%d.sst", level, fileNo))
}

func (pm *PathManager) ManifestPath() string {
	return filepath.Join(pm.BasePath, "MANIFEST")
}

func (pm *PathManager) WALDir() string {
	return filepath.Join(pm.BasePath, "wal")
}

func (pm *PathManager) SSTableDir() string {
	return filepath.Join(pm.BasePath, "sstable")
}

func (pm *PathManager) SeedIndexPath() string {
	return filepath.Join(pm.BasePath, "CLI_SEED_INDEX")
}
