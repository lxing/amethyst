package common

import "fmt"

// SSTablePath returns the file path for an SSTable at the given level and file number.
func SSTablePath(level int, fileNo FileNo) string {
	return fmt.Sprintf("sstable/%d/%d.sst", level, fileNo)
}

// WALPath returns the file path for a WAL with the given file number.
func WALPath(fileNo FileNo) string {
	return fmt.Sprintf("wal/%d.log", fileNo)
}
