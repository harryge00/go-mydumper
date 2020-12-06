// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

// ExternalStorage represents a kind of file system storage.
type ExternalStorage interface {
	// Write file to storage
	WriteFile(name string, data string) error
	// Read storage file
	ReadFile(name string) ([]byte, error)
}
