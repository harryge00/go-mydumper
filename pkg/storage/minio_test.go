package storage

import (
	"context"
	"testing"
)

// Test validates `WriteFile` and 'ReadFile'.
func TestMinioReadWriteFile(t *testing.T) {
	accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	useSSL := true

	ms, err := NewMinioStorage(context.Background(), "play.minio.io:9000", "test-backup",
		accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		t.Errorf("Failed to create NewMinioStorage: %v", err)
	}

	tmpfile := "go-mydumper-testfile"
	expectedContent := "Hello, mydumper."
	err = ms.WriteFile(tmpfile, expectedContent)
	if err != nil {
		t.Errorf("WriteFile failed: %v", err)
	}

	bytes, err := ms.ReadFile(tmpfile)
	if err != nil {
		t.Errorf("ReadFile failed: %v", err)
	}
	if string(bytes) != expectedContent {
		t.Errorf("Content read is different from written")
	}
}
