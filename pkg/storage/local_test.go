package storage

import (
	"io/ioutil"
	"testing"
)

// Test validates `WriteFile` and 'ReadFile'.
func TestLocalReadWriteFile(t *testing.T) {
	tmpdir, _ := ioutil.TempDir("", "dumper-sql")
	s, err := NewLocalStorage(tmpdir)
	if err != nil {
		t.Errorf("Failed to initialize local storage: %v", err)
	}
	fileName := "localtestfile"
	expectedContent := "Hello, mydumper."
	err = s.WriteFile(fileName, expectedContent)
	if err != nil {
		t.Errorf("WriteFile failed: %v", err)
	}

	bytes, err := s.ReadFile(fileName)
	if err != nil {
		t.Errorf("ReadFile failed: %v", err)
	}
	if string(bytes) != expectedContent {
		t.Errorf("Content read is different from written")
	}
}
