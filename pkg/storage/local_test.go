package storage

import (
	"io/ioutil"
	"testing"
)

// Test validates `WriteFile` and 'ReadFile'.
func TestLocalReadWriteFile(t *testing.T) {
	s := NewLocalStorage()
	tmpfile, _ := ioutil.TempFile("", "localtestfile")
	expectedContent := "Hello, mydumper."
	err := s.WriteFile(tmpfile.Name(), expectedContent)
	if err != nil {
		t.Errorf("WriteFile failed: %v", err)
	}

	bytes, err := s.ReadFile(tmpfile.Name())
	if err != nil {
		t.Errorf("ReadFile failed: %v", err)
	}
	if string(bytes) != expectedContent {
		t.Errorf("Content read is different from written")
	}
}
