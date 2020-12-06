/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeBytes(t *testing.T) {
	tests := []struct {
		v   []byte
		exp []byte
	}{
		{[]byte("simple"), []byte("simple")},
		{[]byte(`simplers's "world"`), []byte(`simplers\'s \"world\"`)},
		{[]byte("\x00'\"\b\n\r"), []byte(`\0\'\"\b\n\r`)},
		{[]byte("\t\x1A\\"), []byte(`\t\Z\\`)},
	}
	for _, tt := range tests {
		got := EscapeBytes(tt.v)
		want := tt.exp
		assert.Equal(t, want, got)
	}
}
