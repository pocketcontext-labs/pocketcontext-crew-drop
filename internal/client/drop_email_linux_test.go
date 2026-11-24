//go:build linux && crew_drop

package client

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestDropCodeFrame(t *testing.T) {
	for _, tc := range []struct {
		name, reply string
		wantOK      bool
	}{
		{"accepted", "{\"status\":\"accepted\"}\n", true},
		{"lost acknowledgement", "", false},
		{"unterminated", "{\"status\":\"accepted\"}", false},
		{"throttled", "{\"status\":\"throttled\"}\n", false},
		{"oversized", strings.Repeat(" ", 128) + "{\"status\":\"accepted\"}\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sent bytes.Buffer
			rw := struct {
				io.Reader
				io.Writer
			}{strings.NewReader(tc.reply), &sent}
			err := submitDropCode(rw, "012345")
			if (err == nil) != tc.wantOK || sent.String() != "{\"code\":\"012345\"}\n" {
				t.Fatal("unexpected delivery outcome or frame")
			}
		})
	}
}
