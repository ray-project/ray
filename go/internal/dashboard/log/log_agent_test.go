// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// lineLen is the exact byte length of each "line-i\n" line in fixedLines, so
// offsets are predictable.
const lineLen = 7

// openForTest writes content to a temp file and returns it opened for reading.
func openForTest(t *testing.T, content string) *os.File {
	t.Helper()
	p := filepath.Join(t.TempDir(), "f.log")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// fixedLines builds n newline-terminated lines of the form "line-i\n".
func fixedLines(n int) []byte {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteString("line-")
		buf.WriteByte(byte('0' + i))
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// searchLines builds n newline-terminated lines of the form "i-test-line\n",
// each exactly 12 bytes, matching the Python reference test data.
func searchLines(n int) []byte {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString("-test-line\n")
	}
	return buf.Bytes()
}

// lineOffsets returns the start offset of each line in searchLines(n), i.e.
// the i-th line starts at i*12.
func lineOffsets(n int) []int64 {
	o := make([]int64, n)
	for i := range o {
		o[i] = int64(i * 12)
	}
	return o
}

func TestFindEndOffsetFile(t *testing.T) {
	f := openForTest(t, "abc\ndef\n")
	got, err := findEndOffsetFile(f)
	if err != nil {
		t.Fatalf("findEndOffsetFile: %v", err)
	}
	if want := int64(len("abc\ndef\n")); got != want {
		t.Errorf("end offset = %d, want %d", got, want)
	}
	pos, err := f.Seek(0, os.SEEK_CUR)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Errorf("file pointer moved to %d, want 0", pos)
	}
}

func TestFindOffsetOfContentInFileFromOffset(t *testing.T) {
	f := openForTest(t, string(searchLines(10)))
	o := lineOffsets(10)

	cases := []struct {
		name    string
		content string
		start   int64
		want    int64
	}{
		{"first occurrence from start", "0-test-line", 0, o[0]},
		{"occurrence after start offset", "3-test-line", o[1] + 1, o[3]},
		{"occurrence from mid-line start", "4-test-line", o[1] - 1, o[4]},
		{"not found", "1000-test-line", o[1] - 1, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := findOffsetOfContentInFile(f, []byte(tc.content), tc.start)
			if err != nil {
				t.Fatalf("findOffsetOfContentInFile: %v", err)
			}
			if got != tc.want {
				t.Errorf("findOffsetOfContentInFile = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestFindEndOffsetNextNLinesFromOffset(t *testing.T) {
	fixed := func(t *testing.T, n int) *os.File {
		t.Helper()
		return openForTest(t, string(fixedLines(n)))
	}

	t.Run("forward three lines", func(t *testing.T) {
		f := fixed(t, 10)
		got, err := findEndOffsetNextNLinesFromOffset(f, 0, 3)
		if err != nil {
			t.Fatalf("findEndOffsetNextNLinesFromOffset: %v", err)
		}
		if want := int64(21); got != want {
			t.Errorf("end = %d, want %d", got, want)
		}
	})
	t.Run("zero lines returns end of file", func(t *testing.T) {
		f := fixed(t, 10)
		got, err := findEndOffsetNextNLinesFromOffset(f, 0, 0)
		if err != nil {
			t.Fatalf("findEndOffsetNextNLinesFromOffset: %v", err)
		}
		if want := int64(len(fixedLines(10))); got != want {
			t.Errorf("end = %d, want %d", got, want)
		}
	})
	t.Run("more lines than file", func(t *testing.T) {
		f := fixed(t, 10)
		got, err := findEndOffsetNextNLinesFromOffset(f, 0, 100)
		if err != nil {
			t.Fatalf("findEndOffsetNextNLinesFromOffset: %v", err)
		}
		if want := int64(len(fixedLines(10))); got != want {
			t.Errorf("end = %d, want %d", got, want)
		}
	})
	t.Run("mid-line start", func(t *testing.T) {
		f := fixed(t, 10)
		got, err := findEndOffsetNextNLinesFromOffset(f, int64(lineLen), 2)
		if err != nil {
			t.Fatalf("findEndOffsetNextNLinesFromOffset: %v", err)
		}
		if want := int64(3 * lineLen); got != want {
			t.Errorf("end = %d, want %d", got, want)
		}
	})
	t.Run("no trailing newline counts as a line", func(t *testing.T) {
		f := openForTest(t, "abc\ndef")
		got, err := findEndOffsetNextNLinesFromOffset(f, 0, 2)
		if err != nil {
			t.Fatalf("findEndOffsetNextNLinesFromOffset: %v", err)
		}
		if want := int64(7); got != want {
			t.Errorf("end = %d, want %d", got, want)
		}
	})
}

func TestFindStartOffsetLastNLinesFromOffset(t *testing.T) {
	const content = "aa\nbb\ncc\ndd\n"

	countedOffset := func(t *testing.T, offset int64, n int64) int64 {
		t.Helper()
		f := openForTest(t, content)
		got, err := findStartOffsetLastNLinesFromOffset(f, offset, n, 64)
		if err != nil {
			t.Fatalf("findStartOffsetLastNLinesFromOffset(%d, %d): %v", offset, n, err)
		}
		return got
	}

	t.Run("last two lines", func(t *testing.T) {
		if got := countedOffset(t, 12, 2); got != 6 {
			t.Errorf("offset = %d, want 6", got)
		}
	})
	t.Run("last one line", func(t *testing.T) {
		if got := countedOffset(t, 12, 1); got != 9 {
			t.Errorf("offset = %d, want 9", got)
		}
	})
	t.Run("zero lines returns offset", func(t *testing.T) {
		if got := countedOffset(t, 12, 0); got != 12 {
			t.Errorf("offset = %d, want 12", got)
		}
	})
	t.Run("more lines than file returns start", func(t *testing.T) {
		if got := countedOffset(t, 12, 100); got != 0 {
			t.Errorf("offset = %d, want 0", got)
		}
	})
	t.Run("end of file marker", func(t *testing.T) {
		if got := countedOffset(t, -1, 2); got != 6 {
			t.Errorf("offset = %d, want 6", got)
		}
	})
	t.Run("middle offset", func(t *testing.T) {
		// Tail 2 lines ending at the "dd" line start (exclusive offset 9);
		// the window covers "bb\n" and "cc\n", starting at 3.
		if got := countedOffset(t, 9, 2); got != 3 {
			t.Errorf("offset = %d, want 3", got)
		}
	})
	t.Run("unterminated last line counts", func(t *testing.T) {
		f := openForTest(t, "aa\nbb")
		got, err := findStartOffsetLastNLinesFromOffset(f, 5, 1, 64)
		if err != nil {
			t.Fatalf("findStartOffsetLastNLinesFromOffset: %v", err)
		}
		if got != 3 {
			t.Errorf("offset = %d, want 3", got)
		}
	})
	t.Run("block boundary multiple blocks", func(t *testing.T) {
		f := openForTest(t, "aa\nbb\ncc\ndd\n")
		got, err := findStartOffsetLastNLinesFromOffset(f, 12, 4, 4)
		if err != nil {
			t.Fatalf("findStartOffsetLastNLinesFromOffset: %v", err)
		}
		if got != 0 {
			t.Errorf("offset = %d, want 0", got)
		}
	})
	t.Run("forward last three lines", func(t *testing.T) {
		f := openForTest(t, string(fixedLines(10)))
		got, err := findStartOffsetLastNLinesFromOffset(f, int64(len(fixedLines(10))), 3, 64)
		if err != nil {
			t.Fatalf("findStartOffsetLastNLinesFromOffset: %v", err)
		}
		if want := int64(7 * lineLen); got != want {
			t.Errorf("offset = %d, want %d", got, want)
		}
	})
	t.Run("empty file", func(t *testing.T) {
		f := openForTest(t, "")
		got, err := findStartOffsetLastNLinesFromOffset(f, 0, 3, 64)
		if err != nil {
			t.Fatalf("findStartOffsetLastNLinesFromOffset: %v", err)
		}
		if got != 0 {
			t.Errorf("offset = %d, want 0", got)
		}
	})
}
