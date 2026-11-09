// Copyright 2026 The Ray Authors.
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

package logmonitor

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

func TestReadCappedLineNormal(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("first\nsecond\n"))
	line, truncated, err := readCappedLine(r, defaultMaxLineSize)
	if err != nil || truncated || line != "first\n" {
		t.Fatalf("readCappedLine() = %q(truncated=%v), %v; want %q, false, nil", line, truncated, err, "first\n")
	}
	line, truncated, err = readCappedLine(r, defaultMaxLineSize)
	if err != nil || truncated || line != "second\n" {
		t.Fatalf("readCappedLine() = %q(truncated=%v), %v; want %q, false, nil", line, truncated, err, "second\n")
	}
}

func TestReadCappedLineEOFPartial(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("no-newline"))
	line, truncated, err := readCappedLine(r, defaultMaxLineSize)
	if err != io.EOF || truncated || line != "no-newline" {
		t.Fatalf("readCappedLine() = %q(truncated=%v), %v; want %q, false, io.EOF", line, truncated, err, "no-newline")
	}
}

func TestReadCappedLineTruncatesAndDrains(t *testing.T) {
	const max = 8
	long := strings.Repeat("x", 50) + "\n" + "next\n"
	r := bufio.NewReader(strings.NewReader(long))
	line, truncated, err := readCappedLine(r, max)
	if err != nil || !truncated || len(line) != max || line != strings.Repeat("x", max) {
		t.Fatalf("readCappedLine() = %q(len=%d, truncated=%v), %v; want %d x's, truncated=true, nil", line, len(line), truncated, err, max)
	}
	line, truncated, err = readCappedLine(r, max)
	if err != nil || truncated || line != "next\n" {
		t.Fatalf("after drain readCappedLine() = %q(truncated=%v), %v; want %q, false, nil", line, truncated, err, "next\n")
	}
}

func TestDrainLineStopsAtNewline(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(strings.Repeat("y", 100) + "\nrest\n"))
	drainLine(r)
	line, truncated, err := readCappedLine(r, defaultMaxLineSize)
	if err != nil || truncated || line != "rest\n" {
		t.Fatalf("after drainLine readCappedLine() = %q(truncated=%v), %v; want %q, false, nil", line, truncated, err, "rest\n")
	}
}

func TestReadCappedLineTruncatesLongLineBeyondBuffer(t *testing.T) {
	const max = 16
	const bufSize = 64
	long := strings.Repeat("x", 100000) + "\nnext\n"
	r := bufio.NewReaderSize(strings.NewReader(long), bufSize)
	line, truncated, err := readCappedLine(r, max)
	if err != nil || !truncated || len(line) != max || line != strings.Repeat("x", max) {
		t.Fatalf("readCappedLine() = %q(len=%d, truncated=%v), %v; want %d x's, truncated=true, nil", line, len(line), truncated, err, max)
	}
	line, truncated, err = readCappedLine(r, max)
	if err != nil || truncated || line != "next\n" {
		t.Fatalf("after drain readCappedLine() = %q(truncated=%v), %v; want %q, false, nil", line, truncated, err, "next\n")
	}
}

func TestReadCappedLineEOFTruncatesLongFinalLine(t *testing.T) {
	const max = 8
	r := bufio.NewReader(strings.NewReader(strings.Repeat("z", 50))) // no trailing newline
	line, truncated, err := readCappedLine(r, max)
	if err != io.EOF || !truncated || len(line) != max || line != strings.Repeat("z", max) {
		t.Fatalf("readCappedLine() = %q(len=%d, truncated=%v), %v; want %d z's, truncated=true, io.EOF", line, len(line), truncated, err, max)
	}
}

// TestReadCappedLineSlowPathExactlyAtCap exercises the slow path (line longer
// than the reader buffer) with a line that is exactly maxLineSize bytes
// including the trailing newline: it is a complete line and must not be
// reported as truncated, matching the fast-path semantics.
func TestReadCappedLineSlowPathExactlyAtCap(t *testing.T) {
	const max = 64
	const bufSize = 16
	// 63 'a' + '\n' == 64 == max.
	r := bufio.NewReaderSize(strings.NewReader(strings.Repeat("a", max-1)+"\n"+"next\n"), bufSize)
	line, truncated, err := readCappedLine(r, max)
	if err != nil || truncated || len(line) != max || line != strings.Repeat("a", max-1)+"\n" {
		t.Fatalf("readCappedLine() = %q(len=%d, truncated=%v), %v; want %d bytes, truncated=false, nil", line, len(line), truncated, err, max)
	}
	line, truncated, err = readCappedLine(r, max)
	if err != nil || truncated || line != "next\n" {
		t.Fatalf("after readCappedLine() = %q(truncated=%v), %v; want %q, false, nil", line, truncated, err, "next\n")
	}
}

// TestReadCappedLineSlowPathExactlyAtCapEOF is the same boundary for a final
// line without a trailing newline ending exactly at maxLineSize and aligned
// with the reader buffer, where bufio reports ErrBufferFull before EOF.
func TestReadCappedLineSlowPathExactlyAtCapEOF(t *testing.T) {
	const max = 64
	const bufSize = 16
	r := bufio.NewReaderSize(strings.NewReader(strings.Repeat("b", max)), bufSize)
	line, truncated, err := readCappedLine(r, max)
	if err != nil || truncated || len(line) != max || line != strings.Repeat("b", max) {
		t.Fatalf("readCappedLine() = %q(len=%d, truncated=%v), %v; want %d b's, truncated=false, nil", line, len(line), truncated, err, max)
	}
}

// TestReadCappedLineSlowPathOneOverCap verifies a line strictly over the cap
// is still reported as truncated even when it spills across the reader buffer.
func TestReadCappedLineSlowPathOneOverCap(t *testing.T) {
	const max = 64
	const bufSize = 16
	// 64 'c' + '\n' == 65 > max.
	r := bufio.NewReaderSize(strings.NewReader(strings.Repeat("c", max)+"\n"+"next\n"), bufSize)
	line, truncated, err := readCappedLine(r, max)
	if err != nil || !truncated || len(line) != max || line != strings.Repeat("c", max) {
		t.Fatalf("readCappedLine() = %q(len=%d, truncated=%v), %v; want %d c's, truncated=true, nil", line, len(line), truncated, err, max)
	}
	line, truncated, err = readCappedLine(r, max)
	if err != nil || truncated || line != "next\n" {
		t.Fatalf("after readCappedLine() = %q(truncated=%v), %v; want %q, false, nil", line, truncated, err, "next\n")
	}
}

// TestReadCappedLineFinalFragmentTruncationAtEOF pins the case where the cap is
// crossed inside the final buffered fragment and EOF follows immediately with
// no trailing newline: the bytes dropped by the cap must still be reported even
// though drainLine then has nothing left to discard. The cap (8) is not a
// multiple of the reader buffer (16), so ReadSlice returns the whole 16-byte
// line with ErrBufferFull and appendCapped drops half of it.
func TestReadCappedLineFinalFragmentTruncationAtEOF(t *testing.T) {
	const max = 8
	const bufSize = 16
	r := bufio.NewReaderSize(strings.NewReader(strings.Repeat("d", bufSize)), bufSize)
	line, truncated, err := readCappedLine(r, max)
	if err != nil || !truncated || len(line) != max || line != strings.Repeat("d", max) {
		t.Fatalf("readCappedLine() = %q(len=%d, truncated=%v), %v; want %d d's, truncated=true, nil", line, len(line), truncated, err, max)
	}
}
