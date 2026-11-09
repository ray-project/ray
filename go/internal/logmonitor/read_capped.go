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
)

// readCappedLine reads one line (including the trailing '\n') from r without
// letting bufio grow its internal buffer to the line size. If the line exceeds
// maxLineSize bytes, the returned line is truncated to maxLineSize, the
// returned truncated flag is true, and the remainder of the line is drained so
// the next read starts at the following line. It returns io.EOF with the
// partial line when the input ends without a final newline.
func readCappedLine(r *bufio.Reader, maxLineSize int) (string, bool, error) {
	frag, err := r.ReadSlice('\n')
	if err == nil {
		// Common short-line path: convert to a string directly. This copies
		// the bytes out of the bufio buffer (safe because bufio reuses its
		// internal buffer) in a single allocation.
		if len(frag) <= maxLineSize {
			return string(frag), false, nil
		}
		return string(frag[:maxLineSize]), true, nil
	}
	// Preallocate close to the fragment length (but no more than the reader
	// buffer or the cap) and cap every append at maxLineSize so a long line
	// never grows its backing array far beyond the cap. The common case here is
	// an actively-written file whose final line has no trailing newline, so the
	// fragment is short and allocating a full 16 KiB per file per pass would be
	// wasted churn.
	prealloc := min(maxLineSize, defaultReadBufferSize)
	if len(frag) < prealloc {
		prealloc = max(len(frag)+8, 64)
	}
	line := make([]byte, 0, prealloc)
	var capped bool
	line, capped = appendCapped(line, frag, maxLineSize)
	for {
		switch err {
		case bufio.ErrBufferFull:
			// The line is still being read (no '\n' yet) and the buffer is at
			// the cap: drain the remainder, reporting truncation only when
			// bytes were actually discarded. A line ending exactly at
			// maxLineSize with no remainder is complete, not truncated.
			if len(line) >= maxLineSize {
				if drainLine(r) {
					return string(line[:maxLineSize]), true, nil
				}
				// Nothing left to discard, but appendCapped may already have
				// dropped bytes inside the final fragment; report that.
				return string(line), capped, nil
			}
		case io.EOF:
			return string(line), capped, err
		default:
			return string(line), capped, err
		}
		frag, err = r.ReadSlice('\n')
		line, capped = appendCapped(line, frag, maxLineSize)
		if err == nil {
			return string(line), capped, nil
		}
	}
}

// appendCapped appends src to dst but never lets the result exceed maxBytes,
// so the line buffer stays bounded while the over-long remainder is drained.
// It reports whether any bytes were dropped because of the cap.
func appendCapped(dst, src []byte, maxBytes int) ([]byte, bool) {
	if room := maxBytes - len(dst); room > 0 {
		if len(src) > room {
			return append(dst, src[:room]...), true
		}
		return append(dst, src...), false
	}
	return dst, len(src) > 0
}

// drainLine discards the remainder of an over-long line up to and including
// the next '\n', leaving the reader positioned at the start of the next line.
// It reports whether any bytes were discarded.
func drainLine(r *bufio.Reader) bool {
	drained := false
	for {
		frag, err := r.ReadSlice('\n')
		if len(frag) > 0 {
			drained = true
		}
		if err == nil || err == io.EOF {
			return drained
		}
		if err != bufio.ErrBufferFull {
			return drained
		}
	}
}
