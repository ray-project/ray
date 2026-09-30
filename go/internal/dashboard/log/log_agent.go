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
	"fmt"
	"io"
	"os"
)

// blockSize is the number of bytes read per block when scanning log files. It
// mirrors BLOCK_SIZE in log_agent.py.
const blockSize = 1 << 16 // 64 KB

// findOffsetOfContentInFile returns the offset of the first occurrence of
// content at or after startOffset, or -1 when content is not present. It
// mirrors the Python reference find_offset_of_content_in_file.
func findOffsetOfContentInFile(f *os.File, content []byte, startOffset int64) (int64, error) {
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return 0, err
	}
	offset := startOffset
	buf := make([]byte, blockSize)
	for {
		m, readErr := f.Read(buf)
		if m > 0 {
			if idx := bytes.Index(buf[:m], content); idx >= 0 {
				return offset + int64(idx), nil
			}
			offset += int64(m)
		}
		if readErr != nil {
			if readErr == io.EOF {
				return -1, nil
			}
			return 0, readErr
		}
	}
}

// findEndOffsetFile returns the size of f without disturbing its read
// position, mirroring find_end_offset_file in log_agent.py.
func findEndOffsetFile(f *os.File) (int64, error) {
	oldPos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if _, err := f.Seek(oldPos, io.SeekStart); err != nil {
		return 0, err
	}
	return end, nil
}

// findEndOffsetNextNLinesFromOffset returns the offset immediately after the
// end of the next n lines starting at startOffset (inclusive), or the
// end-of-file offset when fewer than n lines remain. It mirrors
// find_end_offset_next_n_lines_from_offset in log_agent.py.
func findEndOffsetNextNLinesFromOffset(f *os.File, startOffset, n int64) (int64, error) {
	if n == 0 {
		return f.Seek(0, io.SeekEnd)
	}
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return 0, err
	}
	lineEnd, err := readLineThrough(f)
	if err != nil {
		return 0, err
	}
	for i := int64(1); i < n; i++ {
		lineEnd, err = readLineThrough(f)
		if err != nil {
			return 0, err
		}
	}
	return lineEnd, nil
}

// readLineThrough reads the next line from the current file position and
// returns the offset immediately after it, or the end-of-file offset when the
// file is exhausted. The file pointer is left at the returned offset. It
// mirrors a single `file.readline()` in the Python reference loop.
func readLineThrough(f *os.File) (int64, error) {
	buf := make([]byte, blockSize)
	pos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	consumed := int64(0) // bytes before the current buffer
	for {
		m, readErr := f.Read(buf)
		if m > 0 {
			if idx := bytes.IndexByte(buf[:m], '\n'); idx >= 0 {
				// Stop at the newline exactly like readline(); rewind the file
				// pointer so the next call continues after it.
				end := pos + consumed + int64(idx) + 1
				if _, err := f.Seek(end, io.SeekStart); err != nil {
					return 0, err
				}
				return end, nil
			}
			consumed += int64(m)
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return 0, readErr
		}
	}
	// The line had no terminating newline before EOF: return end of file, and
	// leave the pointer at EOF as readline() would.
	return f.Seek(0, io.SeekEnd)
}

// findStartOffsetLastNLinesFromOffset returns the offset of the first byte of
// the last n lines at or before offset, mirroring
// find_start_offset_last_n_lines_from_offset in log_agent.py. offset is
// exclusive: content at the offset itself is not part of the tail window. An
// offset of -1 means end of file; n == 0 returns offset unchanged.
func findStartOffsetLastNLinesFromOffset(f *os.File, offset int64, n int64, blockSize int64) (int64, error) {
	if offset == -1 {
		end, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		offset = end
	} else if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}

	if n == 0 {
		return offset, nil
	}

	var nbytesFromEnd int64

	// A non-newline-terminated trailing line still counts as one line: if the
	// byte just before offset is not "\n", treat that unterminated line as the
	// last line by decrementing the line count.
	if offset > 0 {
		if _, err := f.Seek(offset-1, io.SeekStart); err != nil {
			return 0, err
		}
		one := make([]byte, 1)
		if nn, err := f.Read(one); err == nil && nn == 1 && one[0] != '\n' {
			n--
		}
	}

	linesMore := n
	readOffset := max(0, offset-blockSize)
	prevOffset := offset
	for linesMore >= 0 && readOffset >= 0 {
		if _, err := f.Seek(readOffset, io.SeekStart); err != nil {
			return 0, err
		}
		toRead := min(blockSize, prevOffset-readOffset)
		blockData := make([]byte, toRead)
		nn, err := f.Read(blockData)
		if err != nil && err != io.EOF {
			return 0, err
		}
		blockData = blockData[:nn]
		numLines := int64(bytes.Count(blockData, []byte{'\n'}))

		if numLines > linesMore {
			// This block holds the first line of the tail window: split away
			// the leading extra lines and keep the remainder as the tail.
			parts := bytes.SplitN(blockData, []byte{'\n'}, int(numLines-linesMore)+1)
			nbytesFromEnd += int64(len(parts[len(parts)-1]))
			break
		}

		linesMore -= numLines
		nbytesFromEnd += int64(len(blockData))
		if readOffset == 0 {
			break
		}
		prevOffset = readOffset
		readOffset = max(0, readOffset-blockSize)
	}

	offsetReadStart := offset - nbytesFromEnd
	if offsetReadStart < 0 {
		return 0, fmt.Errorf("read start offset %d should be non-negative", offsetReadStart)
	}
	return offsetReadStart, nil
}
