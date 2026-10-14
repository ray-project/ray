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

package event

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ray-project/ray/go/pkg/log"
)

// monitorFile tracks the last observed size, mtime and read position of an
// event log file.
type monitorFile struct {
	size     int64
	mtime    int64
	position int64
}

// monitor holds the file monitoring state backing monitorEvents.
type monitor struct {
	eventDir   string // events directory being scanned
	startMtime int64  // only files modified after this are monitored
	mu         sync.Mutex
	files      map[string]monitorFile // key: inode (or path) -> read cursor
	semaphore  chan struct{}          // bounds concurrent reads per scan
}

func newMonitor(eventDir string) *monitor {
	return &monitor{
		eventDir:   eventDir,
		startMtime: time.Now().Add(scanEventStartOffsetSeconds * time.Second).UnixNano(),
		files:      make(map[string]monitorFile),
		semaphore:  make(chan struct{}, concurrentReadLimit),
	}
}

// getSourceFiles returns the event log files whose name matches a known
// source type and whose mtime is newer than startMtime.
func (m *monitor) getSourceFiles() ([]string, error) {
	entries, err := os.ReadDir(m.eventDir)
	if err != nil {
		return nil, err
	}

	var sourceFiles []string
	for _, entry := range entries {
		name := entry.Name()
		if !isEventLogName(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			log.Log.V(1).Info("Skipped event log file as its stat failed",
				"file", filepath.Join(m.eventDir, name), "reason", err.Error())
			continue
		}
		if info.ModTime().UnixNano() <= m.startMtime {
			continue
		}
		sourceFiles = append(sourceFiles, filepath.Join(m.eventDir, name))
	}
	return sourceFiles, nil
}

// isEventLogName reports whether name matches "*{source_type}*.log" for one
// of the known source types, so a stray *.log file is ignored.
func isEventLogName(name string) bool {
	if !strings.HasSuffix(name, ".log") {
		return false
	}
	for _, sourceType := range eventSourceAll {
		if strings.Contains(name, sourceType) {
			return true
		}
	}
	return false
}

// readMonitorFile reads the new lines appended to file since its last read
// position and records the updated size/mtime/position. An empty or unchanged
// file is skipped; a known file resumes from its recorded position; a new
// file is read from the beginning.
func (m *monitor) readMonitorFile(file string) ([]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	// Skip empty files.
	if info.Size() <= 0 {
		return nil, nil
	}
	key := fileKey(f, file)
	m.mu.Lock()
	prev, known := m.files[key]
	m.mu.Unlock()
	position := prev.position
	if known {
		// No change: skip reading.
		if prev.position == prev.size && prev.size == info.Size() && prev.mtime == info.ModTime().UnixNano() {
			log.Log.V(1).Info("Skip reading the event log file because there is no change", "file", file)
			return nil, nil
		}
		// Truncated below the cursor (rotated in place): re-read from the start.
		if prev.position > info.Size() {
			position = 0
		}
	}

	result := readFileFrom(f, info.Size(), info.ModTime().UnixNano(), position,
		eventReadLineCountLimit, eventReadLineLengthLimit())
	m.mu.Lock()
	m.files[key] = monitorFile{size: result.size, mtime: result.mtime, position: result.position}
	m.mu.Unlock()
	return result.lines, nil
}

// fileKey returns the file's inode when available, or its path otherwise.
func fileKey(f *os.File, path string) string {
	info, err := f.Stat()
	if err == nil {
		if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Ino != 0 {
			return strconv.FormatUint(stat.Ino, 10)
		}
	}
	return path
}

// readFileResult holds the size, mtime and next read position of the file
// plus the lines read.
type readFileResult struct {
	size     int64
	mtime    int64
	position int64
	lines    []string
}

// readBufferPool reuses the 2MB scratch buffer used to locate newlines,
// avoiding a fresh allocation per file per scan.
var readBufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 2*1024*1024)
		return &b
	},
}

// readFileFrom reads up to nLines newline-terminated lines from file starting
// at position and returns them together with the updated metadata. A line
// longer than lengthLimit bytes is dropped, logging a truncated sample of its
// first 100 bytes. The next position is the byte after the last consumed
// newline.
func readFileFrom(f *os.File, size, mtime, position int64, nLines, lengthLimit int) readFileResult {
	lines := make([]string, 0, nLines)
	start := position
	remPtr := readBufferPool.Get().(*[]byte)
	rem := *remPtr
	defer readBufferPool.Put(remPtr)
	for i := 0; i < nLines; i++ {
		// Bound the newline search to start + lengthLimit so a line running
		// past the bound is dropped instead of being re-read on every scan.
		sep, err := findNewline(f, start, start+int64(lengthLimit), rem)
		if err != nil {
			// The file changed while reading (e.g. truncated): stop this scan.
			log.Log.V(1).Info("Read event log file failed", "reason", err.Error())
			break
		}
		if sep < 0 {
			// No newline within lengthLimit: either the line is genuinely
			// overlong, or the file ends mid-line (a half-written tail line,
			// or a final line with no trailing newline). Distinguish with an
			// unbounded search, matching python event_utils._read_file, which
			// stops at the first missing newline and leaves the cursor
			// unchanged so a half-written line is re-read in full once the
			// writer completes it.
			end, err := findNewline(f, start, -1, rem)
			if err != nil {
				break
			}
			if end < 0 {
				// Unterminated tail line: keep the cursor at start; the line
				// is re-read (in full) on the next scan after the newline
				// arrives. Never reported, never skipped.
				break
			}
			// Genuinely overlong line: drop it and skip past its newline.
			truncatedSize := min(100, lengthLimit)
			sample := make([]byte, truncatedSize)
			if _, err := f.ReadAt(sample, start); err == nil {
				log.Log.Error(nil, "Ignored long string in event log file", "sample", string(sample))
			}
			start = end + 1
			continue
		}
		line := make([]byte, sep-start)
		if _, err := f.ReadAt(line, start); err == nil {
			lines = append(lines, string(line))
		}
		start = sep + 1
	}
	return readFileResult{size: size, mtime: mtime, position: start, lines: lines}
}

// findNewline returns the offset of the first '\n' at or after start in f, or
// -1 when the file has no newline past start. When limit >= 0 the search is
// bounded to offsets strictly below limit; -1 scans unbounded. rem is a
// reusable scratch buffer.
func findNewline(f *os.File, start, limit int64, rem []byte) (int64, error) {
	pos := start
	for {
		if limit >= 0 && pos >= limit {
			return -1, nil
		}
		// Never read past the limit.
		readLen := int64(len(rem))
		if limit >= 0 && pos+readLen > limit {
			readLen = limit - pos
		}
		n, err := f.ReadAt(rem[:readLen], pos)
		if idx := bytes.IndexByte(rem[:n], '\n'); idx >= 0 {
			return pos + int64(idx), nil
		}
		if err != nil {
			if err == io.EOF {
				return -1, nil
			}
			return -1, err
		}
		if n == 0 {
			return -1, nil
		}
		pos += int64(n)
	}
}

// monitorEvents continuously scans eventDir for new event lines and delivers
// them to queue. The first scan fires immediately, subsequent scans run on a
// ticker; a scan error is logged and the loop keeps going. The loop ends when
// ctx is cancelled or the queue cannot accept a batch because it is closed.
// Delivery is non-blocking: when the queue is full a batch is dropped and
// counted in droppedBatches (when non-nil) so a slow or hung report loop cannot
// stall event collection.
func monitorEvents(ctx context.Context, eventDir string, queue chan<- []string, droppedBatches *atomic.Int64) error {
	m := newMonitor(eventDir)
	log.Log.Info("Monitor events logs modified after the start mtime",
		"start_mtime", m.startMtime, "event_dir", eventDir)

	// Clamp to a positive interval so a non-positive override cannot panic
	// NewTicker.
	interval := time.Duration(scanEventDirIntervalSeconds()) * time.Second
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	callback := func(lines []string) error {
		select {
		case queue <- lines:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Queue full: drop the batch and keep scanning, decoupling
			// monitoring from the report loop. Dropping is preferred over
			// blocking so one hung dashboard cannot stall the whole event
			// collection chain for minutes.
			if droppedBatches != nil {
				droppedBatches.Add(1)
			}
			log.Log.Error(nil, "Dropped event batch because the report queue is full",
				"dropped_lines", len(lines))
			return nil
		}
	}

	for {
		if err := m.scan(ctx, callback); err != nil {
			log.Log.Error(err, "Error scanning event log files; continuing", "event_dir", eventDir)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// scan lists the event files and reads every changed one, sending each batch
// of lines to callback. Concurrent reads are bounded to avoid exhausting file
// descriptors.
func (m *monitor) scan(ctx context.Context, callback func([]string) error) error {
	sourceFiles, err := m.getSourceFiles()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(sourceFiles))
	for _, file := range sourceFiles {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			select {
			case m.semaphore <- struct{}{}:
				defer func() { <-m.semaphore }()
			case <-ctx.Done():
				return
			}
			lines, err := m.readMonitorFile(file)
			if err != nil {
				errCh <- err
				return
			}
			if len(lines) == 0 {
				return
			}
			log.Log.V(1).Info("Monitor events read a batch of event lines",
				"file", file, "count", len(lines))
			if err := callback(lines); err != nil {
				errCh <- err
			}
		}(file)
	}
	wg.Wait()
	close(errCh)
	var firstErr error
	for err := range errCh {
		if err != nil && firstErr == nil {
			log.Log.Error(err, "Read event file failed")
			firstErr = err
		}
	}
	return firstErr
}
