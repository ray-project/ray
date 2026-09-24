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
	"os"
	"strconv"
)

type logFileInfo struct {
	filename           string
	sizeWhenLastOpened int64
	filePosition       int64
	// consumedBytes counts bytes read through reader since the current
	// handle was opened; after each poll it becomes filePosition, so
	// buffered-but-unread lines do not advance the recorded position.
	consumedBytes int64
	fileHandle    *os.File
	// reader persists across polls so lines left in its buffer when
	// maxLinesPerRead ends a poll are delivered on the next poll.
	reader       *bufio.Reader
	isErrFile    bool
	jobID        string
	workerPID    *int
	componentPID string
	actorName    string
	taskName     string
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (f *logFileInfo) currentSize() (int64, error) {
	info, err := os.Stat(f.filename)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (f *logFileInfo) openAtCurrentPosition() error {
	handle, err := os.Open(f.filename)
	if err != nil {
		return err
	}
	if _, err := handle.Seek(f.filePosition, 0); err != nil {
		handle.Close()
		return err
	}
	f.fileHandle = handle
	// The new handle starts reading at filePosition, so buffered reader
	// state from a previous handle is invalid.
	f.reader = nil
	f.consumedBytes = f.filePosition
	return nil
}

func (f *logFileInfo) reopenIfNecessary() error {
	if f.fileHandle == nil {
		return f.openAtCurrentPosition()
	}

	currentInfo, err := os.Stat(f.filename)
	if err != nil {
		return err
	}
	openInfo, err := f.fileHandle.Stat()
	if err != nil {
		return err
	}

	needReopen := !os.SameFile(currentInfo, openInfo)
	// The file may be truncated and rewritten in place without changing its
	// inode. The new size can land anywhere below the last observed size,
	// including between the read position and the size seen when the file
	// was opened, so compare against both (matches log_monitor.py).
	rewindToStart := currentInfo.Size() < max64(f.filePosition, f.sizeWhenLastOpened)
	if needReopen || rewindToStart {
		_ = f.fileHandle.Close()
		f.fileHandle = nil
		if rewindToStart {
			f.filePosition = 0
		}
		if err := f.openAtCurrentPosition(); err != nil {
			return err
		}
		f.sizeWhenLastOpened = currentInfo.Size()
		return nil
	}
	f.sizeWhenLastOpened = currentInfo.Size()
	return nil
}

func (f *logFileInfo) publishPID() string {
	if f.componentPID != "" {
		return f.componentPID
	}
	if f.workerPID == nil {
		return ""
	}
	return strconv.Itoa(*f.workerPID)
}
