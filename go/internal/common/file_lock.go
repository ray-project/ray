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


package common

import (
	"context"
	"time"

	"github.com/gofrs/flock"
)

type AsyncFileLock struct {
	lock *flock.Flock
}

func NewAsyncFileLock(lockFile string) *AsyncFileLock {
	return &AsyncFileLock{
		lock: flock.New(lockFile),
	}
}

// Acquire acquires the lock using TryLockContext, periodically retrying until
// the lock is acquired or the context is cancelled.
func (l *AsyncFileLock) Acquire(ctx context.Context) error {
	// Retry acquiring the lock at a 100ms interval.
	retryDuration := 100 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			acquired, err := l.lock.TryLockContext(ctx, retryDuration)
			if err != nil {
				return err
			}
			if acquired {
				return nil
			}
		}
	}
}

func (l *AsyncFileLock) Release() {
	if l.lock != nil {
		_ = l.lock.Unlock()
	}
}
