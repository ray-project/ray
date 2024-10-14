/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package log

import (
	"sync"

	"github.com/go-logr/logr"
)

// KubeAPIWarningLoggerOptions controls the behavior
// of a rest.WarningHandler constructed using NewKubeAPIWarningLogger().
type KubeAPIWarningLoggerOptions struct {
	// Deduplicate indicates a given warning message should only be written once.
	// Setting this to true in a long-running process handling many warnings can
	// result in increased memory use.
	Deduplicate bool
}

// KubeAPIWarningLogger is a wrapper around
// a provided logr.Logger that implements the
// rest.WarningHandler interface.
type KubeAPIWarningLogger struct {
	// logger is used to log responses with the warning header
	logger logr.Logger
	// opts contain options controlling warning output
	opts KubeAPIWarningLoggerOptions
	// writtenLock gurads written
	writtenLock sync.Mutex
	// used to keep track of already logged messages
	// and help in de-duplication.
	written map[string]struct{}
}

// HandleWarningHeader handles logging for responses from API server that are
// warnings with code being 299 and uses a logr.Logger for its logging purposes.
func (l *KubeAPIWarningLogger) HandleWarningHeader(code int, agent string, message string) {
	if code != 299 || len(message) == 0 {
		return
	}

	if l.opts.Deduplicate {
		l.writtenLock.Lock()
		defer l.writtenLock.Unlock()

		if _, alreadyLogged := l.written[message]; alreadyLogged {
			return
		}
		l.written[message] = struct{}{}
	}
	l.logger.Info(message)
}

// NewKubeAPIWarningLogger returns an implementation of rest.WarningHandler that logs warnings
// with code = 299 to the provided logr.Logger.
func NewKubeAPIWarningLogger(l logr.Logger, opts KubeAPIWarningLoggerOptions) *KubeAPIWarningLogger {
	h := &KubeAPIWarningLogger{logger: l, opts: opts}
	if opts.Deduplicate {
		h.written = map[string]struct{}{}
	}
	return h
}
