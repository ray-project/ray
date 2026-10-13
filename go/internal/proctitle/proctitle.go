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

// Package proctitle mirrors C++'s process_title_guard and Python's
// setproctitle for Go workers: it overwrites the process title so that
// ps/psutil show "ray::IDLE" / "ray::<task>" / "ray::<actor class>", and
// scrubs sensitive argv (e.g. --redis-password) that the raylet passed on
// the worker command line.
//
// Go does not expose argc/argv to user code, so the C kernel below locates
// the original argv[0] string area by walking backward from environ[0] (the
// kernel places argv and environ as two contiguous string blocks on the
// initial stack; this is the same technique spt_setup uses for Python's
// setproctitle on Linux). If that walk fails, the package degrades to
// prctl(PR_SET_NAME) only (/proc/self/comm), never panicking.
package proctitle

/*
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>

extern char **environ;

// Captured once by proctitleInit(): the start of the original argv string
// area and its size, so later proctitleSet() calls can overwrite the process
// title in place. Read without synchronization because Init() runs early in
// command execution before any concurrent goroutine exists.
static char *g_proctitle_argv0 = NULL;
static size_t g_proctitle_argv0_len = 0;

// proctitleInit finds the original argv string area by walking back from
// environ[0]. nargs must equal len(/proc/self/cmdline split by '\0').
// Strings are encountered nearest-first (environ, then argv backwards), so
// the last string found is argv[0], the start of the string block. Returns 1
// on success; 0 on any failure (layout not as expected), in which case
// proctitleSet degrades to prctl-only.
static int proctitleInit(int nargs) {
  if (nargs <= 0) return 0;
  const char *env0 = environ[0];
  if (!env0) return 0;
  const char *limit = env0 - (96 * 1024);  // ARG_MAX-style safety bound
  const char *end = env0;
  const char *arg0 = NULL;
  for (int iter = 0; iter < nargs; iter++) {
    if (end <= limit) return 0;
    const char *s_end = end - 1;
    if (*s_end != '\0') return 0;  // not contiguous -> abort
    const char *s = s_end;
    while (s > limit && s[-1] != '\0') s--;
    arg0 = s;
    if (s == limit) {
      // Reached the lower bound early: walking further would read past the
      // bound, so no argv area is captured. Report failure (0) rather than
      // claiming success without setting g_proctitle_argv0; proctitleSet then
      // degrades to prctl-only.
      return 0;
    }
    end = s;
  }
  // arg0 must be the start of the string block (nothing before it).
  if (arg0 > limit && arg0[-1] != '\0') return 0;
  g_proctitle_argv0 = (char *)arg0;
  g_proctitle_argv0_len = (size_t)(env0 - arg0);
  return 1;
}

// proctitleSet sets the process title: always prctl(PR_SET_NAME)
// (/proc/self/comm, <=15 bytes), and if the argv area was captured, also
// overwrites it in place (visible to ps aux / psutil cmdline(), scrubbing any
// secrets in the original command line). Mirrors C++
// ProcessTitleGuard::SetProcTitle.
static void proctitleSet(const char *title) {
  prctl(PR_SET_NAME, title, 0, 0, 0);
  if (!g_proctitle_argv0) return;
  size_t len = strlen(title);
  if (len >= g_proctitle_argv0_len) {
    len = g_proctitle_argv0_len > 0 ? g_proctitle_argv0_len - 1 : 0;
  }
  memcpy(g_proctitle_argv0, title, len);
  memset(g_proctitle_argv0 + len, 0, g_proctitle_argv0_len - len);
}
*/
import "C"

import (
	"os"
	"strings"
	"sync"
	"unsafe"
)

// kIdleProcessTitle matches Python's WORKER_PROCESS_TYPE_IDLE_WORKER and
// C++'s kIdleProcessTitle.
const kIdleProcessTitle = "ray::IDLE"

// IdleTitle returns the idle worker process title ("ray::IDLE").
func IdleTitle() string { return kIdleProcessTitle }

// proctitleMu serializes title updates. Python holds a global lock in its
// setproctitle; C++ notes its guard is single-threaded. Go workers execute
// tasks on concurrent goroutines, so the atomicity of each write matters even
// though concurrent tasks legitimately race for the final title.
var proctitleMu sync.Mutex

// Init captures the original argv area. It is idempotent and fail-soft: on
// failure SetTitle() continues to update /proc/self/comm only. nargs is
// derived from /proc/self/cmdline so the C walk knows how many argv strings
// to look for.
func Init() {
	nargs := 0
	if cmdline, err := os.ReadFile("/proc/self/cmdline"); err == nil {
		// /proc/self/cmdline is NUL-separated with a trailing NUL.
		nargs = len(strings.Split(string(cmdline), "\x00")) - 1
	}
	if nargs > 0 {
		C.proctitleInit(C.int(nargs))
	}
}

// SetTitle sets the process title to title (e.g. "ray::IDLE").
func SetTitle(title string) {
	proctitleMu.Lock()
	defer proctitleMu.Unlock()
	ctitle := C.CString(title)
	defer C.free(unsafe.Pointer(ctitle))
	C.proctitleSet(ctitle)
}

// Guard is an RAII helper mirroring C++ ProcessTitleGuard / Python
// _changeproctitle: SetTitle(title) now, SetTitle(nextTitle) on End (or
// defer End()).
type Guard struct {
	nextTitle string
}

// Begin sets title immediately and returns a Guard that will restore
// nextTitle when End() is called.
func Begin(title, nextTitle string) *Guard {
	SetTitle(title)
	return &Guard{nextTitle: nextTitle}
}

// End restores the guard's next title.
func (g *Guard) End() {
	if g != nil {
		SetTitle(g.nextTitle)
	}
}
