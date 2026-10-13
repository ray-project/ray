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

package proctitle

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

// SetTitle's prctl(PR_SET_NAME) targets the *calling thread's* comm. /proc/self/comm
// always reflects the thread-group leader's comm, which a test goroutine is not
// guaranteed to run on. So pin the test goroutine to its thread and read that
// exact thread's comm via /proc/self/task/<tid>/comm — deterministic regardless
// of which OS thread the test lands on.
//
// (In the real worker this is not an issue: GoExecuteTask is reached from C++
// RunTaskExecutionLoop off the Go main goroutine's single call stack, i.e. the
// leader thread, so the task titles show up in ps/psutil like Python does.)
func selfThreadComm(t *testing.T) string {
	t.Helper()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	b, err := os.ReadFile(fmt.Sprintf("/proc/self/task/%d/comm", syscall.Gettid()))
	if err != nil {
		t.Skipf("no /proc/self/task/<tid>/comm: %v", err)
	}
	return strings.TrimSpace(string(b))
}

// selfCmdline returns the current /proc/self/cmdline split by NUL.
func selfCmdline(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile("/proc/self/cmdline")
	if err != nil {
		t.Skipf("no /proc/self/cmdline: %v", err)
	}
	parts := strings.Split(string(b), "\x00")
	// Drop the trailing NUL-produced empty element.
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func TestInitCapturesArgvAndSetIdle(t *testing.T) {
	// SetTitle("ray::IDLE") must (1) update /proc/self/comm (prctl) and
	// (2) overwrite the argv cmdline area so no sensitive args remain.
	//
	// Note: Go's os.Args strings are copies owned by the Go runtime, so they
	// are NOT clobbered by the argv-area overwrite; only the real argv memory
	// (read via /proc/self/cmdline) is.
	Init()
	SetTitle(kIdleProcessTitle)

	if got := selfThreadComm(t); got != "ray::IDLE" {
		t.Fatalf("/proc/self/comm = %q, want %q", got, kIdleProcessTitle)
	}

	cmdline := selfCmdline(t)
	if len(cmdline) == 0 {
		t.Fatal("empty cmdline")
	}
	if cmdline[0] != "ray::IDLE" {
		t.Fatalf("cmdline[0] = %q, want %q (argv area not clobbered)", cmdline[0], kIdleProcessTitle)
	}
	// The rest of the argv block must be NUL-padded (no leftover argv entries),
	// which is exactly what scrubs secrets from the worker command line.
	for i, c := range cmdline[1:] {
		if c != "" {
			t.Fatalf("cmdline entry %d = %q, want empty (argv area not zero-filled)", i+1, c)
		}
	}
}

func TestGuardRestoresIdle(t *testing.T) {
	// Begin(title="ray::DoWork", next="ray::IDLE") then End must restore IDLE.
	Init()
	g := Begin("ray::DoWork", kIdleProcessTitle)
	if got := selfThreadComm(t); got != "ray::DoWork" {
		t.Fatalf("/proc/self/comm during task = %q, want %q", got, "ray::DoWork")
	}
	g.End()
	if got := selfThreadComm(t); got != kIdleProcessTitle {
		t.Fatalf("/proc/self/comm after End = %q, want %q", got, kIdleProcessTitle)
	}
}

func TestGuardRestoresActorClass(t *testing.T) {
	// For actor tasks the restore title is the actor class (ray::MyActor).
	Init()
	g := Begin("ray::MyActor.DoWork", "ray::MyActor")
	g.End()
	if got := selfThreadComm(t); got != "ray::MyActor" {
		t.Fatalf("/proc/self/comm after End = %q, want %q", got, "ray::MyActor")
	}
}

// TestScrubsSensitiveArg verifies that after SetTitle the original cmdline no
// longer contains secrets (e.g. --redis-password). The secret is injected via
// the TEST_PROCTITLE_SECRET env var (the Bazel test runner can set it); the
// test then re-runs Init+Set and asserts the secret no longer appears in
// /proc/self/cmdline. Without the env var the test skips.
//
// Note: this test intentionally reads /proc/self/cmdline (the real argv
// memory area) rather than os.Args/flag.Args. Strings returned by Go's
// os.Args/flag.Args point into the same argv area our clobber overwrites, so
// they become garbage after SetTitle — that is expected and is precisely how
// the scrub works.
func TestScrubsSensitiveArg(t *testing.T) {
	secret := os.Getenv("TEST_PROCTITLE_SECRET")
	if secret == "" {
		t.Skip("TEST_PROCTITLE_SECRET not set; set it to a value in the launch command line")
	}
	Init()
	SetTitle(kIdleProcessTitle)
	for _, a := range selfCmdline(t) {
		if strings.Contains(a, secret) {
			t.Fatalf("secret still visible in cmdline: %q", a)
		}
	}
}
