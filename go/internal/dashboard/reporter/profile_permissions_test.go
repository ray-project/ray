package reporter

import (
	"strings"
	"testing"
)

// stubOSName pins the platform the permission guidance renders for, so the
// darwin/linux chown variants can both be exercised on any host.
func stubOSName(t *testing.T, name string) {
	t.Helper()
	orig := osName
	osName = name
	t.Cleanup(func() { osName = orig })
}

// TestFormatFailedCommandWithoutPermissionHint pins the message shape for a
// failure whose stderr is not a permission error. The expected string is the
// exact output of Python's _format_failed_profiler_command for the same inputs.
func TestFormatFailedCommandWithoutPermissionHint(t *testing.T) {
	got := formatFailedCommand("memray attach 7", "memray", CommandResult{
		Stderr:   "boom",
		Stdout:   "some stdout",
		ExitCode: 1,
	})

	// Python: 'Failed to execute `memray attach 7`.\n\n=== stderr ===\nboom\n\n=== stdout ===\nsome stdout\n'
	want := "Failed to execute `memray attach 7`.\n" +
		"\n=== stderr ===\n" +
		"boom\n" +
		"\n=== stdout ===\n" +
		"some stdout\n"
	if got != want {
		t.Errorf("formatFailedCommand =\n%q\nwant\n%q", got, want)
	}
	if strings.Contains(got, "root permissions") {
		t.Error("non-permission stderr must not carry the permission guidance")
	}
}

// TestFormatFailedCommandPermissionHintLinux pins the permission-hint fix: a stderr that
// mentions permission gains the full guidance block with the linux chown form.
// The expected string is the exact output of Python's
// _format_failed_profiler_command for the same inputs.
func TestFormatFailedCommandPermissionHintLinux(t *testing.T) {
	stubOSName(t, "linux")

	got := formatFailedCommand("py-spy record -o /tmp/x.svg -p 42", "py-spy", CommandResult{
		Stderr:   "Permission denied (os error 13)",
		ExitCode: 1,
	})

	want := "Failed to execute `py-spy record -o /tmp/x.svg -p 42`.\n" +
		"\nNote that this command requires `py-spy` to be installed with root permissions. You\n" +
		"can install `py-spy` and give it root permissions as follows:\n" +
		"  $ pip install py-spy\n" +
		"  $ sudo chown root:root `which py-spy`\n" +
		"  $ sudo chmod u+s `which py-spy`\n" +
		"\nAlternatively, you can start Ray with passwordless sudo / root permissions.\n" +
		"\n\n" +
		"=== stderr ===\n" +
		"Permission denied (os error 13)\n" +
		"\n=== stdout ===\n" +
		"\n"
	if got != want {
		t.Errorf("formatFailedCommand =\n%q\nwant\n%q", got, want)
	}
}

// TestFormatFailedCommandPermissionHintDarwin covers the other chown variant:
// Python uses `sudo chown root:` without a group on macOS.
func TestFormatFailedCommandPermissionHintDarwin(t *testing.T) {
	stubOSName(t, "darwin")

	got := formatFailedCommand("py-spy dump -p 1", "py-spy", CommandResult{
		Stderr:   "permission",
		ExitCode: 1,
	})
	if !strings.Contains(got, "sudo chown root: `which py-spy`") {
		t.Errorf("darwin guidance must use the group-less chown form, got:\n%s", got)
	}
	if strings.Contains(got, "sudo chown root:root") {
		t.Errorf("darwin guidance must not use the linux chown form, got:\n%s", got)
	}
}

// TestFormatFailedCommandPermissionIsCaseInsensitive verifies the match on
// "permission" mirrors Python's stderr_str.lower() check, so an uppercase errno
// banner still gets the guidance.
func TestFormatFailedCommandPermissionIsCaseInsensitive(t *testing.T) {
	stubOSName(t, "linux")

	for _, stderr := range []string{
		"PERMISSION ERROR",
		"Permission denied",
		"operation requires permission",
	} {
		got := formatFailedCommand("py-spy record -p 1", "py-spy", CommandResult{
			Stderr:   stderr,
			ExitCode: 1,
		})
		if !strings.Contains(got, "Note that this command requires `py-spy`") {
			t.Errorf("stderr %q did not produce the permission guidance:\n%s", stderr, got)
		}
	}
}

// TestFormatFailedCommandEmitsEmptySections pins the sections Python always
// prints. An earlier Go version dropped a section whose stream was empty, which
// made the message diverge from Python for the common case of a command that
// writes to stderr only.
func TestFormatFailedCommandEmitsEmptySections(t *testing.T) {
	got := formatFailedCommand("memray attach 7", "memray", CommandResult{
		Stderr:   "boom",
		ExitCode: 1,
	})
	if !strings.HasSuffix(got, "\n\n=== stdout ===\n\n") {
		t.Errorf("message must end with an empty stdout section, got:\n%q", got)
	}
	if !strings.Contains(got, "\n=== stderr ===\nboom\n") {
		t.Errorf("message must carry the stderr section, got:\n%q", got)
	}
}

// TestFormatFailedCommandNamesTheProfiler verifies the profiler argument is what
// appears in the guidance, not a hard-coded name, so the py-spy and memray call
// sites each render their own binary.
func TestFormatFailedCommandNamesTheProfiler(t *testing.T) {
	stubOSName(t, "linux")

	got := formatFailedCommand("memray attach 7", "memray", CommandResult{
		Stderr:   "permission denied",
		ExitCode: 1,
	})
	for _, want := range []string{
		"requires `memray` to be installed",
		"pip install memray",
		"chown root:root `which memray`",
		"chmod u+s `which memray`",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("guidance for memray must contain %q, got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "py-spy") {
		t.Errorf("guidance for memray must not mention py-spy, got:\n%s", got)
	}
}

// TestProfilerPermissionsHintEmptyWhenNoMatch keeps the helper honest for the
// no-match case, which the message tests cover only indirectly.
func TestProfilerPermissionsHintEmptyWhenNoMatch(t *testing.T) {
	stubOSName(t, "linux")

	if got := profilerPermissionsHint("py-spy", "no such file"); got != "" {
		t.Errorf("hint for a non-permission stderr = %q, want empty", got)
	}
	if got := profilerPermissionsHint("py-spy", ""); got != "" {
		t.Errorf("hint for empty stderr = %q, want empty", got)
	}
	if got := profilerPermissionsHint("py-spy", "Permission denied"); got == "" {
		t.Error("hint for a permission stderr must not be empty")
	}
}

// TestCallSitesPassTheRightProfiler name closes the gap a direct
// formatFailedCommand test leaves open: each profiler call site must hand over
// its own binary name, otherwise the memray guidance would tell the user to
// install py-spy. The failure text is driven end to end through the manager so
// a wrong literal at any call site fails here.
func TestCallSitesPassTheRightProfilerName(t *testing.T) {
	stubOSName(t, "linux")
	stubPasswordlessSudo(t, false)

	for _, c := range []struct {
		name        string
		run         func(m *ProfileManager) string
		wantHas     string
		wantNotHave string
	}{
		{
			name: "TraceDump uses py-spy",
			run: func(m *ProfileManager) string {
				_, out := m.TraceDump(42, false, false)
				return out
			},
			wantHas:     "pip install py-spy",
			wantNotHave: "memray",
		},
		{
			name: "CPUProfile uses py-spy",
			run: func(m *ProfileManager) string {
				_, out := m.CPUProfile(42, "flamegraph", 1, false, false)
				return out
			},
			wantHas:     "pip install py-spy",
			wantNotHave: "memray",
		},
		{
			name: "MemoryProfileAttach uses memray",
			run: func(m *ProfileManager) string {
				_, _, out := m.MemoryProfileAttach(42, false, false, false)
				return out
			},
			wantHas:     "pip install memray",
			wantNotHave: "py-spy",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			// The profiler must resolve, otherwise the not-installed pre-check
			// short-circuits before the failure formatting under test.
			stubLookPath(t)

			orig := runCommandDetail
			defer func() { runCommandDetail = orig }()
			runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{
				Stderr:   "Permission denied",
				ExitCode: 1,
			})

			out := c.run(newTestManager(t, t.TempDir()))
			if !strings.Contains(out, "Note that this command requires") {
				t.Fatalf("output lacks the permission guidance:\n%s", out)
			}
			if !strings.Contains(out, c.wantHas) {
				t.Errorf("output must contain %q, got:\n%s", c.wantHas, out)
			}
			if strings.Contains(out, c.wantNotHave) {
				t.Errorf("output must not mention %q, got:\n%s", c.wantNotHave, out)
			}
		})
	}
}
