# Changes to `signal_test.cc`: Fixing Signal Test Failures on macOS

## Test Fixed

**`//src/ray/util/tests:signal_test`** — all 5 subtests. Before the fix, the entire test binary failed in 0.7s (killed by a stray SIGTERM). After the fix, all 5 subtests pass in 1.4s.

The 5 subtests are:
- `SignalTest.SendTermSignalTest`
- `SignalTest.SendBusSignalTest`
- `SignalTest.SIGABRT_Test`
- `SignalTest.SIGSEGV_Test`
- `SignalTest.SIGILL_Test`

## What the Test Does

The test verifies that Ray's failure signal handler (`AbslFailureSignalHandler`, installed via `ray::RayLog::InstallFailureSignalHandler`) correctly logs stack traces when a process receives fatal signals. Each subtest follows the same pattern:

1. The test binary's `main()` installs `AbslFailureSignalHandler`, which intercepts SIGTERM, SIGBUS, SIGABRT, SIGSEGV, and SIGILL.
2. The subtest calls `fork()` to create a child process.
3. The child either spins in an infinite loop (for `SendTermSignalTest` and `SendBusSignalTest`) or triggers a signal directly (`std::abort()`, illegal memory write, `raise(SIGILL)`).
4. The parent sleeps 100ms, then sends a signal to the child via `kill(pid, signal)`.
5. The child's inherited `AbslFailureSignalHandler` fires, prints a stack trace to stderr, and terminates the child.
6. The parent sleeps another 100ms and returns.

The test has no explicit assertions beyond `ASSERT_GE(pid, 0)` (fork succeeded). Success means the parent process survives to completion — the test is a smoke test that the signal handler logs output without crashing the parent.

## Why the Test Failed on macOS

After `fork()`, the child process inherits the parent's signal handlers, including `AbslFailureSignalHandler`. When the parent sends SIGTERM to the child (in `SendTermSignalTest`), the following happens:

1. The child's `AbslFailureSignalHandler` fires and logs the stack trace.
2. The handler resets the signal disposition to `SIG_DFL` and calls `raise(SIGTERM)` to re-deliver the signal with the default handler, which terminates the child.

On macOS, the parent and child share the same **process group** after `fork()`. When the child terminates due to a signal, macOS can propagate signal-related events to other processes in the same process group. The test log confirmed this: both the child PID and the parent PID received SIGTERM at the exact same timestamp. The parent's own `AbslFailureSignalHandler` then fired, printed a stack trace, and terminated the parent — causing the test binary to exit with a non-zero status.

On Linux, this propagation does not occur. The child's signal termination only generates `SIGCHLD` to the parent (which is ignored by default), and the parent continues unaffected.

The test log showed two interleaved stack traces:
- One from the child, with PC at `__kill` (inside the busy loop, receiving the signal)
- One from the parent, with PC at `__semwait_signal` / `nanosleep` (sleeping in the `Sleep()` call, receiving the unexpected SIGTERM)

## What the Fix Does

### Change 1: `setpgid(0, 0)` in the child process

```cpp
if (pid == 0) {
    setpgid(0, 0);  // new
    while (true) { ... }
}
```

Immediately after `fork()`, the child calls `setpgid(0, 0)`, which places it into its own process group (with PGID equal to its own PID). This isolates the child from the parent's process group. When the child is later killed by a signal, any process-group-level signal propagation on macOS stays within the child's group and does not reach the parent.

This call is applied to all 5 subtests: `SendTermSignalTest`, `SendBusSignalTest`, `SIGABRT_Test`, `SIGSEGV_Test`, and `SIGILL_Test`.

### Change 2: `waitpid(pid, nullptr, WNOHANG)` in the parent

```cpp
    Sleep();
    waitpid(pid, nullptr, WNOHANG);  // new
}
```

After the parent sends the signal and sleeps, it calls `waitpid()` with `WNOHANG` to reap the terminated child. Without this, the child becomes a zombie process — it has exited but its entry remains in the process table until the parent collects its exit status. The `WNOHANG` flag makes the call non-blocking: if the child hasn't exited yet (unlikely after 100ms), the call returns immediately without waiting.

### Change 3: `#include <sys/wait.h>`

Added at the top of the file to provide the `waitpid()` and `WNOHANG` declarations.

## Why This Doesn't Break Linux

On Linux, the child is already isolated from signal propagation after `fork()` — a child dying from a signal does not send that signal to the parent. The `setpgid(0, 0)` call is a no-op in terms of observable behavior: the child gets its own process group, but since there was no cross-group signal leakage on Linux to begin with, nothing changes.

The `waitpid()` call is purely beneficial on both platforms — it prevents zombie accumulation regardless of OS. The original code left zombies on both Linux and macOS; this was harmless for a short-lived test binary, but the fix is correct hygiene.

## Production Impact

This change is test-only. The modified file is `src/ray/util/tests/signal_test.cc`, which is a test binary and not part of the Ray runtime. There is no impact on production code.
