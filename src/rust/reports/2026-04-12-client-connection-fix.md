# Changes to `client_connection.cc`: Fixing Disconnect Detection on macOS

## Test Fixed

**`//src/ray/raylet_ipc_client/tests:client_connection_test`** — specifically the subtest `ClientConnectionTest.CheckForClientDisconnects`. Before the fix, 8 of 9 subtests passed and this 1 failed. After the fix, all 9 pass.

## What the Test Does

The test creates three pairs of Unix domain socket connections using `boost::asio::local::connect_pair()`. Each pair has a "client" end and a "server" end. The test then:

1. Verifies no connections are reported as disconnected initially.
2. Calls `shutdown(client1->GetNativeHandle(), SHUT_RDWR)` to close one client's socket.
3. Calls `CheckForClientDisconnects()` on the three server-side connections and asserts that only `server1` (the peer of the shut-down client) is reported as disconnected.
4. Repeats the check 10 times to verify consistency.
5. Shuts down the remaining two clients and verifies all three are now reported as disconnected.

The test was failing at step 3 — `disconnects[1]` was `false` when it should have been `true`.

## Why the Test Failed on macOS

The original `CheckForClientDisconnects()` implementation relied exclusively on `POLLHUP` (hang-up) events from the `poll()` system call:

```cpp
// Original code
poll_fds[i] = {fd, /*events=*/0, /*revents=*/0};  // don't request any events
// ...
if (poll_fds[i].revents & POLLHUP) {               // check for POLLHUP
    result[i] = true;
}
```

This works on **Linux**, where the kernel delivers `POLLHUP` to the server end of a Unix domain socket when the client calls `shutdown()` or `close()`. However, **macOS (Darwin) does not deliver `POLLHUP` for Unix domain sockets** in this scenario. This is a well-known behavioral difference between Linux and macOS in their `poll()` implementations.

On macOS, when one end of a Unix domain socket is shut down, the peer receives a `POLLIN` event instead — indicating there is something to read. A subsequent `read()` or `recv()` on that socket returns 0 bytes, which is the standard POSIX indication of EOF (the peer has disconnected). But since the original code never asked for `POLLIN` (the `events` field was set to `0`) and never checked for it, the disconnect was invisible.

The `events=0` approach worked on Linux only because `POLLHUP` is an output-only flag — the kernel populates it in `revents` regardless of what you request in `events`. On macOS, neither `POLLHUP` nor `POLLIN` appears in `revents` when `events=0` for this socket type.

## What the Fix Does

The fix adds a two-tier detection strategy:

**Tier 1 — Request `POLLIN` in the events mask:**
```cpp
poll_fds[i] = {fd, /*events=*/POLLIN, /*revents=*/0};
```
This tells `poll()` to also report when data (or EOF) is available to read. On macOS, a peer shutdown produces a `POLLIN` event.

**Tier 2 — Distinguish data from EOF when `POLLIN` fires:**
```cpp
if (poll_fds[i].revents & (POLLHUP | POLLERR)) {
    result[i] = true;                    // still handle POLLHUP/POLLERR (Linux path)
} else if (poll_fds[i].revents & POLLIN) {
    char buf;
    ssize_t n = recv(poll_fds[i].fd, &buf, 1, MSG_PEEK | MSG_DONTWAIT);
    if (n == 0) {
        result[i] = true;               // EOF = peer disconnected (macOS path)
    }
}
```

When `POLLIN` is set, it could mean either (a) the peer sent actual data, or (b) the peer disconnected (EOF). To distinguish between these, the code calls `recv()` with two flags:
- `MSG_PEEK` — look at the data without consuming it from the buffer, so normal reads are unaffected.
- `MSG_DONTWAIT` — return immediately if there's nothing to read (non-blocking).

If `recv()` returns 0, it means EOF — the peer has closed its end. If it returns > 0, there's actual data available and the connection is still alive. If it returns -1 (error), the connection is also still considered alive (the error will be handled by normal I/O paths).

## Why This Doesn't Break Linux

On Linux, `poll()` still returns `POLLHUP` for this case, so the first check (`revents & (POLLHUP | POLLERR)`) catches it before reaching the `POLLIN` branch. The `recv()` path is only reached on macOS where `POLLHUP` is absent. Adding `POLLIN` to the events mask has no negative effect on Linux — it simply means the kernel may also set `POLLIN` in `revents`, but the `POLLHUP` check takes precedence.

## Production Impact

Beyond the test, this function is called in production by `NodeManager::CheckForWorkerDisconnects()` (`src/ray/raylet/node_manager.cc:608`). The raylet periodically calls this to detect workers that have silently disconnected. On macOS, the original code would **never** detect these disconnects, meaning dead workers would not be cleaned up until the raylet tried to send them a message and got an I/O error. The fix makes silent disconnect detection work correctly on macOS.
