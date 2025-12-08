# Fix for Flaky Actor Shutdown Tests

## Problem Statement

Two tests were flaky in CI:
1. `test_actor_ray_shutdown_called_on_del`
2. `test_actor_ray_shutdown_called_on_scope_exit`

Both tests verify that the `__ray_shutdown__` callback is invoked when actors go out of scope (via `del` or scope exit). The tests were failing intermittently because the callback was sometimes not being invoked.

## Root Cause Analysis

### Issue: Force Kill on Reference Deletion

In `src/ray/gcs/gcs_actor_manager.cc` at line 973-975, when all references to an actor are deleted, the GCS was calling `DestroyActor` with `force_kill=true`:

```cpp
DestroyActor(actor_id,
             GenActorRefDeletedCause(GetActor(actor_id)),
             /*force_kill=*/true);  // ← Incorrect!
```

### Why This Caused the Problem

When `force_kill=true`:
1. The GCS sends a `KillLocalActor` RPC with `force_kill=true` to the raylet
2. The raylet forwards this to the core worker's `HandleKillActor`
3. The core worker calls `ForceExit()` which:
   - Kills child processes immediately
   - Disconnects services
   - Calls `QuickExit()` to terminate the process immediately
   - **Never calls the `__ray_shutdown__` callback**

### Evidence from Logs

From the CI failure logs (`/Users/sagar/Desktop/test_ray_shutdown_called_on_scope_exit/`):

**Worker 57730 and 57864** (force killed):
```
[2025-11-13 01:31:04,007] core_worker.cc:4056: Force kill actor request has received. 
exiting immediately... The actor is dead because all references to the actor were 
removed including lineage ref count.
[2025-11-13 01:31:04,009] core_worker_shutdown_executor.cc:288: Quick exit - terminating 
process immediately
```

**Worker 57820** (graceful shutdown attempted):
```
[2025-11-13 01:31:15,873] core_worker_shutdown_executor.cc:94: Executing worker exit: 
INTENDED_SYSTEM_EXIT - Worker exits because the actor is killed. The actor is dead 
because all references to the actor were removed. (timeout: -1ms)
```

This shows a **race condition** where sometimes actors were force-killed and sometimes they went through graceful shutdown, explaining the test flakiness.

### Why Was This Inconsistent with Other Code?

There was already correct handling for "out of scope" actors at line 292-300 in the same file:

```cpp
int64_t timeout_ms = RayConfig::instance().actor_graceful_shutdown_timeout_ms();
DestroyActor(
    actor_id,
    GenActorOutOfScopeCause(actor),
    /*force_kill=*/false,  // ← Uses graceful shutdown
    [reply, send_reply_callback]() {
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    },
    timeout_ms);
```

But the reference deletion callback path (line 975) was using `force_kill=true`, which was inconsistent.

## The Fix

Changed line 973-975 in `src/ray/gcs/gcs_actor_manager.cc` to use graceful shutdown with the configured timeout:

```cpp
int64_t timeout_ms = RayConfig::instance().actor_graceful_shutdown_timeout_ms();
DestroyActor(actor_id,
             GenActorRefDeletedCause(GetActor(actor_id)),
             /*force_kill=*/false,  // ← Now uses graceful shutdown
             nullptr,
             timeout_ms);
```

### What This Achieves

With `force_kill=false`:
1. The core worker's `HandleKillActor` calls `Exit()` instead of `ForceExit()`
2. `Exit()` triggers a graceful shutdown sequence that:
   - Calls the `__ray_shutdown__` callback (if defined)
   - Drains pending tasks
   - Cleanly disconnects services
   - Shuts down gracefully
3. If the shutdown exceeds the timeout, a force kill fallback is automatically triggered

### Safety and Fallback

The graceful shutdown has a built-in timeout fallback (`actor_graceful_shutdown_timeout_ms`, default 2000ms):
- If `__ray_shutdown__` hangs or takes too long, the actor is force-killed after the timeout
- This prevents actors with buggy cleanup code from hanging indefinitely
- The test `test_actor_graceful_shutdown_timeout_fallback` verifies this behavior

## Test Results

All tests pass consistently after the fix:

```bash
# Previously flaky test 1 - now passes 10/10 times
pytest ray/tests/test_actor_failures.py -k "test_actor_ray_shutdown_called_on_del" --count=10
# Result: 10 passed

# Previously flaky test 2 - now passes 10/10 times  
pytest ray/tests/test_actor_failures.py -k "test_actor_ray_shutdown_called_on_scope_exit" --count=10
# Result: 10 passed

# All shutdown-related tests pass
pytest ray/tests/test_actor_failures.py -k "shutdown"
# Result: 7 passed
```

## Related Code Paths

The fix affects the following scenarios where actors are destroyed:

1. **Actor goes out of scope** (`del actor`) - Fixed ✓
2. **Actor scope exit** (function return) - Fixed ✓
3. **Explicit `__ray_terminate__`** - Already uses graceful shutdown ✓
4. **`ray.kill()` with force=False** - Uses graceful shutdown ✓
5. **`ray.kill()` with force=True** - Correctly uses force kill (no callback expected) ✓

The fix makes behavior consistent across all reference deletion scenarios while maintaining the force kill option when explicitly requested.

## Configuration

The graceful shutdown timeout can be configured via:
- `RayConfig::actor_graceful_shutdown_timeout_ms()` (C++)
- Default: 2000ms (2 seconds)

This provides a balance between allowing cleanup time and preventing hung actors.

