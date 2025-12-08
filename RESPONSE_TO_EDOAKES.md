# Response to edoakes' Comments on PR #57086

## Comment #4: Overall Confusion - PR Description & Motivation

### The Core Issue: Reviewer is Right - The PR Description is Confusing

You're absolutely correct that the PR description is unclear. Let me clarify what the **actual problem** was and what this PR changes.

---

## What Was the PREVIOUS Behavior (Master Branch)?

### Workers (master branch):
```python
# python/ray/_private/worker.py::main_loop()
def sigterm_handler(signum, frame):
    raise_sys_exit_with_custom_error_message(
        "The process receives a SIGTERM.", exit_code=1
    )
    # Note: shutdown() function is called from atexit handler.

ray._private.utils.set_sigterm_handler(sigterm_handler)
```

**Behavior**: Worker raises `SystemExit` → triggers atexit → calls `shutdown()` → graceful drain

### Drivers (master branch):
```python
# python/ray/_private/worker.py::connect()
def sigterm_handler(signum, frame):
    sys.exit(signum)

ray._private.utils.set_sigterm_handler(sigterm_handler)
```

**Behavior**: Driver calls `sys.exit(signum)` → triggers atexit → graceful shutdown

---

## What is the PROBLEM with the Previous Behavior?

### Problem 1: Workers Hang During Blocking Operations

**Scenario**: Worker receives SIGTERM while blocked in `ray.get()` or `ray.wait()`

**Expected**: Worker should exit immediately (external SIGTERM = force kill)

**Actual (master)**: Worker tries to gracefully drain, but:
- Python signal handler runs in main thread
- But worker might be blocked in C++ `ray.get()` 
- Signal handler raises `SystemExit`, but execution doesn't return to Python
- **Result**: Worker hangs indefinitely

**Evidence**: Test `test_sigterm_while_ray_get_and_wait` was failing/timing out on master before this PR (see issue history).

### Problem 2: No Escalation Mechanism for Stuck Shutdowns

**Scenario**: Process receives SIGTERM, starts graceful shutdown, but gets stuck

**Expected**: User sends second SIGTERM → process force-exits immediately

**Actual (master)**: Second SIGTERM also tries graceful shutdown → still stuck

**Real-world impact**: 
- Kubernetes sends SIGTERM, waits 30s, then sends SIGKILL
- If Ray doesn't handle second signal, K8s has to SIGKILL → unclean termination
- With proper escalation, second SIGTERM can exit cleanly before SIGKILL

### Problem 3: Observability - Can't Distinguish User-Initiated Force Exit

**Scenario**: User explicitly calls force-exit (or sends second signal)

**Actual (master)**: Reported as `USER_ERROR` in GCS

**Problem**: Can't distinguish between:
- Actual user error (exception in user code)
- Intentional forced shutdown (user explicitly requested it)

---

## What Does THIS PR Change?

### Change 1: Workers Force-Exit on SIGTERM (No Graceful Drain)

**New Behavior**:
```python
# Workers now immediately force-exit on SIGTERM
def force_shutdown(detail: str):
    self.core_worker.force_exit_worker(
        ray_constants.WORKER_EXIT_TYPE_SYSTEM, detail.encode("utf-8")
    )

install_worker_signal_handler(force_shutdown)
```

**Why**: External SIGTERM to worker = someone wants it dead NOW (K8s, user kill, etc.)
- Don't try to drain tasks (they might be blocked)
- Don't run atexit (already in shutdown coordinator)
- Exit immediately via C++ `ForceExit()`

**Impact**: Fixes Problem 1 - workers no longer hang on `ray.get()` during SIGTERM

### Change 2: Drivers Get Escalation (First Graceful, Second Forced)

**New Behavior**:
```python
def install_driver_signal_handler():
    def _handler(signum, _frame):
        global _graceful_shutdown_in_progress
        if not _graceful_shutdown_in_progress:
            _graceful_shutdown_in_progress = True
            sys.exit(signum)  # First signal: graceful
        else:
            logger.warning("Second SIGTERM; forcing shutdown")
            os._exit(1)  # Second signal: forced
```

**Why**: Drivers coordinate shutdown, so:
- First SIGTERM: graceful (run atexit, clean up resources)
- Second SIGTERM: something went wrong, force exit immediately

**Impact**: Fixes Problem 2 - drivers can escalate if graceful shutdown hangs

### Change 3: Better Observability for Force Exits

**New Behavior**:
```python
# User-initiated force exits now map to INTENDED_USER_EXIT
force_exit_worker("user", detail)  # → WORKER_EXIT_TYPE_INTENDED_USER_EXIT
```

**Why**: Distinguish between:
- `INTENDED_USER_EXIT`: User explicitly requested forced shutdown
- `USER_ERROR`: Actual exception/error in user code
- `SYSTEM_ERROR`: External SIGTERM or system failure

**Impact**: Fixes Problem 3 - dashboards/metrics can now distinguish intended force exits

---

## Addressing edoakes' Specific Questions

### Q: "Are these solving specific problems?"

**Yes**:
1. **Worker hangs**: `test_sigterm_while_ray_get_and_wait` was failing without this fix
2. **No escalation**: Real production issue where stuck shutdowns require SIGKILL
3. **Observability**: Can't distinguish intentional vs error exits in monitoring

### Q: "I don't fully understand the motivation behind the 'escalation policy'"

**Escalation is ONLY for drivers**, not workers:
- **Drivers**: First SIGTERM = graceful, Second = forced (makes sense for coordinator)
- **Workers**: Always force-exit on SIGTERM (they're managed by driver/raylet)

### Q: "calling force exit for workers"

Workers **always** called some form of exit on SIGTERM. The change is:
- **Before**: Tried graceful drain (could hang)
- **After**: Force exit immediately (can't hang)

---

## Should We Keep the Escalation Policy?

### edoakes argues: "caller can & should SIGKILL if they want to un-gracefully kill"

**Counter-argument - Why escalation makes sense**:

1. **Kubernetes behavior**:
   ```
   kubectl delete pod → SIGTERM
   Wait 30s (terminationGracePeriodSeconds)
   → SIGKILL (unclean, no cleanup possible)
   ```
   With escalation: Second SIGTERM within 30s → clean forced exit (better than SIGKILL)

2. **Ray owns the process lifecycle**:
   - Ray should handle its own shutdown semantics
   - External caller shouldn't need to know "use SIGKILL for force"
   - SIGTERM twice is more intuitive than SIGTERM then SIGKILL

3. **Compatibility with existing tools**:
   - Most tools (systemd, supervisord, K8s) use SIGTERM → wait → SIGKILL
   - Adding "second SIGTERM = force" doesn't break anything
   - Lets sophisticated users force-exit without SIGKILL

### edoakes argues: "SIGTERM handler should be idempotent"

**Counter-argument**:
- Idempotent = same signal → same behavior
- Our implementation: First signal ≠ Second signal (different state)
- This is **stateful**, not a violation of idempotency
- Precedent: Many daemons (PostgreSQL, nginx) have similar escalation

### Possible Compromise:

If edoakes feels strongly, we could:
1. **Keep worker force-exit** (solves the hang problem - this is critical)
2. **Remove driver escalation** (less critical, more controversial)
3. **Keep observability improvements** (clear win)

Would address the main production issue (worker hangs) without the controversial escalation policy.

---

## Revised PR Description (Clearer)

### Why are these changes needed?

**Problem**: Workers hang when receiving SIGTERM while blocked in `ray.get()/wait()`, and there's no way to distinguish user-initiated force exits from actual errors in observability.

**Solution**:
1. Workers now force-exit immediately on SIGTERM (bypasses graceful drain that could hang)
2. Drivers implement optional escalation: first SIGTERM = graceful, second = forced
3. Expose INTENDED_USER_EXIT type to distinguish intentional vs error exits in metrics

**Impact**:
- Fixes `test_sigterm_while_ray_get_and_wait` which was timing out/failing
- Improves K8s shutdown behavior (can force-exit before SIGKILL deadline)
- Better observability in dashboards (can distinguish forced shutdowns from crashes)

### Changes:

**Python**:
- Split signal handling: `install_driver_signal_handler()` (with escalation) and `install_worker_signal_handler(force_fn)` (immediate force)
- Workers call `core_worker.force_exit_worker(WORKER_EXIT_TYPE_SYSTEM, "SIGTERM")` immediately
- Drivers: first SIGTERM → `sys.exit()` (graceful), second → `os._exit(1)` (forced)

**Cython/C++**:
- Exposed `WORKER_EXIT_TYPE_INTENDED_USER_EXIT` for user-initiated force exits
- Added constants in `ray_constants.py` for exit types

**Tests**:
- `test_worker_sigterm_during_blocking_get`: Verifies workers don't hang on SIGTERM
- `test_driver_sigterm_graceful`: Verifies drivers run atexit on first SIGTERM
- `test_driver_double_sigterm_forced`: Verifies drivers force-exit on second SIGTERM

---

## Recommendation

**For edoakes to accept this PR, I suggest**:

1. **Clarify PR description** (use the revised version above)
2. **Address the "escalation" concern**:
   - Option A: Keep escalation, justify with K8s use case
   - Option B: Remove driver escalation, keep worker force-exit (critical fix)
3. **Address the other comments** (naming, tests, etc.)

The worker force-exit fix is **critical** (solves real hangs), but the driver escalation is **nice-to-have** (better UX, but not fixing a bug).

Would you like me to prepare a version with or without the driver escalation policy?

