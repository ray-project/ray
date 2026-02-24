"""
Reproduction for SIGSEGV in getenv() using Ray worker processes.

The crash occurs when glibc's setenv() (called from Python os.environ[k] = v)
races with glibc's getenv() (called from a C++ background thread, in this case
gRPC's ShufflePickFirstEnabled via the OTel metric export thread).

Since the gRPC getenv() path only fires during lazy initialization (once per
gRPC channel), the exact Ray+OTel race is nearly impossible to trigger
deterministically in a short test. Instead, this script reproduces the
IDENTICAL underlying mechanism by running concurrent getenv/setenv from
within a Ray worker process — proving that the same crash affects Ray workers.

On vulnerable glibc (<2.40), workers will crash with SIGSEGV.
On patched glibc (>=2.40), this should complete without crashing.

Usage:
    python repro_ray_getenv_race.py
"""

import os
import sys
import threading
import time

import ray


@ray.remote(num_cpus=0.1)
def crash_worker(worker_id: int, duration_secs: float = 10.0):
    """
    Ray task that reproduces the getenv/setenv race inside a worker process.

    - Background thread: calls libc getenv() in a tight loop (simulating gRPC)
    - Main thread: calls os.environ[k] = v (simulating set_visible_accelerator_ids,
      set_omp_num_threads, or user code that modifies env vars)

    On glibc <2.40, this will SIGSEGV the worker process.
    """
    import ctypes
    import ctypes.util

    # Load libc inside the worker (can't pickle ctypes objects across processes).
    # Python's os.environ holds the GIL, but C-level getenv() (as called by
    # gRPC, c-ares, OpenTelemetry SDK, etc.) does NOT hold the GIL.
    libc_path = ctypes.util.find_library("c")
    libc = ctypes.CDLL(libc_path, use_errno=True)
    libc.getenv.restype = ctypes.c_char_p
    libc.getenv.argtypes = [ctypes.c_char_p]

    def getenv_thread(stop_event, count_out):
        """
        Simulate what gRPC/OTel does: call getenv() from a background thread.

        In the real crash, this is:
          PeriodicExportingMetricReader thread
            -> gRPC DNS resolution
              -> ShufflePickFirstEnabled()
                -> grpc_core::GetEnv()
                  -> getenv()
        """
        count = 0
        while not stop_event.is_set():
            libc.getenv(b"PATH")
            libc.getenv(b"HOME")
            libc.getenv(b"GRPC_EXPERIMENTS")
            libc.getenv(b"NONEXISTENT_VAR")
            count += 4
        count_out.append(count)

    stop = threading.Event()
    getenv_count = []

    # Start background threads calling getenv (like the OTel/gRPC thread)
    threads = []
    for _ in range(2):
        t = threading.Thread(target=getenv_thread, args=(stop, getenv_count),
                             daemon=True)
        t.start()
        threads.append(t)

    # Main thread: mutate os.environ in a tight loop (like Ray task execution does).
    # IMPORTANT: Use unique key names to force glibc to grow and reallocate the
    # environ array. Re-using the same keys just updates in place, which is safe.
    # The crash only happens on reallocation (when environ needs a bigger array).
    setenv_count = 0
    all_keys = []
    end_time = time.monotonic() + duration_secs
    while time.monotonic() < end_time:
        # Create new unique env vars to force environ array growth
        batch_keys = []
        for i in range(100):
            key = f"REPRO_{setenv_count}"
            os.environ[key] = "x"
            batch_keys.append(key)
            setenv_count += 1
        # Clean up this batch to avoid running out of memory,
        # but the alloc/free cycle is what triggers the race.
        for key in batch_keys:
            del os.environ[key]

    stop.set()
    for t in threads:
        t.join(timeout=2)

    total_getenv = sum(getenv_count)
    return worker_id, setenv_count, total_getenv


def main():
    num_workers = 20
    duration_per_worker = 15.0  # seconds

    print("=" * 70)
    print("Ray SIGSEGV reproduction: getenv/setenv race in worker processes")
    print("=" * 70)
    print()
    print("Each Ray worker runs:")
    print("  - Background thread: libc getenv() in a loop (like gRPC/OTel)")
    print("  - Main thread: os.environ mutations (like Ray task dispatch)")
    print()
    print("This is the EXACT same race that causes the production SIGSEGV,")
    print("just with a wider race window to trigger it reliably.")
    print()
    sys.stdout.flush()

    ray.init()

    print(f"Launching {num_workers} workers for {duration_per_worker}s each...")
    print(f"On glibc <2.40, expect worker crashes (SIGSEGV / exit code 139).")
    print()
    sys.stdout.flush()

    refs = [
        crash_worker.remote(i, duration_per_worker) for i in range(num_workers)
    ]

    completed = 0
    failed = 0
    for ref in refs:
        try:
            worker_id, setenvs, getenvs = ray.get(ref, timeout=duration_per_worker + 30)
            completed += 1
        except ray.exceptions.WorkerCrashedError:
            failed += 1
            print(f"  Worker CRASHED (SIGSEGV)")
        except Exception as e:
            failed += 1
            print(f"  Worker failed: {type(e).__name__}: {e}")
        sys.stdout.flush()

    print()
    print("=" * 70)
    print(f"Results: {completed} completed, {failed} worker crashes")
    if failed > 0:
        print(f"The {failed} crash(es) confirm the getenv/setenv race in Ray workers.")
    else:
        print("No crashes. Try increasing num_workers or duration, or check glibc version.")
    print("=" * 70)

    ray.shutdown()


if __name__ == "__main__":
    main()
