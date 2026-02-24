"""
Reproduction for SIGSEGV in getenv() in the context of Ray + gRPC + OpenTelemetry.

This script mimics the exact race condition that causes the crash:
  - Thread A: Mutates os.environ (like Ray worker threads setting
    CUDA_VISIBLE_DEVICES, OMP_NUM_THREADS, runtime env vars, etc.)
  - Thread B: Makes gRPC calls that trigger internal getenv() via
    grpc_core::GetEnv() -> ShufflePickFirstEnabled() -> getenv()

The gRPC call triggers DNS resolution and load-balancing config parsing,
which lazily calls getenv("GRPC_EXPERIMENTS") on the gRPC thread.
Meanwhile the Python thread is calling setenv() via os.environ[k] = v.

On vulnerable glibc (<2.40), this will typically SIGSEGV within seconds.

Usage:
    pip install grpcio
    python repro_getenv_race.py
"""

import os
import sys
import threading
import time


def env_writer_thread(stop_event: threading.Event):
    """
    Simulates Ray worker threads that modify os.environ during task execution.

    In Ray, these mutations happen in:
      - set_omp_num_threads()           -> os.environ["OMP_NUM_THREADS"] = ...
      - set_visible_accelerator_ids()   -> os.environ["CUDA_VISIBLE_DEVICES"] = ...
      - update_envs()                   -> os.environ[key] = value  (runtime env)
    """
    i = 0
    while not stop_event.is_set():
        # Rotate through many unique keys to force environ array growth/reallocation
        key = f"RAY_REPRO_ENV_{i % 200}"
        os.environ[key] = f"value_{i}"
        i += 1
    print(f"  Writer thread did {i} setenv() calls")


def grpc_caller_thread(stop_event: threading.Event):
    """
    Simulates the OpenTelemetry PeriodicExportingMetricReader thread that
    exports metrics via gRPC.

    The actual crash path is:
      OtlpGrpcMetricExporter::Export()
        -> gRPC BlockingUnaryCallImpl
          -> DNS resolution (c-ares)
            -> PickFirstFactory::ParseLoadBalancingConfig()
              -> ShufflePickFirstEnabled()
                -> grpc_core::GetEnv()
                  -> getenv()  <-- SIGSEGV

    We trigger this by making gRPC calls to a non-existent endpoint,
    which forces gRPC to go through DNS resolution and LB config parsing.
    """
    try:
        import grpc
    except ImportError:
        print("ERROR: grpcio not installed. Run: pip install grpcio")
        sys.exit(1)

    calls = 0
    while not stop_event.is_set():
        try:
            # Create a channel to a non-existent endpoint.
            # This forces gRPC to perform DNS resolution, which triggers
            # the lazy getenv() call in ShufflePickFirstEnabled().
            channel = grpc.insecure_channel(
                f"nonexistent-host-{calls % 50}.example.com:50051"
            )
            # Force the channel to connect (triggers DNS + LB config parsing)
            try:
                grpc.channel_ready_future(channel).result(timeout=0.01)
            except grpc.FutureTimeoutError:
                pass
            channel.close()
            calls += 1
        except Exception as e:
            # Ignore connection errors - we only care about the getenv race
            calls += 1

    print(f"  gRPC thread did {calls} connection attempts")


def main():
    duration = 30  # seconds

    print("Reproducing getenv/setenv race condition (glibc thread-safety bug)")
    print()
    print("This mimics the Ray crash where:")
    print("  Thread A: Python worker sets os.environ (-> glibc setenv())")
    print("  Thread B: gRPC/OpenTelemetry metric export calls getenv() internally")
    print()
    print(f"Running for {duration} seconds...")
    print(f"On vulnerable glibc (<2.40), expect SIGSEGV (exit code -11).")
    print(f"On patched glibc (>=2.40), this should complete without crashing.")
    print()
    sys.stdout.flush()

    stop_event = threading.Event()

    # Start multiple writer threads (simulating Ray worker env mutations)
    writers = []
    for i in range(3):
        t = threading.Thread(target=env_writer_thread, args=(stop_event,), daemon=True)
        t.start()
        writers.append(t)

    # Start multiple gRPC threads (simulating OpenTelemetry metric export)
    grpc_threads = []
    for i in range(2):
        t = threading.Thread(target=grpc_caller_thread, args=(stop_event,), daemon=True)
        t.start()
        grpc_threads.append(t)

    # Let it run
    time.sleep(duration)

    print(f"\nStopping after {duration}s...")
    stop_event.set()

    for t in writers + grpc_threads:
        t.join(timeout=5)

    # Clean up the env vars we created
    for i in range(200):
        os.environ.pop(f"RAY_REPRO_ENV_{i}", None)

    print("\nCompleted without crash - your glibc appears to be safe.\n")


if __name__ == "__main__":
    main()
