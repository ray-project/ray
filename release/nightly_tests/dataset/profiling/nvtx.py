# ABOUTME: NVTX annotation helpers and CUDA profiler capture range control for nsys.
# ABOUTME: Provides context managers for NVTX ranges and batch-count-based profiler start/stop.

from contextlib import contextmanager


@contextmanager
def profiling_range(name):
    """Context manager that pushes/pops an NVTX range.

    Falls back to a no-op if torch.cuda.nvtx is not available.
    """
    try:
        import torch.cuda.nvtx as nvtx

        nvtx.range_push(name)
    except (ImportError, AttributeError):
        yield
        return
    try:
        yield
    finally:
        nvtx.range_pop()


def cuda_profiler_fence(call_count, skip_batches, active_batches, node_ip=""):
    """Start/stop the CUDA profiler based on batch call count.

    Used with nsys cudaProfilerApi capture range mode. Starts profiling
    after skip_batches and stops after active_batches more.

    In practice the stop branch rarely fires: PROFILE_ACTIVE_BATCHES is
    typically left at its large default so the capture spans the entire
    actor lifetime, and the matching cudaProfilerStop is invoked from
    Infer's atexit hook instead. Paired with capture-range-end:stop in
    nsys_runtime_env(), that finalizes the .nsys-rep synchronously
    before Ray tears the actor down.

    Args:
        call_count: Current batch call count (1-indexed).
        skip_batches: Number of batches to skip before starting capture.
        active_batches: Number of batches to capture.
        node_ip: Node IP for log messages (optional).

    Returns:
        (profiling_active, profiler_done) tuple of booleans.
    """
    import torch

    if call_count == skip_batches + 1:
        torch.cuda.cudart().cudaProfilerStart()
        print(f"[{node_ip}] nsys capture started at batch {call_count}", flush=True)
        return True, False
    elif call_count == skip_batches + active_batches + 1:
        torch.cuda.cudart().cudaProfilerStop()
        print(
            f"[{node_ip}] nsys capture stopped at batch {call_count} "
            f"({active_batches} batches captured)",
            flush=True,
        )
        return False, True
    return None, None
