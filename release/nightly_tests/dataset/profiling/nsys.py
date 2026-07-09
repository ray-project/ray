# ABOUTME: Builds nsys (NVIDIA Nsight Systems) runtime_env configuration for Ray workers.
# ABOUTME: Configures nsys to wrap worker processes and use cudaProfilerApi for capture range control.


def runtime_env(outdir):
    """Build a runtime_env dict that wraps Ray workers with nsys profile.

    Args:
        outdir: Shared storage directory for .nsys-rep output files.

    Returns:
        Dict suitable for passing as runtime_env to map_batches() etc.
    """
    # capture-range-end=stop: when cudaProfilerStop() is called, nsys finalizes
    # the .nsys-rep file synchronously and detaches. Without this, nsys only
    # finalizes at process exit; if Ray Data hard-kills the Infer actor at end
    # of pipeline (which it sometimes does), the file is left empty/missing.
    # cuda_profiler_fence() in profiling/nvtx.py is what triggers cudaProfilerStop
    # after active_batches batches, so active_batches must be set lower than the
    # number of batches each actor actually processes.
    return {
        "nsight": {
            "t": "cuda,cudnn,cublas,nvtx",
            "capture-range": "cudaProfilerApi",
            "capture-range-end": "stop",
            "stop-on-exit": "true",
            "o": f"{outdir}/nsys_%h_%p",
        }
    }
