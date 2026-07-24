# ABOUTME: Orchestrates all profiling and monitoring based on environment variables.
# ABOUTME: Provides start/stop methods so benchmarks don't need per-profiler boilerplate.

import os

from . import gpu_monitor, net_monitor, nsys, object_store, perf, pyspy, telemetry


class Profiling:
    """Coordinates all profiling and monitoring for a benchmark run.

    Reads configuration from environment variables:
        PROFILER_MODE:          "nsys", "torch", or "none" (default: "none")
        PROFILE_SKIP_BATCHES:   Batches to skip before nsys capture (default: 0)
        PROFILE_ACTIVE_BATCHES: Upper bound on captured batches per actor
                                (default: 10000). The in-loop fence calls
                                cudaProfilerStop after this many batches; in
                                normal use the value is left high so the fence
                                never fires and Infer's atexit hook closes the
                                capture range at process exit. Combined with
                                capture-range-end:stop in nsys.runtime_env(),
                                that finalizes the .nsys-rep synchronously
                                before Ray tears the actor down. Set this lower
                                if you want to bound the capture window (e.g.
                                profile only a warm steady-state window).
        PYSPY_ENABLED:          "1" to enable py-spy (default: "0")
        PYSPY_NUM_CPU_WORKERS:  CPU worker nodes to py-spy (default: 5)
        PYSPY_NUM_GPU_WORKERS:  GPU worker nodes to py-spy (default: 5)
        PERF_PROFILING_ENABLED: "1" to enable perf record (default: "0")
        PERF_NUM_CPU_WORKERS:   CPU worker nodes to profile (default: 5)
        PERF_NUM_GPU_WORKERS:   GPU worker nodes to profile (default: 5)
        GPU_MONITOR_ENABLED:    "1" to enable nvidia-smi monitoring (default: "0")
        NET_MONITOR_ENABLED:    "1" to enable network I/O monitoring (default: "0")
        OBJECT_STORE_MONITOR_ENABLED: "1" to enable object-store sampling (default: "0")
        OBJECT_STORE_MONITOR_INTERVAL_S: seconds between samples (default: 5)
        OBJECT_STORE_MONITOR_FAST_WINDOW_S: seconds at the start of the run to
            sample at OBJECT_STORE_MONITOR_FAST_INTERVAL_S instead of
            OBJECT_STORE_MONITOR_INTERVAL_S (default: 60). Set to 0 to disable.
        OBJECT_STORE_MONITOR_FAST_INTERVAL_S: tick interval during fast window (default: 1)
        RAY_MAX_LIMIT_FROM_API_SERVER: object-store sampling defaults to a 10k
            object cap per call. Set this env var to e.g. "200000" on the head
            node to fully sample large runs (otherwise samples are truncated).
        PROFILING_S3_BUCKET:    S3 bucket for telemetry upload

    Usage:
        profiling = Profiling(outdir="/mnt/shared_storage/my_benchmark/job123",
                              num_gpu_nodes=40)
        profiling.start()

        infer_kwargs["runtime_env"] = profiling.nsys_runtime_env()
        # ... run benchmark ...

        profiling.stop(s3_prefix="my-benchmark/job123")
    """

    _instance_active = False

    def __init__(self, outdir, num_gpu_nodes=0):
        self.outdir = outdir
        self.num_gpu_nodes = num_gpu_nodes

        self.profiler_mode = os.environ.get("PROFILER_MODE", "none")
        self.pyspy_enabled = os.environ.get("PYSPY_ENABLED", "0") == "1"
        self.pyspy_num_cpu_workers = int(os.environ.get("PYSPY_NUM_CPU_WORKERS", "5"))
        self.pyspy_num_gpu_workers = int(os.environ.get("PYSPY_NUM_GPU_WORKERS", "5"))
        self.perf_enabled = os.environ.get("PERF_PROFILING_ENABLED", "0") == "1"
        self.perf_num_cpu_workers = int(os.environ.get("PERF_NUM_CPU_WORKERS", "5"))
        self.perf_num_gpu_workers = int(os.environ.get("PERF_NUM_GPU_WORKERS", "5"))
        self.gpu_monitor_enabled = os.environ.get("GPU_MONITOR_ENABLED", "0") == "1"
        self.net_monitor_enabled = os.environ.get("NET_MONITOR_ENABLED", "0") == "1"
        self.object_store_monitor_enabled = (
            os.environ.get("OBJECT_STORE_MONITOR_ENABLED", "0") == "1"
        )
        self.object_store_monitor_interval_s = int(
            os.environ.get("OBJECT_STORE_MONITOR_INTERVAL_S", "5")
        )
        self.object_store_monitor_fast_window_s = int(
            os.environ.get("OBJECT_STORE_MONITOR_FAST_WINDOW_S", "60")
        )
        self.object_store_monitor_fast_interval_s = int(
            os.environ.get("OBJECT_STORE_MONITOR_FAST_INTERVAL_S", "1")
        )

        self._worker_perf_actors = []
        self._head_perf_handles = []
        self._worker_pyspy_actors = []

    def is_enabled(self):
        """Return True if any profiling or monitoring is enabled."""
        return (
            self.profiler_mode != "none"
            or self.pyspy_enabled
            or self.perf_enabled
            or self.gpu_monitor_enabled
            or self.net_monitor_enabled
            or self.object_store_monitor_enabled
        )

    def start(self, extra_config=None):
        """Start all enabled profilers and monitors.

        Args:
            extra_config: Optional dict of extra config key/value pairs to print.
        """
        Profiling._instance_active = True
        print("Configuration:")
        if extra_config:
            for key, value in extra_config.items():
                print(f"  {key}: {value}")
        print(f"  PROFILER_MODE:          {self.profiler_mode}")
        print(f"  PYSPY_ENABLED:          {self.pyspy_enabled}")
        if self.pyspy_enabled:
            print(
                f"    workers (cpu/gpu):    "
                f"{self.pyspy_num_cpu_workers}/{self.pyspy_num_gpu_workers}"
            )
        print(f"  PERF_PROFILING_ENABLED: {self.perf_enabled}")
        print(f"  GPU_MONITOR_ENABLED:    {self.gpu_monitor_enabled}")
        print(f"  NET_MONITOR_ENABLED:    {self.net_monitor_enabled}")
        print(
            f"  OBJECT_STORE_MONITOR:   {self.object_store_monitor_enabled} "
            f"(interval={self.object_store_monitor_interval_s}s, "
            f"fast_window={self.object_store_monitor_fast_window_s}s @ "
            f"{self.object_store_monitor_fast_interval_s}s)"
        )
        print(f"  SHARED_OUTDIR:          {self.outdir}")
        print()

        os.makedirs(self.outdir, exist_ok=True)

        if self.gpu_monitor_enabled and self.num_gpu_nodes > 0:
            gpu_monitor.start(self.outdir, self.num_gpu_nodes)
        if self.net_monitor_enabled:
            net_monitor.start(self.outdir)
        if self.object_store_monitor_enabled:
            object_store.start(
                self.outdir,
                interval_s=self.object_store_monitor_interval_s,
                fast_window_s=self.object_store_monitor_fast_window_s,
                fast_interval_s=self.object_store_monitor_fast_interval_s,
            )

        if self.pyspy_enabled:
            pyspy.start(self.outdir)
            if self.pyspy_num_cpu_workers > 0 or self.pyspy_num_gpu_workers > 0:
                self._worker_pyspy_actors = pyspy.start_worker_nodes(
                    self.outdir,
                    num_cpu_workers=self.pyspy_num_cpu_workers,
                    num_gpu_workers=self.pyspy_num_gpu_workers,
                )

        if self.perf_enabled:
            self._head_perf_handles = perf.start_head_node(self.outdir)
            self._worker_perf_actors = perf.start_worker_nodes(
                self.outdir,
                num_cpu_workers=self.perf_num_cpu_workers,
                num_gpu_workers=self.perf_num_gpu_workers,
            )

    def nsys_runtime_env(self):
        """Return nsys runtime_env dict if profiler_mode is "nsys", else empty dict."""
        if self.profiler_mode == "nsys":
            return nsys.runtime_env(self.outdir)
        return {}

    def stop(self, s3_prefix=None, s3_bucket=None):
        """Stop all profilers and upload telemetry to S3.

        Args:
            s3_prefix: S3 key prefix for telemetry upload. If None, skips upload.
            s3_bucket: S3 bucket name. Defaults to PROFILING_S3_BUCKET env var.
        """
        if self.pyspy_enabled:
            # Worker py-spy first: each actor's stop() blocks until its
            # start() has finished attaching (Ray actor methods are serial),
            # then SIGINTs py-spy on the worker to flush output to shared
            # storage before we move on.
            pyspy.stop_workers(self._worker_pyspy_actors)
            pyspy.stop()

        if self.perf_enabled:
            # Each stop_* converts its own .data to collapsed stacks on the
            # node that produced it — worker .data symbolizes on the worker
            # (where /tmp/ray/session_* runtime libs still exist), head .data
            # on the head. A central pass on the head would symbolize worker
            # data against the head's filesystem and lose most user-space
            # symbols.
            perf.stop_workers(self._worker_perf_actors)
            perf.stop_head(self._head_perf_handles)

        if s3_prefix is not None:
            telemetry.upload(self.outdir, s3_prefix, s3_bucket=s3_bucket)

        Profiling._instance_active = False
