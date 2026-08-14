"""Per-node worker-memory sampling for the dataset release benchmarks.

**What problem this solves.** The release suite's memory evidence came from Prometheus
``query_range`` over node memory. That series is scraped rarely enough that a short job
gets a handful of distinct values, it says nothing about *which* stage held the memory,
and a node's total tells you nothing about whether the decode heap or the object store
grew. Ray's own per-task ``MemoryProfiler`` (surfaced by
``benchmark.collect_operator_metrics``) fixes the attribution but only exists inside map
tasks — shuffle tasks, actors and the raylet are invisible to it.

This module is the third view: one actor per node, sampling every ``ray::*`` worker
process on that node at ~1 s, keyed by the **proctitle** — which Ray sets to
``ray::<task or actor name>`` — plus the node's own used bytes. That yields
peak-with-provenance: not just "the node reached 58 GB" but "the node reached 58 GB
while ``ray::ReadParquet`` workers held 41 GB of it across 8 processes".

**Off by default.** Set ``RAY_DATA_BENCH_NODE_MEM_MONITOR=1`` to enable; every driver
that runs through ``Benchmark.run_fn`` then gets it with no code change. The sampler
actors take ``num_cpus=0`` so they never displace benchmark work.

Environment:
    RAY_DATA_BENCH_NODE_MEM_MONITOR   "1" to enable (default off)
    RAY_DATA_BENCH_NODE_MEM_INTERVAL  seconds between samples (default 1.0)
    RAY_DATA_BENCH_NODE_MEM_DIR       where to write the JSONL trace
                                      (default: alongside TEST_OUTPUT_JSON, else CWD)
    RAY_DATA_BENCH_NODE_MEM_MAX_SAMPLES  per-node sample cap before decimation
                                      (default 20000; ~5.5 h at 1 Hz)

Result-dict fields it adds (all ``None``/absent if disabled or unavailable):
    node_mem_peak_used_gb          peak "used" bytes on the worst node
    node_mem_peak_used_node        that node's IP
    node_mem_peak_used_source      cgroup | meminfo | psutil — which one it came from
    node_mem_peak_worker_uss_gb    peak summed worker USS on the worst node (Linux)
    node_mem_peak_worker_rss_gb    same in RSS, which includes object-store pages
    node_mem_top_workers_uss/_rss  {proctitle: peak GB}, biggest first — the provenance
    node_mem_trace_path            the JSONL trace, one line per (node, sample)
    node_mem_nodes / node_mem_samples / node_mem_error
"""
import json
import logging
import os
import sys
import threading
import time
from typing import Any, Dict, List, Optional

import ray

logger = logging.getLogger(__name__)

ENABLE_ENV = "RAY_DATA_BENCH_NODE_MEM_MONITOR"
INTERVAL_ENV = "RAY_DATA_BENCH_NODE_MEM_INTERVAL"
DIR_ENV = "RAY_DATA_BENCH_NODE_MEM_DIR"
MAX_SAMPLES_ENV = "RAY_DATA_BENCH_NODE_MEM_MAX_SAMPLES"

# Proctitles are ``ray::<name>``; keep the whole thing, it is already the stage label.
_WORKER_PREFIX = "ray::"
_SELF_PREFIX = "ray::_NodeMemorySampler"


def enabled() -> bool:
    return os.environ.get(ENABLE_ENV, "0") == "1"


def _bytes_to_gb(b: Optional[float]) -> Optional[float]:
    return round(b / (1024**3), 4) if b else b


def _read_node_used_bytes() -> Dict[str, Optional[int]]:
    """This node's memory pressure, by the same convention Ray's memory monitor uses.

    Three numbers, because they disagree in ways that matter:

    * ``cgroup_used`` — the container's own accounting (``memory.current`` on cgroup v2,
      ``memory.usage_in_bytes`` on v1). This is what the OOM killer acts on, and it
      *includes* the object store's ``/dev/shm`` pages.
    * ``meminfo_used`` — ``MemTotal - MemAvailable`` from ``/proc/meminfo``: the whole
      box, including anything outside our cgroup.
    * ``psutil_used`` — ``total - available`` from psutil. Same convention as the second,
      but portable, so local (macOS) smoke runs still produce a number. On Linux it is a
      cross-check, not a third opinion.

    The first two are ``None`` off Linux.
    """
    out: Dict[str, Optional[int]] = {
        "cgroup_used": None,
        "meminfo_used": None,
        "psutil_used": None,
    }
    for path in (
        "/sys/fs/cgroup/memory.current",  # v2
        "/sys/fs/cgroup/memory/memory.usage_in_bytes",  # v1
    ):
        try:
            with open(path) as fh:
                out["cgroup_used"] = int(fh.read().strip())
                break
        except (OSError, ValueError):
            continue
    try:
        total = avail = None
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    total = int(line.split()[1]) * 1024
                elif line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) * 1024
                if total is not None and avail is not None:
                    break
        if total is not None and avail is not None:
            out["meminfo_used"] = total - avail
    except (OSError, ValueError, IndexError):
        pass
    try:
        import psutil

        vm = psutil.virtual_memory()
        out["psutil_used"] = vm.total - vm.available
    except Exception:  # noqa: BLE001 - diagnostics only
        pass
    return out


@ray.remote(num_cpus=0)
class _NodeMemorySampler:
    """Samples one node's Ray worker processes, grouped by proctitle.

    ``num_cpus=0`` on purpose: this must not take a slot away from the workload it is
    measuring, and it is idle between samples.
    """

    def __init__(self, interval_s: float, max_samples: int):
        self._interval_s = interval_s
        self._max_samples = max_samples
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._samples: List[Dict[str, Any]] = []
        # Decimation factor: after the cap, keep every Nth sample and thin what we hold,
        # so a long run degrades resolution instead of dying or truncating its tail.
        self._keep_every = 1
        self._seen = 0
        self._uss_ok = True
        self._error: Optional[str] = None
        self._t0 = time.time()

    def node_info(self) -> Dict[str, Any]:
        return {
            "node_id": ray.get_runtime_context().get_node_id(),
            "node_ip": ray.util.get_node_ip_address(),
        }

    def _sample_once(self) -> Optional[Dict[str, Any]]:
        try:
            import psutil
        except ImportError:  # pragma: no cover - psutil ships with ray[default]
            self._error = "psutil not available"
            return None

        by_title: Dict[str, Dict[str, float]] = {}
        for proc in psutil.process_iter(["cmdline"]):
            try:
                cmdline = proc.info.get("cmdline") or []
                if not cmdline or not cmdline[0].startswith(_WORKER_PREFIX):
                    continue
                title = cmdline[0]
                # Don't measure the measurement: this actor's own process appears as
                # ray::_NodeMemorySampler[.method] and would otherwise show up in the
                # provenance table alongside the stages we care about.
                if title.startswith(_SELF_PREFIX):
                    continue
                if self._uss_ok:
                    try:
                        mi = proc.memory_full_info()
                        uss = getattr(mi, "uss", 0)
                        rss = mi.rss
                    except (psutil.AccessDenied, NotImplementedError):
                        self._uss_ok = False
                        uss, rss = 0, proc.memory_info().rss
                else:
                    uss, rss = 0, proc.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied, IndexError):
                continue
            entry = by_title.setdefault(title, {"n": 0, "rss": 0, "uss": 0})
            entry["n"] += 1
            entry["rss"] += rss
            entry["uss"] += uss

        return {
            "t": round(time.time() - self._t0, 3),
            **_read_node_used_bytes(),
            "workers_rss": sum(e["rss"] for e in by_title.values()),
            "workers_uss": sum(e["uss"] for e in by_title.values())
            if self._uss_ok
            else None,
            "by_title": by_title,
        }

    def _record(self):
        sample = self._sample_once()
        if sample is None:
            return
        self._seen += 1
        if self._seen % self._keep_every:
            return
        self._samples.append(sample)
        if len(self._samples) >= self._max_samples:
            # Halve resolution: drop every other sample we already hold, and take half
            # as many from here on. Repeats as needed, so memory is bounded.
            self._samples = self._samples[::2]
            self._keep_every *= 2

    def _run(self):
        while not self._stop.wait(self._interval_s):
            self._record()

    def start(self) -> bool:
        self._t0 = time.time()
        self._record()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return True

    def stop(self) -> Dict[str, Any]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval_s * 5)
        self._record()

        # Per-stage peak, kept separately for USS and RSS. They are NOT interchangeable:
        # RSS counts mapped object-store pages, so a task that merely reads big blocks
        # looks huge in RSS and small in USS. Never silently substitute one for the
        # other — the caller decides, knowing which it got.
        peak_uss_by_title: Dict[str, float] = {}
        peak_rss_by_title: Dict[str, float] = {}
        for sample in self._samples:
            for title, entry in sample["by_title"].items():
                peak_rss_by_title[title] = max(
                    peak_rss_by_title.get(title, 0), entry["rss"]
                )
                if self._uss_ok:
                    peak_uss_by_title[title] = max(
                        peak_uss_by_title.get(title, 0), entry["uss"]
                    )

        def _peak(key):
            vals = [s[key] for s in self._samples if s.get(key) is not None]
            return max(vals) if vals else None

        return {
            **self.node_info(),
            "error": self._error,
            "uss_available": self._uss_ok,
            "num_samples": len(self._samples),
            "keep_every": self._keep_every,
            "peak_cgroup_used": _peak("cgroup_used"),
            "peak_meminfo_used": _peak("meminfo_used"),
            "peak_psutil_used": _peak("psutil_used"),
            "peak_workers_rss": _peak("workers_rss"),
            "peak_workers_uss": _peak("workers_uss"),
            "peak_uss_by_title": peak_uss_by_title,
            "peak_rss_by_title": peak_rss_by_title,
            "samples": self._samples,
        }


class NodeMemoryMonitor:
    """Driver-side context manager: one sampler actor per alive node.

    Best-effort throughout — a monitor that fails must never fail the benchmark, so
    every step degrades to a recorded ``node_mem_error`` instead of raising. Use as::

        with NodeMemoryMonitor("read_parquet") as mon:
            ...run the workload...
        result.update(mon.summary())
    """

    def __init__(self, case_name: str, interval_s: Optional[float] = None):
        self._case = "".join(c if c.isalnum() or c in "-_." else "_" for c in case_name)
        self._interval_s = interval_s or float(os.environ.get(INTERVAL_ENV, "1.0"))
        self._max_samples = int(os.environ.get(MAX_SAMPLES_ENV, "20000"))
        self._actors: List[Any] = []
        self._summary: Dict[str, Any] = {}
        self._error: Optional[str] = None

    def __enter__(self) -> "NodeMemoryMonitor":
        try:
            self._start()
        except Exception as e:  # noqa: BLE001 - never fail the benchmark
            self._error = f"start failed: {e!r}"
            logger.warning("NodeMemoryMonitor failed to start", exc_info=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self._stop()
        except Exception as e:  # noqa: BLE001
            self._error = f"stop failed: {e!r}"
            logger.warning("NodeMemoryMonitor failed to stop", exc_info=True)

    def _start(self):
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        # Serialize the actor class BY VALUE. By default cloudpickle stores a class as
        # (module, name), so the workers must be able to `import node_memory_monitor` —
        # true when the driver's directory is uploaded as the working_dir, false when a
        # benchmark is launched from anywhere else, and the failure mode is an
        # ActorDiedError raised at stop(), by which point every sample is already gone.
        # (A per-actor `runtime_env={"py_modules": [...]}` cannot fix this: local paths
        # are only accepted at the job level, i.e. in ray.init.)
        try:
            import ray.cloudpickle as cloudpickle

            cloudpickle.register_pickle_by_value(sys.modules[__name__])
        except Exception:  # noqa: BLE001 - fall back to import-by-reference
            logger.warning("could not register by-value pickling", exc_info=True)

        nodes = [n for n in ray.nodes() if n.get("Alive")]
        for node in nodes:
            actor = _NodeMemorySampler.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node["NodeID"], soft=False
                ),
                # The workload is what should be scheduled; a sampler that cannot be
                # placed (drained node, race with autoscaling) is dropped, not waited on.
                max_restarts=0,
            ).remote(self._interval_s, self._max_samples)
            self._actors.append(actor)
        ray.get([a.start.remote() for a in self._actors], timeout=120)

    def _outdir(self) -> str:
        explicit = os.environ.get(DIR_ENV)
        if explicit:
            return explicit
        # Default next to the release result JSON so the trace is picked up as an
        # artifact wherever the results are.
        return os.path.dirname(os.path.abspath(os.environ.get("TEST_OUTPUT_JSON", ".")))

    def _stop(self):
        if not self._actors:
            return
        per_node = ray.get([a.stop.remote() for a in self._actors], timeout=300)
        for actor in self._actors:
            ray.kill(actor)
        self._actors = []

        outdir = self._outdir()
        trace_path = os.path.join(outdir, f"node_mem_{self._case}.jsonl")
        try:
            os.makedirs(outdir, exist_ok=True)
            with open(trace_path, "w") as fh:
                for node in per_node:
                    for sample in node["samples"]:
                        fh.write(
                            json.dumps(
                                {
                                    "node_ip": node["node_ip"],
                                    "node_id": node["node_id"],
                                    **sample,
                                }
                            )
                            + "\n"
                        )
        except OSError as e:
            self._error = f"trace write failed: {e!r}"
            trace_path = None

        # Peak "used" is per node, so take the worst node rather than a sum: a sum
        # across nodes answers no question anyone asks about an OOM.
        def _worst(key):
            vals = [(n[key], n["node_ip"]) for n in per_node if n.get(key) is not None]
            return max(vals) if vals else (None, None)

        # Source order is deliberate: the cgroup number is the one the OOM killer acts
        # on, so prefer it and fall back only where it does not exist (macOS).
        peak_used, peak_used_node = _worst("peak_cgroup_used")
        used_source = "cgroup"
        for key, label in (
            ("peak_meminfo_used", "meminfo"),
            ("peak_psutil_used", "psutil"),
        ):
            if peak_used is not None:
                break
            peak_used, peak_used_node = _worst(key)
            used_source = label
        peak_worker_uss, _ = _worst("peak_workers_uss")
        peak_worker_rss, _ = _worst("peak_workers_rss")

        # Worst single node per proctitle — the provenance the node total lacks.
        # USS where available, RSS as a separate field rather than a silent substitute.
        def _top(key):
            top: Dict[str, float] = {}
            for node in per_node:
                for title, val in (node.get(key) or {}).items():
                    top[title] = max(top.get(title, 0), val)
            ranked = sorted(top.items(), key=lambda kv: kv[1], reverse=True)[:8]
            return {t: _bytes_to_gb(v) for t, v in ranked}

        self._summary = {
            "node_mem_peak_used_gb": _bytes_to_gb(peak_used),
            "node_mem_peak_used_node": peak_used_node,
            "node_mem_peak_used_source": used_source if peak_used is not None else None,
            "node_mem_peak_worker_uss_gb": _bytes_to_gb(peak_worker_uss),
            "node_mem_peak_worker_rss_gb": _bytes_to_gb(peak_worker_rss),
            "node_mem_top_workers_uss": _top("peak_uss_by_title"),
            "node_mem_top_workers_rss": _top("peak_rss_by_title"),
            "node_mem_nodes": len(per_node),
            "node_mem_samples": sum(n["num_samples"] for n in per_node),
            "node_mem_trace_path": trace_path,
        }
        node_errors = [n["error"] for n in per_node if n.get("error")]
        if node_errors:
            self._error = "; ".join(sorted(set(node_errors)))

    def summary(self) -> Dict[str, Any]:
        out = dict(self._summary)
        if self._error:
            out["node_mem_error"] = self._error
        return out
