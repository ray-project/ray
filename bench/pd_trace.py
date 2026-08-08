"""Per-request stage timing for PD disaggregation.

Attributes the Ray-vs-native TTFT gap to specific stages instead of inferring
it from aggregate percentiles. Writes one JSON record per request with a
``perf_counter`` stamp at each stage boundary.

Deliberately not OpenTelemetry: at c=256 the span machinery's own cost lands
inside the window being measured. This is an append to a preallocated list
plus one line of JSON at request end.

Enable with ``RAY_PD_TRACE=/path/to/trace.jsonl``; a no-op when unset, so the
instrumentation can stay in the code path for the untraced benchmark runs.

Stages recorded on the decode replica, in request order:

  ``t0``               orchestration entered
  ``choose_start``     before choose_replica (routing + reserve_slot RPC)
  ``chosen``           replica selected, slot reserved
  ``prefill_sent``     prefill dispatched
  ``decode_started``   local decode generator created
  ``first_token``      first decode chunk yielded  <- TTFT lands here
  ``prefill_done``     remote prefill drained
  ``end``              generator exhausted

``chosen - choose_start`` isolates the two blocking RPCs; ``first_token -
decode_started`` isolates the Phase-1 racing loop. Those are the two candidate
costs the aggregate numbers cannot separate.
"""

import json
import os
import threading
import time
from typing import Dict, List, Optional

_TRACE_PATH = os.environ.get("RAY_PD_TRACE", "")
ENABLED = bool(_TRACE_PATH)

_lock = threading.Lock()
_fh = None


def _out():
    """Open the trace file lazily, once per process."""
    global _fh
    if _fh is None:
        with _lock:
            if _fh is None:
                # Each replica is its own process; suffix by pid so replicas
                # never interleave partial lines into one file.
                base, ext = os.path.splitext(_TRACE_PATH)
                _fh = open(f"{base}.{os.getpid()}{ext or '.jsonl'}", "a")
    return _fh


class RequestTrace:
    """Stage stamps for one request. All times are ``perf_counter`` seconds."""

    __slots__ = ("stages", "meta")

    def __init__(self, **meta):
        self.stages: List[tuple] = []
        self.meta: Dict = meta

    def mark(self, stage: str) -> None:
        self.stages.append((stage, time.perf_counter()))

    def emit(self) -> None:
        """Write the record, converting stamps to ms deltas from the first."""
        if not self.stages:
            return
        t0 = self.stages[0][1]
        rec = dict(self.meta)
        rec["stages_ms"] = {name: (t - t0) * 1000.0 for name, t in self.stages}
        rec["total_ms"] = (self.stages[-1][1] - t0) * 1000.0
        line = json.dumps(rec)
        fh = _out()
        # One write per request; the GIL makes a single write() of a short
        # line effectively atomic, so no lock on the hot path.
        fh.write(line + "\n")
        fh.flush()


class _NullTrace:
    """Zero-cost stand-in when tracing is disabled."""

    __slots__ = ()

    def mark(self, stage: str) -> None:
        pass

    def emit(self) -> None:
        pass


_NULL = _NullTrace()


def new_trace(**meta) -> "RequestTrace":
    """A live trace when enabled, else a no-op object."""
    if not ENABLED:
        return _NULL
    return RequestTrace(**meta)


def summarize(paths: List[str], concurrency: Optional[int] = None) -> None:
    """Print per-stage p50/p95 across trace files. Used offline, not on the node."""

    records = []
    for p in paths:
        with open(p) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    if concurrency is not None:
        records = [r for r in records if r.get("concurrency") == concurrency]
    if not records:
        print("no records")
        return

    # Stage order is stable per request; take it from the longest record so a
    # request that errored early doesn't truncate the column list.
    order = max((list(r["stages_ms"].keys()) for r in records), key=len)
    print(f"n={len(records)}")
    print(f"{'stage':>16} {'p50 ms':>9} {'p95 ms':>9} {'delta p50':>10}")
    prev = 0.0
    for stage in order:
        vals = sorted(r["stages_ms"][stage] for r in records if stage in r["stages_ms"])
        if not vals:
            continue
        p50 = vals[int(len(vals) * 0.5)]
        p95 = vals[int(len(vals) * 0.95)]
        print(f"{stage:>16} {p50:>9.2f} {p95:>9.2f} {p50 - prev:>10.2f}")
        prev = p50


def collect(pattern: str, out_path: str) -> int:
    """Merge every replica's trace file into one. Returns the record count.

    Each replica writes its own pid-suffixed file, and the pids aren't known
    until after the run, so exporting them off the node one by one means
    listing first. Merging to a single known path makes it one export.
    """
    import glob

    n = 0
    with open(out_path, "w") as out:
        for path in sorted(glob.glob(pattern)):
            with open(path) as fh:
                for line in fh:
                    if line.strip():
                        out.write(line)
                        n += 1
    return n


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "collect":
        # bench/pd_trace.py collect '/tmp/pdtrace.*.jsonl' /tmp/pdtrace_all.jsonl
        count = collect(sys.argv[2], sys.argv[3])
        print(f"merged {count} records -> {sys.argv[3]}")
    else:
        summarize(sys.argv[1:])
