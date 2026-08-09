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

  ``http_in``          ASGI handler entered (first line of user code)
  ``body_ready``       request body parsed/validated, raw info extracted
  ``t0``               orchestration entered
  ``choose_start``     before choose_replica (routing + reserve_slot RPC)
  ``chosen``           replica selected, slot reserved
  ``prefill_sent``     prefill dispatched
  ``decode_started``   local decode generator created
  ``first_token``      first decode chunk yielded  <- TTFT lands here
  ``prefill_done``     remote prefill drained
  ``end``              generator exhausted
  ``http_out``         first chunk handed to the HTTP response layer

``chosen - choose_start`` isolates the two blocking RPCs; ``first_token -
decode_started`` isolates the Phase-1 racing loop.

``http_in`` is the earliest point reachable from user code on the replica.
Client-measured TTFT minus ``http_in`` is everything upstream of it -- Serve
proxy, ASGI transport, replica scheduling -- which the first version of this
tracer could not see. A c=256 run measured 412ms client-side against 165ms
from ``t0``, so that upstream window was the largest single unattributed
cost; ``http_in``/``body_ready`` split it from request shaping.

The wall-clock stamps (``*_wall``) exist for exactly that subtraction: they
are ``time.time()``, comparable against a client-side timestamp, whereas the
``perf_counter``-based stage offsets are only comparable within one process.
"""

import contextvars
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

    __slots__ = ("stages", "meta", "_emitted", "_wall0")

    def __init__(self, **meta):
        self.stages: List[tuple] = []
        self.meta: Dict = meta
        # Guards against a second emit() when both the ASGI wrapper and the
        # orchestrator own a reference to the same trace (they do, by design:
        # whichever finishes last must not write a duplicate record).
        self._emitted = False
        # Wall clock paired with the first mark, so offsets computed against
        # perf_counter can still be placed on a timeline shared with a client.
        self._wall0: Optional[float] = None

    def mark(self, stage: str) -> None:
        if self._wall0 is None:
            self._wall0 = time.time()
        self.stages.append((stage, time.perf_counter()))

    def emit(self) -> None:
        """Write the record, converting stamps to ms deltas from the first."""
        if self._emitted or not self.stages:
            return
        self._emitted = True
        t0 = self.stages[0][1]
        rec = dict(self.meta)
        rec["stages_ms"] = {name: (t - t0) * 1000.0 for name, t in self.stages}
        rec["total_ms"] = (self.stages[-1][1] - t0) * 1000.0
        # Absolute wall clock of the first stage. A client that logs its own
        # send time can subtract these to get the upstream (proxy/transport)
        # window that no replica-side stamp can observe.
        if self._wall0 is not None:
            rec["t0_wall"] = self._wall0
            rec["end_wall"] = self._wall0 + rec["total_ms"] / 1000.0
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

# The ASGI handler creates the trace, but ``_pd_handle_request`` is reached
# through ``LLMServer.chat``/``completions``, whose signatures are shared with
# every non-P/D server -- threading a trace argument through them would change
# public-ish method signatures for benchmark-only instrumentation. A ContextVar
# is copied into each asyncio task automatically, so the orchestrator picks up
# the trace its own request created and never another concurrent request's.
_current: contextvars.ContextVar = contextvars.ContextVar("pd_trace", default=None)


def new_trace(**meta) -> "RequestTrace":
    """A live trace when enabled, else a no-op object."""
    if not ENABLED:
        return _NULL
    return RequestTrace(**meta)


def start_request(**meta) -> "RequestTrace":
    """Create a trace for this request and publish it to the ContextVar.

    Called at the HTTP boundary. ``current()`` then returns this object for
    everything downstream in the same asyncio task.
    """
    if not ENABLED:
        return _NULL
    trace = RequestTrace(**meta)
    _current.set(trace)
    return trace


def current() -> "RequestTrace":
    """The trace for the in-flight request, or a no-op if there is none.

    Returns the no-op (rather than creating one) when nothing upstream started
    a trace: the non-direct-streaming path reaches the orchestrator through the
    separate ingress deployment, where no ASGI hook of ours runs.
    """
    if not ENABLED:
        return _NULL
    return _current.get() or _NULL


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

    def _pct(vals, p):
        vals = sorted(vals)
        k = (len(vals) - 1) * p / 100.0
        f = int(k)
        c = min(f + 1, len(vals) - 1)
        return vals[f] + (vals[c] - vals[f]) * (k - f)

    # Stage order is stable per request; take it from the longest record so a
    # request that errored early doesn't truncate the column list.
    order = max((list(r["stages_ms"].keys()) for r in records), key=len)
    print(f"n={len(records)}")
    print(f"{'stage':>16} {'p50 ms':>9} {'p95 ms':>9} {'delta p50':>10}")
    prev = 0.0
    for stage in order:
        vals = [r["stages_ms"][stage] for r in records if stage in r["stages_ms"]]
        if not vals:
            continue
        p50 = _pct(vals, 50)
        p95 = _pct(vals, 95)
        print(f"{stage:>16} {p50:>9.2f} {p95:>9.2f} {p50 - prev:>10.2f}")
        prev = p50

    # Server-side TTFT, for subtracting from a client-side measurement. The
    # difference is the upstream window (proxy, transport, replica scheduling)
    # that no stamp in this process can reach.
    ttfts = [
        r["stages_ms"]["first_token"]
        for r in records
        if "first_token" in r["stages_ms"]
    ]
    if ttfts:
        zero = order[0]
        print(
            f"\nserver-side TTFT ({zero} -> first_token): "
            f"p50={_pct(ttfts, 50):.2f}ms  p95={_pct(ttfts, 95):.2f}ms"
        )
        if zero == "http_in":
            print(
                "  subtract this from the client's TTFT p50 to size everything "
                "upstream of the replica handler."
            )
        else:
            print(
                "  NOTE: traced from 't0', not 'http_in' -- the HTTP-boundary "
                "stages are missing, so this understates replica-side time."
            )


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
