"""Benchmark runner functions for multi-turn LLM inference benchmarks.

Contains:
  - Concurrency-based (closed-loop) runner
  - Rate-based (constant QPS) runner
  - Smoke test runner
  - Direct entry point (run_direct)
"""

import argparse
import asyncio
import hashlib
import json
import logging
import math
import random
import sys
import time
from collections import Counter
from statistics import mean
from typing import Optional

import aiohttp
import numpy as np
from tqdm import tqdm

from ray.llm._internal.serve.benchmark.http_client import send_chat_completion
from ray.llm._internal.serve.benchmark.models import TurnMetric, WorkloadSpec
from ray.llm._internal.serve.benchmark.reporting import report_results
from ray.llm._internal.serve.benchmark.text_gen import (
    Conversation,
    TextGenerator,
    conversation_factory,
)
from ray.llm._internal.serve.benchmark.turn import execute_single_turn

# ============================================================================
# LOGGING SETUP
# ============================================================================


class TqdmLoggingHandler(logging.Handler):
    """Custom logging handler that writes through tqdm to keep progress bar at bottom."""

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg, file=sys.stderr)
            self.flush()
        except Exception:
            self.handleError(record)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[TqdmLoggingHandler()],
)
logger = logging.getLogger(__name__)


# ============================================================================
# BENCHMARK STATE (concurrency mode)
# ============================================================================


class BenchmarkState:
    """Centralized state management for benchmark execution.

    Consolidates all shared state and synchronization into a single class
    with one lock, replacing the previous 7+ separate locks.
    """

    def __init__(self, spec: WorkloadSpec):
        self.spec = spec

        # Metrics storage
        self.warmup_metrics: list[TurnMetric] = []
        self.measured_metrics: list[TurnMetric] = []

        # Session tracking
        self.conversations: dict[int, Conversation] = {}
        self.session_is_warmup: dict[int, bool] = {}
        self.next_session_id = 0
        self.completed_sessions = 0

        # Concurrency tracking
        self.inflight = 0
        self.max_inflight = 0
        self.inflight_samples: list[int] = []

        # Warm-up state
        self.warmup_complete = False
        self.active_turns: dict[str, int] = {}  # session_id -> turn_idx

        # Single lock for all state
        self._lock = asyncio.Lock()

    async def record_metric(self, metric: TurnMetric) -> None:
        """Thread-safe metric recording."""
        async with self._lock:
            if self.warmup_complete:
                self.measured_metrics.append(metric)
            else:
                self.warmup_metrics.append(metric)

    async def get_next_session(self) -> Optional[int]:
        """Get next available session ID, or None if exhausted."""
        async with self._lock:
            limit = self.spec.num_sessions
            if limit is None or self.next_session_id < limit:
                session_id = self.next_session_id
                self.next_session_id += 1
                return session_id
            return None

    async def has_remaining_sessions(self) -> bool:
        """True if more session indices can still be allocated (does not consume an ID)."""
        async with self._lock:
            limit = self.spec.num_sessions
            if limit is None:
                return True
            return self.next_session_id < limit

    async def get_inflight(self) -> int:
        """Current in-flight HTTP requests (concurrency benchmark)."""
        async with self._lock:
            return self.inflight

    async def get_or_create_conversation(
        self,
        session_idx: int,
        spec: WorkloadSpec,
        shared_system_text: str,
        text_gen: TextGenerator,
    ) -> tuple[Conversation, bool]:
        """Get or create a conversation. Returns (conversation, is_warmup)."""
        async with self._lock:
            if session_idx not in self.conversations:
                conv = conversation_factory(
                    session_idx, spec, shared_system_text, text_gen
                )
                self.conversations[session_idx] = conv
                # Mark as warmup if steady-state not yet reached
                self.session_is_warmup[session_idx] = not self.warmup_complete
            else:
                conv = self.conversations[session_idx]

            is_warmup = self.session_is_warmup[session_idx]
            return conv, is_warmup

    async def mark_session_active(self, session_id: str, turn_idx: int) -> None:
        """Mark a session as active at a specific turn."""
        async with self._lock:
            self.active_turns[session_id] = turn_idx

    async def mark_session_complete(self, session_idx: int, session_id: str) -> None:
        """Mark a session as complete and clean up."""
        async with self._lock:
            self.active_turns.pop(session_id, None)
            if session_idx in self.conversations:
                del self.conversations[session_idx]
            self.completed_sessions += 1

    async def track_inflight_start(self) -> int:
        """Track start of inflight request. Returns current inflight count."""
        async with self._lock:
            self.inflight += 1
            if self.inflight > self.max_inflight:
                self.max_inflight = self.inflight
            self.inflight_samples.append(self.inflight)
            return self.inflight

    async def track_inflight_end(self) -> int:
        """Track end of inflight request. Returns inflight count BEFORE decrement."""
        async with self._lock:
            current = self.inflight
            self.inflight -= 1
            return current

    async def mark_warmup_complete(self) -> None:
        """Mark warm-up phase complete. All future sessions are measurement."""
        async with self._lock:
            self.warmup_complete = True

    async def check_and_complete_warmup(self) -> bool:
        """Check if steady-state reached and complete warm-up if so.

        Returns True if warm-up was just completed, False otherwise.
        """
        if self.warmup_complete:
            return False

        async with self._lock:
            # Double-check after acquiring lock
            if self.warmup_complete:
                return False

            if not self.active_turns:
                return False

            # Calculate entropy. For num_turns==1, max_entropy=log2(1)=0 so the
            # threshold is 0.0 — any non-empty active_turns distribution satisfies
            # entropy >= 0, so warm-up ends on the first turn completion.
            max_entropy = np.log2(self.spec.num_turns)
            entropy_threshold = 0.50 * max_entropy
            entropy = self._calculate_entropy()

            if entropy >= entropy_threshold:
                self.warmup_complete = True
                pct_of_target = (
                    (entropy / entropy_threshold * 100)
                    if entropy_threshold > 0
                    else 100.0
                )
                logger.info(
                    "Steady-state reached! Entropy=%.3f (threshold=%.3f, %.0f%% of target)",
                    entropy,
                    entropy_threshold,
                    pct_of_target,
                )
                logger.info(
                    "Warm-up complete (%d sessions, %d requests discarded). "
                    "Now measuring steady-state...",
                    self.completed_sessions,
                    len(self.warmup_metrics),
                )
                return True

        return False

    def _calculate_entropy(self) -> float:
        """Calculate Shannon entropy of active turn distribution (must be called with lock held)."""
        if not self.active_turns:
            return 0.0
        counts = Counter(self.active_turns.values())
        total = sum(counts.values())
        probs = [c / total for c in counts.values()]
        return -sum(p * np.log2(p) for p in probs if p > 0)

    async def get_stats(self) -> dict:
        """Get current benchmark stats (thread-safe)."""
        async with self._lock:
            return {
                "warmup_metrics": len(self.warmup_metrics),
                "measured_metrics": len(self.measured_metrics),
                "completed_sessions": self.completed_sessions,
                "max_inflight": self.max_inflight,
                "avg_inflight": (
                    mean(self.inflight_samples) if self.inflight_samples else 0
                ),
                "warmup_complete": self.warmup_complete,
            }


# ============================================================================
# PROGRESS BAR
# ============================================================================


def create_progress_bar(total: int) -> tqdm:
    """Create a sticky progress bar for request tracking."""
    return tqdm(
        total=total,
        desc="Requests",
        unit="req",
        bar_format=(
            "{l_bar}{bar}| {n_fmt}/{total_fmt} "
            "[{elapsed}<{remaining}, {rate_fmt}, inflight={postfix}]"
        ),
        position=0,
        leave=True,
        file=sys.stderr,
    )


# ============================================================================
# CONCURRENCY-BASED BENCHMARK
# ============================================================================


async def run_benchmark(
    spec: WorkloadSpec,
    base_url: str,
    model: str,
    shared_system_text: str,
    text_gen: TextGenerator,
    first_chunk_threshold: int = 16,
    api_key: Optional[str] = None,
    warmup_jitter_max_s: float = 10.0,
) -> list[TurnMetric]:
    """Run the full benchmark with rolling session pool and steady-state warm-up.

    Maintains a pool of `concurrency` worker tasks that continuously pull turns
    from a queue. This creates a natural turn distribution mix (some sessions at
    turn 0, others at turn 5, etc.) after warm-up.

    Warm-up continues until steady-state is reached: turn distribution entropy
    >= 50% of maximum (indicating reasonable spread across multiple turns).
    """
    if spec.concurrency is None:
        raise ValueError("closed-loop benchmark requires concurrency to be set")
    if spec.num_sessions is None:
        raise ValueError("closed-loop (concurrency) benchmark requires --num-sessions")

    state = BenchmarkState(spec)
    request_sem = asyncio.Semaphore(spec.concurrency)

    bench_start_ns = time.perf_counter_ns()

    total_expected = spec.num_sessions * spec.num_turns

    connector = aiohttp.TCPConnector(limit=spec.concurrency * 2)
    async with aiohttp.ClientSession(connector=connector) as http_session:
        pbar = create_progress_bar(total_expected)
        pbar.set_postfix_str("0")

        # Turn queue: (session_idx, turn_idx, delay_until)
        turn_queue: asyncio.Queue[tuple[int, int, float]] = asyncio.Queue()

        async def enqueue_new_session() -> bool:
            """Add a new session (turn 0) to the queue if any remain."""
            session_idx = await state.get_next_session()
            if session_idx is not None:
                # Stagger initial session launches using configured ramp_interval
                delay = session_idx * spec.ramp_interval
                await turn_queue.put((session_idx, 0, time.perf_counter() + delay))
                return True
            return False

        # Initialize: fill queue with just enough sessions to saturate concurrency
        for _ in range(min(spec.concurrency, spec.num_sessions)):
            await enqueue_new_session()

        async def execute_turn(session_idx: int, turn_idx: int) -> None:
            """Execute a single turn for a session."""
            conv, is_warmup = await state.get_or_create_conversation(
                session_idx, spec, shared_system_text, text_gen
            )
            await state.mark_session_active(conv.session_id, turn_idx)

            async with request_sem:
                await state.track_inflight_start()
                try:
                    outcome = await execute_single_turn(
                        http_session=http_session,
                        conv=conv,
                        turn_idx=turn_idx,
                        base_url=base_url,
                        model=model,
                        max_tokens=spec.osl,
                        bench_start_ns=bench_start_ns,
                        first_chunk_threshold=first_chunk_threshold,
                        api_key=api_key,
                    )
                except Exception as e:
                    logger.error(
                        "Session %s turn %d failed: %s",
                        conv.session_id,
                        turn_idx,
                        e,
                    )
                    current_inflight = await state.track_inflight_end()
                    pbar.set_postfix_str(str(current_inflight))
                    pbar.update(1)
                    await state.mark_session_complete(session_idx, conv.session_id)
                    await enqueue_new_session()
                    return
                current_inflight = await state.track_inflight_end()

            await state.record_metric(outcome.metric)
            pbar.set_postfix_str(str(current_inflight))
            pbar.update(1)

            if turn_idx < spec.num_turns - 1:
                next_turn = turn_idx + 1
                if is_warmup:
                    jitter = np.random.uniform(0.0, warmup_jitter_max_s)
                    delay_until = time.perf_counter() + jitter
                else:
                    delay_until = time.perf_counter() + spec.think_time
                await turn_queue.put((session_idx, next_turn, delay_until))
            else:
                await state.mark_session_complete(session_idx, conv.session_id)
                await enqueue_new_session()

            await state.check_and_complete_warmup()

        async def worker() -> None:
            """Worker task: continuously pull turns from queue and execute them."""
            while True:
                try:
                    session_idx, turn_idx, delay_until = await asyncio.wait_for(
                        turn_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    # Do not call get_next_session() here — it allocates a session ID
                    # without enqueueing work. Peek completion instead.
                    if (
                        not await state.has_remaining_sessions()
                        and turn_queue.empty()
                        and await state.get_inflight() == 0
                    ):
                        break
                    continue

                # Wait until delay_until (for think time/jitter)
                now = time.perf_counter()
                wait_time = delay_until - now
                if wait_time > 0:
                    await asyncio.sleep(wait_time)

                await execute_turn(session_idx, turn_idx)

        max_entropy = np.log2(spec.num_turns)
        entropy_threshold = 0.50 * max_entropy

        logger.info(
            "Starting benchmark: %d sessions x %d turns = %d requests, concurrency=%d",
            spec.num_sessions,
            spec.num_turns,
            total_expected,
            spec.concurrency,
        )
        logger.info(
            "  Rolling pool: maintaining ~%d active sessions across all turns",
            spec.concurrency,
        )
        logger.info(
            "  Warm-up: until steady-state (entropy >= %.3f, 50%% of max=%.3f)",
            entropy_threshold,
            max_entropy,
        )
        logger.info(
            "  Measurement: starts after warm-up, using think-time=%.1fs",
            spec.think_time,
        )

        # Launch worker pool
        workers = [asyncio.create_task(worker()) for _ in range(spec.concurrency)]
        await asyncio.gather(*workers)

        pbar.close()

    bench_elapsed_s = (time.perf_counter_ns() - bench_start_ns) / 1e9

    stats = await state.get_stats()
    logger.info(
        "Benchmark complete: %d measured requests in %.1fs (%.1f req/s), "
        "%d warmup requests discarded",
        stats["measured_metrics"],
        bench_elapsed_s,
        stats["measured_metrics"] / bench_elapsed_s if bench_elapsed_s > 0 else 0,
        stats["warmup_metrics"],
    )
    logger.info(
        "Concurrency: max=%d, avg=%.1f",
        stats["max_inflight"],
        stats["avg_inflight"],
    )

    return state.measured_metrics


# ============================================================================
# RATE-BASED BENCHMARK
# ============================================================================


async def run_rate_based_benchmark(
    spec: WorkloadSpec,
    base_url: str,
    model: str,
    shared_system_text: str,
    text_gen: TextGenerator,
    first_chunk_threshold: int = 16,
    duration_s: float = 0.0,
    warmup_s: float = 0.0,
    api_key: Optional[str] = None,
) -> tuple[list[TurnMetric], int]:
    """Closed-loop constant-QPS benchmark with turn-order guarantees.

    Uses separate asyncio locks for metrics/inflight here (time-based warm-up
    filtering) while :class:`BenchmarkState` powers the concurrency path
    (entropy-based warm-up). Unifying both under one state object is possible
    but is left for a follow-up refactor.

    Architecture
    ============
    - **Ready queue**: sessions whose previous turn has completed push themselves
      here so their next turn can be dispatched.
    - **Pacer**: a single coroutine drains the ready queue at exactly
      ``spec.request_rate`` req/s using a token-bucket algorithm.  It sleeps
      until the next slot is due, pops one item from the ready queue, and fires
      the turn as a background task.
    - **Lazy session pool**: the pacer holds a pre-computed budget of session
      indices (``ceil(rate * duration / num_turns)``).  When the ready queue is
      empty (all active sessions are waiting for server responses), it mints a
      fresh conversation from the budget on the spot -- no upfront allocation.
      If the budget is also exhausted (server extremely slow), it generates
      extra conversations beyond the budget and logs a warning.
    - **Duration-based**: the benchmark runs for ``duration_s`` seconds, then
      the pacer stops and waits for all in-flight requests to complete.
    - **Turn-order guarantee**: turn k+1 is only enqueued after the HTTP
      response for turn k is fully received and injected into history.

    Session budget
    ==============
    Budget = ceil(request_rate * duration_s / num_turns).
    This is the exact number of unique sessions needed if the server keeps up.
    If the server is slower, some in-flight sessions are unavailable and the
    pacer mints extras beyond the budget to maintain QPS.
    """
    if spec.request_rate is None:
        raise ValueError("rate-based benchmark requires request_rate to be set")
    if warmup_s < 0:
        raise ValueError("warmup_s must be >= 0")

    bench_start_ns = time.perf_counter_ns()

    # --------------------------------------------------------------------------
    # Session budget: computed from rate * duration / num_turns
    # --------------------------------------------------------------------------
    if duration_s > 0:
        session_budget = math.ceil(spec.request_rate * duration_s / spec.num_turns)
    elif spec.num_sessions is not None:
        session_budget = spec.num_sessions
    else:
        session_budget = 1000  # fallback; shouldn't reach here after CLI validation

    # Monotonically increasing session index (never resets; unique per conversation)
    next_session_idx = 0
    sessions_beyond_budget = 0  # count of extra sessions created due to server lag

    def _next_conv() -> Conversation:
        """Mint a fresh conversation, lazily, from the session budget (or beyond)."""
        nonlocal next_session_idx, sessions_beyond_budget
        idx = next_session_idx
        next_session_idx += 1
        if idx >= session_budget:
            sessions_beyond_budget += 1
        return conversation_factory(idx, spec, shared_system_text, text_gen)

    # --------------------------------------------------------------------------
    # Ready queue: (conv, turn_idx) -- only sessions whose prior turn completed
    # --------------------------------------------------------------------------
    ready_queue: asyncio.Queue[tuple[Conversation, int]] = asyncio.Queue()

    # --------------------------------------------------------------------------
    # Shared counters (only mutated by execute_turn background tasks)
    # --------------------------------------------------------------------------
    metrics: list[TurnMetric] = []
    warmup_metrics: list[TurnMetric] = []
    metrics_lock = asyncio.Lock()

    inflight = 0
    max_inflight = 0
    inflight_samples: list[int] = []
    inflight_lock = asyncio.Lock()

    stop_dispatching = asyncio.Event()
    all_done = asyncio.Event()

    total_slots = int(spec.request_rate * duration_s) if duration_s > 0 else 0

    # Two stacked progress bars: sent (top) chased by done (bottom).
    _bar_fmt_sent = (
        "{desc}: {bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    )
    _bar_fmt_done = (
        "{desc}: {bar}| {n_fmt}/{total_fmt} "
        "[{elapsed}<{remaining}, {rate_fmt}] {postfix}"
    )
    _bar_fmt_no_total = "{desc}: {bar}| {n_fmt} [{elapsed}, {rate_fmt}]"
    _bar_fmt_no_total_pf = "{desc}: {bar}| {n_fmt} [{elapsed}, {rate_fmt}] {postfix}"

    pbar_sent = tqdm(
        total=total_slots if total_slots > 0 else None,
        desc="  Sent",
        unit="req",
        bar_format=_bar_fmt_sent if total_slots > 0 else _bar_fmt_no_total,
        position=1,
        leave=True,
        file=sys.stderr,
        colour="cyan",
    )
    pbar_done = tqdm(
        total=total_slots if total_slots > 0 else None,
        desc="  Done",
        unit="req",
        bar_format=_bar_fmt_done if total_slots > 0 else _bar_fmt_no_total_pf,
        position=0,
        leave=True,
        file=sys.stderr,
        colour="green",
    )
    pbar_done.set_postfix_str("inflight=0")

    logger.info(
        "Closed-loop QPS benchmark: target=%.2f req/s, duration=%.1fs, "
        "num_turns=%d, session_budget=%d (auto from rate*duration/turns)",
        spec.request_rate,
        duration_s,
        spec.num_turns,
        session_budget,
    )
    logger.info(
        "Stats warm-up: %.1fs (requests dispatched before this are excluded "
        "from reported metrics)",
        warmup_s,
    )

    warmup_cutoff_ns = bench_start_ns + int(warmup_s * 1e9)

    # --------------------------------------------------------------------------
    # Per-turn execution (background task, one per in-flight request)
    # --------------------------------------------------------------------------
    async def execute_turn(
        conv: Conversation,
        turn_idx: int,
        http_session: aiohttp.ClientSession,
    ) -> None:
        nonlocal inflight, max_inflight

        async with inflight_lock:
            inflight += 1
            if inflight > max_inflight:
                max_inflight = inflight
            inflight_samples.append(inflight)
            pbar_done.set_postfix_str(f"inflight={inflight}")

        try:
            outcome = await execute_single_turn(
                http_session=http_session,
                conv=conv,
                turn_idx=turn_idx,
                base_url=base_url,
                model=model,
                max_tokens=spec.osl,
                bench_start_ns=bench_start_ns,
                first_chunk_threshold=first_chunk_threshold,
                api_key=api_key,
            )
        except Exception as e:
            logger.error(
                "Session %s turn %d failed: %s",
                conv.session_id,
                turn_idx,
                e,
            )
            async with inflight_lock:
                inflight -= 1
                pbar_done.set_postfix_str(f"inflight={inflight}")
            pbar_done.update(1)
            if not stop_dispatching.is_set():
                await ready_queue.put((_next_conv(), 0))
            async with inflight_lock:
                if stop_dispatching.is_set() and inflight == 0:
                    all_done.set()
            return

        async with inflight_lock:
            inflight -= 1
            pbar_done.set_postfix_str(f"inflight={inflight}")

        async with metrics_lock:
            req_start_ns = bench_start_ns + int(outcome.metric.start_time_ms * 1e6)
            if req_start_ns >= warmup_cutoff_ns:
                metrics.append(outcome.metric)
            else:
                warmup_metrics.append(outcome.metric)
        pbar_done.update(1)

        if not stop_dispatching.is_set():
            next_turn = turn_idx + 1
            if next_turn < spec.num_turns:
                await ready_queue.put((conv, next_turn))
            else:
                await ready_queue.put((_next_conv(), 0))

        async with inflight_lock:
            if stop_dispatching.is_set() and inflight == 0:
                all_done.set()

    # --------------------------------------------------------------------------
    # Pacer: token-bucket, single coroutine, no lock needed
    # --------------------------------------------------------------------------
    async def pacer(http_session: aiohttp.ClientSession) -> None:
        """Dispatch one request per slot at exactly request_rate req/s."""
        nonlocal next_session_idx, sessions_beyond_budget
        interval_s = 1.0 / spec.request_rate
        next_dispatch = time.perf_counter()

        # Throttle "server lagging" log to once per second
        _last_lag_log: float = 0.0
        _lag_count_since_log: int = 0

        while not stop_dispatching.is_set():
            now = time.perf_counter()
            wait = next_dispatch - now
            if wait > 0:
                await asyncio.sleep(wait)

            if stop_dispatching.is_set():
                break

            # Prefer a session whose prior turn already completed (ready queue).
            # If empty the server is lagging -- mint a fresh session from the
            # budget (or beyond) so QPS is maintained.
            try:
                conv, turn_idx = ready_queue.get_nowait()
                # Ready queue had something -- server is keeping up at this slot
                pbar_sent.set_description("  Sent")
            except asyncio.QueueEmpty:
                _lag_count_since_log += 1
                now = time.perf_counter()
                beyond_budget = next_session_idx >= session_budget
                # Update sent bar description to show lag state continuously
                if beyond_budget:
                    pbar_sent.set_description(
                        f"  Sent [lag+{sessions_beyond_budget + _lag_count_since_log}]"
                    )
                else:
                    pbar_sent.set_description(
                        f"  Sent [lag {next_session_idx}/{session_budget}]"
                    )
                if now - _last_lag_log >= 1.0:
                    elapsed = (time.perf_counter_ns() - bench_start_ns) / 1e9
                    if beyond_budget:
                        logger.warning(
                            "Server lagging at %.1fs: ready queue empty, budget exhausted "
                            "(budget=%d). Minted %d extra session(s) in last %.1fs to hold QPS.",
                            elapsed,
                            session_budget,
                            _lag_count_since_log,
                            now - _last_lag_log if _last_lag_log else elapsed,
                        )
                    else:
                        logger.info(
                            "Server lagging at %.1fs: ready queue empty, pulling from budget "
                            "(%d/%d sessions used). %d slot(s) filled from budget in last %.1fs.",
                            elapsed,
                            next_session_idx,
                            session_budget,
                            _lag_count_since_log,
                            now - _last_lag_log if _last_lag_log else elapsed,
                        )
                    _last_lag_log = now
                    _lag_count_since_log = 0
                conv = _next_conv()
                turn_idx = 0

            asyncio.create_task(execute_turn(conv, turn_idx, http_session))
            pbar_sent.update(1)
            next_dispatch += interval_s

        # Pacer finished -- trigger all_done if nothing is in-flight
        async with inflight_lock:
            if inflight == 0:
                all_done.set()

    # --------------------------------------------------------------------------
    # Duration timer
    # --------------------------------------------------------------------------
    async def duration_timer() -> None:
        await asyncio.sleep(duration_s)
        logger.info("Duration %.1fs elapsed -- stopping new dispatches.", duration_s)
        stop_dispatching.set()

    # --------------------------------------------------------------------------
    # Main execution
    # --------------------------------------------------------------------------
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as http_session:
        # Seed the ready queue with enough turn-0 conversations to keep the
        # pacer busy for the first second without generating them mid-sleep.
        seed_count = max(1, int(spec.request_rate) + 1)
        for _ in range(seed_count):
            await ready_queue.put((_next_conv(), 0))

        timer_task = asyncio.create_task(duration_timer())
        pacer_task = asyncio.create_task(pacer(http_session))

        await timer_task
        await pacer_task

        pbar_sent.set_description("  Sent")
        logger.info("Waiting for %d in-flight requests to complete...", inflight)
        await all_done.wait()

        pbar_sent.close()
        pbar_done.close()

    bench_elapsed_s = (time.perf_counter_ns() - bench_start_ns) / 1e9
    avg_inf = sum(inflight_samples) / len(inflight_samples) if inflight_samples else 0.0
    logger.info(
        "Benchmark complete: %d requests in %.1fs (%.2f req/s actual); "
        "max_inflight=%d, avg_inflight=%.1f",
        len(metrics),
        bench_elapsed_s,
        len(metrics) / bench_elapsed_s if bench_elapsed_s > 0 else 0,
        max_inflight,
        avg_inf,
    )
    if warmup_metrics:
        logger.info(
            "Warm-up filtered out %d requests (start_time < %.1fs)",
            len(warmup_metrics),
            warmup_s,
        )
    logger.info(
        "Sessions: budget=%d, used=%d (%s)",
        session_budget,
        next_session_idx,
        f"{sessions_beyond_budget} extra beyond budget -- server was lagging"
        if sessions_beyond_budget > 0
        else "within budget",
    )
    return metrics, len(warmup_metrics)


# ============================================================================
# SEED & ENTRY POINTS
# ============================================================================


def set_seed(args: argparse.Namespace) -> int:
    """Set random seed combining base seed with workload spec hash.

    This ensures:
    - Reproducibility: same configuration always generates same conversations
    - Variation: different configurations (concurrency, rate, ISL, etc.)
      generate different conversations

    Args:
        args: Parsed command-line arguments.

    Returns:
        The combined seed value used.
    """
    # Generate random base seed if not provided
    if args.seed is None:
        base_seed = random.randint(0, 2**31 - 1)
        logger.info("No seed provided, generated random base seed: %d", base_seed)
    else:
        base_seed = args.seed

    # Get all args but exclude parameters that don't affect workload shape
    # (e.g., output paths, server URLs, infrastructure settings)
    exclude_params = {
        "seed",  # Base seed (used separately)
        "save_result",  # Output path doesn't affect workload
        "base_url",  # Server URL doesn't affect workload
        "model",  # Model name doesn't affect workload shape
        "tokenizer",  # Tokenizer path doesn't affect workload (token counts do)
        "first_chunk_threshold",  # Measurement detail, not workload shape
        "warmup_jitter_max",  # Warm-up pacing only; does not change generated text
    }

    # Include all workload-affecting parameters
    workload_params = {k: v for k, v in vars(args).items() if k not in exclude_params}

    # Create stable string representation and hash it
    workload_str = json.dumps(workload_params, sort_keys=True, default=str)
    workload_hash = int(hashlib.sha256(workload_str.encode()).hexdigest()[:8], 16)

    # Combine base seed with workload hash; clamp to numpy's valid range [0, 2**32-1]
    combined_seed = (base_seed + workload_hash) % (2**32)

    # Set seeds
    random.seed(combined_seed)
    np.random.seed(combined_seed)

    logger.info(
        "Random seed: base=%d, workload_hash=%d, combined=%d",
        base_seed,
        workload_hash,
        combined_seed,
    )

    return combined_seed


async def _run_smoke_async(args) -> dict:
    """Run a single smoke-test request and return metrics."""
    messages = [{"role": "user", "content": "Say 'this is a test' and nothing else."}]
    async with aiohttp.ClientSession() as session:
        result = await send_chat_completion(
            session=session,
            base_url=args.base_url,
            model=args.model,
            messages=messages,
            first_chunk_threshold=args.first_chunk_threshold,
            api_key=getattr(args, "api_key", None),
        )
    return {
        "ttft_ms": round(result.ttft_ms, 2),
        "fc_ms": round(result.fc_ms, 2),
        "tpot_ms": round(result.tpot_ms, 2),
        "latency_ms": round(result.latency_ms, 2),
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
    }


def run_smoke(args) -> int:
    """Run smoke test synchronously, return exit code."""
    try:
        result = asyncio.run(_run_smoke_async(args))
        print(json.dumps(result, indent=2))
        return 0
    except Exception as e:
        logger.error(f"Smoke test failed: {e}")
        return 1


def run_direct(args) -> int:
    """Run a single benchmark (concurrency or rate-based) and report results."""
    # Resolve tokenizer
    tokenizer_name = args.tokenizer if args.tokenizer else args.model
    try:
        from transformers import AutoTokenizer

        logger.info("Loading tokenizer: %s", tokenizer_name)
        tokenizer = AutoTokenizer.from_pretrained(
            tokenizer_name, trust_remote_code=True
        )
    except Exception as e:
        logger.error(
            "Failed to load tokenizer '%s'. If your --model is not a valid "
            "HuggingFace repo, provide --tokenizer explicitly. Error: %s",
            tokenizer_name,
            e,
        )
        return 1

    set_seed(args)
    text_gen = TextGenerator(tokenizer)

    # Validate
    if args.concurrency is not None and args.num_sessions is None:
        logger.error("--num-sessions is required when using --concurrency mode.")
        return 1
    if (
        args.request_rate is not None
        and args.num_sessions is None
        and (args.duration is None or args.duration <= 0)
    ):
        logger.error(
            "For --request-rate mode without --num-sessions, specify --duration > 0."
        )
        return 1

    # Build spec (simple mode only)
    # Filter out None values so dataclass defaults (e.g. num_turns=1, osl=1)
    # are preserved when the user omits those CLI flags.
    spec_kwargs = dict(
        num_sessions=args.num_sessions,
        num_turns=args.num_turns,
        osl=args.osl,
        think_time=args.think_time,
        concurrency=args.concurrency,
        request_rate=args.request_rate,
        ramp_interval=args.ramp_interval,
        shared_system_prompt_ratio=args.shared_system_prompt_ratio,
        isl=args.isl,
        hit_rate=args.hit_rate,
        duration_s=args.duration or 0.0,
    )
    spec = WorkloadSpec(**{k: v for k, v in spec_kwargs.items() if v is not None})
    try:
        spec.resolve()
    except ValueError as e:
        logger.error("Workload spec error: %s", e)
        return 1

    spec.print_summary()

    shared_system_text = text_gen.generate(spec.shared_s)

    bench_start = time.perf_counter()
    discarded_warmup_requests = 0
    report_elapsed = None

    api_key = getattr(args, "api_key", None)
    fc_thresh = args.first_chunk_threshold
    warmup_jitter_max = getattr(args, "warmup_jitter_max", 10.0)

    if spec.request_rate is not None:
        if spec.duration_s > 0:
            effective_duration = spec.duration_s
        else:
            # _validate() ensures num_sessions is set when duration_s <= 0
            assert spec.num_sessions is not None
            effective_duration = spec.num_sessions * spec.num_turns / spec.request_rate
        if args.warm_up >= effective_duration:
            logger.error(
                "--warm-up (%.3fs) must be less than effective duration (%.3fs).",
                args.warm_up,
                effective_duration,
            )
            return 1
        metrics, discarded_warmup_requests = asyncio.run(
            run_rate_based_benchmark(
                spec,
                args.base_url,
                args.model,
                shared_system_text,
                text_gen,
                first_chunk_threshold=fc_thresh,
                duration_s=effective_duration,
                warmup_s=args.warm_up,
                api_key=api_key,
            )
        )
        report_elapsed = max(1e-9, effective_duration - args.warm_up)
    else:
        metrics = asyncio.run(
            run_benchmark(
                spec,
                args.base_url,
                args.model,
                shared_system_text,
                text_gen,
                first_chunk_threshold=fc_thresh,
                api_key=api_key,
                warmup_jitter_max_s=warmup_jitter_max,
            )
        )

    bench_elapsed = time.perf_counter() - bench_start
    if report_elapsed is None:
        report_elapsed = bench_elapsed

    report_results(
        metrics,
        spec,
        report_elapsed,
        first_chunk_threshold=fc_thresh,
        save_path=args.save_result,
        warmup_s=args.warm_up if spec.request_rate is not None else 0.0,
        discarded_warmup_requests=discarded_warmup_requests,
    )
    return 0
