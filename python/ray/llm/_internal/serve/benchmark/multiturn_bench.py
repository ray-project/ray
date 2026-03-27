"""Multi-turn benchmark engine for LLM inference servers.

Supports two traffic modes:
  - Concurrency-based (closed-loop): maintains a rolling pool of concurrent sessions
  - Rate-based (constant QPS): dispatches requests at a fixed rate with token-bucket pacing

Key features:
  - Rolling session pool with entropy-based warm-up detection
  - Lazy conversation generation with exact token counts
  - Per-turn latency breakdown (TTFT, FC, TPOT)
  - Cross-session prefix sharing control
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
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from transformers import PreTrainedTokenizerBase

import aiohttp
import numpy as np
from tqdm import tqdm

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
# DATA CLASSES
# ============================================================================


@dataclass
class TurnResult:
    """Result of a single turn's HTTP request."""

    ttft_ms: float  # time to first token
    fc_ms: float  # first-chunk latency (time to N-th content chunk)
    tpot_ms: float  # average time per output token (decode phase)
    latency_ms: float  # total request latency
    input_tokens: int  # reported by server (usage.prompt_tokens)
    output_tokens: int  # reported by server (usage.completion_tokens)
    generated_text: str  # generated text


@dataclass
class TurnMetric:
    """Metrics for a single turn."""

    session_id: str
    turn: int  # 0-indexed
    ttft_ms: float
    fc_ms: float  # first-chunk latency
    tpot_ms: float
    latency_ms: float
    input_tokens: int
    output_tokens: int
    start_time_ms: float  # relative to benchmark start


@dataclass
class WorkloadSpec:
    """Workload specification for multi-turn session benchmarks.

    Supports simple mode: specify isl + hit_rate, derive u and s.
    All parameters are scalar (fixed) values -- no distributions.
    """

    # Core parameters
    num_sessions: Optional[
        int
    ] = None  # N_s: total unique sessions (None = duration-based)
    num_turns: int = 1  # N_t: turns per session
    osl: int = 1  # o: output sequence length per turn
    think_time: float = 0.0  # seconds between turns within a session

    # Traffic (use either concurrency or request_rate, not both)
    concurrency: Optional[int] = None  # C: max concurrent in-flight requests
    request_rate: Optional[
        float
    ] = None  # QPS: requests per second (constant rate mode)
    ramp_interval: float = -1.0  # seconds between session launches (-1 = auto)

    # Duration-based mode (used with request_rate)
    duration_s: float = 0.0  # seconds to run benchmark (0 = use num_sessions)

    # Cross-sharing: fraction of system prompt shared across all sessions
    # 1.0 = identical system prompt, 0.0 = all unique
    cross_sharing: float = 0.0  # f

    # Simple mode inputs (derive u, s)
    isl: Optional[int] = None
    hit_rate: Optional[float] = None

    # Resolved values (computed by resolve())
    _u: int = field(default=0, init=False, repr=False)
    _s: int = field(default=0, init=False, repr=False)

    def resolve(self) -> "WorkloadSpec":
        """Resolve the spec: derive u and s from inputs. Call after init."""
        if self.isl is None or self.hit_rate is None:
            raise ValueError("Simple mode requires both --isl and --hit-rate.")
        self._derive_from_simple()
        self._validate()
        return self

    def _derive_from_simple(self) -> None:
        """Solve for u and s from (ISL, hit_rate, num_turns, OSL, cross_sharing)."""
        isl = self.isl
        h = self.hit_rate
        n = self.num_turns
        o = self.osl
        f = self.cross_sharing

        if n == 1:
            # Single turn: hit rate comes entirely from cross-session system prompt sharing
            u = isl * (1 - h)
            s = isl - u
        elif f == 1.0:
            # Full cross-sharing: turn 1 caches all of s
            u = isl * (1 - h)
            s = isl - u * (n + 1) / 2 - o * (n - 1) / 2
        else:
            # General case: f < 1
            denom = (n + 1) / 2 - n / (1 - f)
            if abs(denom) < 1e-9:
                raise ValueError(
                    f"Degenerate parameter combination: N_t={n}, f={f}. Cannot solve for u."
                )
            numer = isl - o * (n - 1) / 2 - n * isl * (1 - h) / (1 - f)
            u = numer / denom
            s = n * (isl * (1 - h) - u) / (1 - f)

        self._u = max(1, int(round(u)))
        self._s = max(0, int(round(s)))

    def _validate(self) -> None:
        """Validate resolved parameters."""
        if self._u < 1:
            raise ValueError(
                f"Derived user_tokens (u) = {self._u} < 1. "
                f"The (ISL={self.isl}, h={self.hit_rate}, N_t={self.num_turns}, "
                f"OSL={self.osl}, f={self.cross_sharing}) combination is infeasible. "
                f"Try increasing ISL, decreasing h, or increasing f."
            )
        if self._s < 0:
            raise ValueError(
                f"Derived system_prompt_tokens (s) = {self._s} < 0. "
                f"The (ISL={self.isl}, h={self.hit_rate}, N_t={self.num_turns}, "
                f"OSL={self.osl}, f={self.cross_sharing}) combination is infeasible. "
                f"Try increasing ISL, increasing h, or decreasing N_t."
            )
        if self.num_turns < 1:
            raise ValueError("num_turns must be >= 1.")
        if self.osl < 1:
            raise ValueError("osl must be >= 1.")
        if self.num_sessions is not None and self.num_sessions < 1:
            raise ValueError("num_sessions must be >= 1.")
        if self.num_sessions is None and self.duration_s <= 0:
            raise ValueError(
                "Must specify either --num-sessions or --duration (> 0) for rate-based mode."
            )
        if not (0 <= self.cross_sharing <= 1):
            raise ValueError("cross_sharing must be in [0, 1].")
        if self.think_time < 0:
            raise ValueError("think_time must be >= 0.")

        # Validate traffic control: either concurrency or request_rate
        if self.concurrency is None and self.request_rate is None:
            raise ValueError("Must specify either --concurrency or --request-rate.")
        if self.concurrency is not None and self.request_rate is not None:
            raise ValueError("Cannot specify both --concurrency and --request-rate.")
        if self.concurrency is not None and self.concurrency < 1:
            raise ValueError("concurrency must be >= 1.")
        if self.request_rate is not None and self.request_rate <= 0:
            raise ValueError("request_rate must be > 0.")

        # Auto-compute ramp_interval for smooth traffic (closed-loop mode only)
        if self.ramp_interval < 0:
            if self.concurrency is not None:
                if self.think_time > 0:
                    self.ramp_interval = self.think_time / self.concurrency
                else:
                    self.ramp_interval = 0.0
            else:
                # Rate-based mode doesn't use ramp_interval
                self.ramp_interval = 0.0

        # Warn if num_sessions may be too low (closed-loop concurrency mode only)
        if (
            self.concurrency is not None
            and self.think_time > 0
            and self.num_sessions is not None
            and self.num_sessions < self.concurrency * 2
        ):
            logger.warning(
                "num_sessions=%d may be too low to sustain concurrency=%d "
                "with think_time=%.1f. Consider increasing num_sessions.",
                self.num_sessions,
                self.concurrency,
                self.think_time,
            )

    @property
    def u(self) -> int:
        """New user tokens per turn."""
        return self._u

    @property
    def s(self) -> int:
        """Total system prompt tokens."""
        return self._s

    @property
    def shared_s(self) -> int:
        """Shared portion of system prompt (same across all sessions)."""
        return int(round(self._s * self.cross_sharing))

    @property
    def unique_s(self) -> int:
        """Unique portion of system prompt (different per session)."""
        return self._s - self.shared_s

    def turn_input_tokens(self, k: int) -> int:
        """Total input tokens at turn k (1-indexed)."""
        return self._s + k * self._u + (k - 1) * self.osl

    @property
    def effective_isl(self) -> float:
        """Average input sequence length across all turns."""
        n = self.num_turns
        return self._s + self._u * (n + 1) / 2 + self.osl * (n - 1) / 2

    @property
    def effective_h(self) -> float:
        """Token-weighted average hit rate."""
        f = self.cross_sharing
        n = self.num_turns
        avg_new = (1 - f) * self._s / n + self._u
        isl = self.effective_isl
        return 1.0 - avg_new / isl if isl > 0 else 0.0

    def summary(self) -> dict:
        """Return a summary dict of all resolved parameters."""
        per_turn = []
        for k in range(1, self.num_turns + 1):
            total = self.turn_input_tokens(k)
            if k == 1:
                cached = int(round(self._s * self.cross_sharing))
            else:
                cached = self._s + (k - 1) * self._u + (k - 1) * self.osl
            new = total - cached
            h_k = cached / total if total > 0 else 0.0
            per_turn.append(
                {
                    "turn": k,
                    "total": total,
                    "cached": cached,
                    "new": new,
                    "hit_rate": round(h_k, 4),
                }
            )

        return {
            "num_sessions": self.num_sessions,
            "duration_s": self.duration_s,
            "num_turns": self.num_turns,
            "osl": self.osl,
            "think_time": self.think_time,
            "concurrency": self.concurrency,
            "request_rate": self.request_rate,
            "cross_sharing": self.cross_sharing,
            "user_tokens_per_turn": self._u,
            "system_prompt_tokens": self._s,
            "shared_system_prompt": self.shared_s,
            "unique_system_prompt": self.unique_s,
            "effective_isl": round(self.effective_isl, 1),
            "effective_hit_rate": round(self.effective_h, 4),
            "per_turn": per_turn,
        }

    def print_summary(self) -> None:
        """Print a human-readable summary."""
        s = self.summary()
        print("=" * 70)
        print("Workload Spec (resolved)")
        print("=" * 70)
        if s["num_sessions"] is not None:
            print(f"  Sessions (N_s):           {s['num_sessions']}")
        else:
            print("  Sessions (N_s):           unlimited (duration-based)")
        if s["duration_s"] > 0:
            print(f"  Duration:                 {s['duration_s']}s")
        print(f"  Turns per session (N_t):  {s['num_turns']}")
        print(f"  User tokens/turn (u):     {s['user_tokens_per_turn']}")
        print(
            f"  System prompt (s):        {s['system_prompt_tokens']}  "
            f"(shared={s['shared_system_prompt']}, unique={s['unique_system_prompt']})"
        )
        print(f"  Output tokens (o):        {s['osl']}")
        print(f"  Think time:               {s['think_time']}s")
        if self.concurrency is not None:
            print(f"  Concurrency (C):          {self.concurrency}")
            print(f"  Ramp interval:            {self.ramp_interval:.3f}s")
        if self.request_rate is not None:
            print(f"  Request rate (QPS):       {self.request_rate}")
        print(f"  Cross-sharing (f):        {s['cross_sharing']}")
        print(f"  Effective avg ISL:        {s['effective_isl']}")
        print(f"  Effective avg hit rate:   {s['effective_hit_rate']:.1%}")
        print("-" * 70)
        print(f"  {'Turn':<6} {'Total':<8} {'Cached':<8} {'New':<8} {'Hit Rate':<10}")
        for t in s["per_turn"]:
            print(
                f"  {t['turn']:<6} {t['total']:<8} {t['cached']:<8} "
                f"{t['new']:<8} {t['hit_rate']:.1%}"
            )
        print("=" * 70)


# ============================================================================
# HTTP CLIENT
# ============================================================================


async def send_chat_completion(
    session: aiohttp.ClientSession,
    base_url: str,
    model: str,
    messages: list[dict[str, str]],
    session_id: str = "",
    max_tokens: int = 256,
    first_chunk_threshold: int = 16,
    timeout_sec: int = 300,
    api_key: Optional[str] = None,
) -> TurnResult:
    """Send a streaming chat completion request and collect metrics."""
    url = f"{base_url}/v1/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "stream": True,
        "stream_options": {"include_usage": True},
        "temperature": 0.0,
    }

    headers = {
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if session_id:
        headers["X-Session-Id"] = session_id

    timeout = aiohttp.ClientTimeout(total=timeout_sec)

    start_ns = time.perf_counter_ns()
    ttft_ns: Optional[int] = None
    fc_ns: Optional[int] = None
    content_chunk_count = 0
    chunk_times: list[int] = []
    generated_text = ""
    input_tokens = 0
    output_tokens = 0
    prev_ts = start_ns

    async with session.post(
        url, json=payload, headers=headers, timeout=timeout
    ) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {body[:500]}")

        async for raw_line in resp.content:
            line = raw_line.strip()
            if not line:
                continue
            text = line.decode("utf-8", errors="replace")
            if not text.startswith("data: "):
                continue
            data_str = text[6:]
            if data_str == "[DONE]":
                continue

            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            # Check for usage in the chunk
            usage = data.get("usage")
            if usage:
                input_tokens = usage.get("prompt_tokens", input_tokens)
                output_tokens = usage.get("completion_tokens", output_tokens)

            choices = data.get("choices", [])
            if not choices:
                continue

            delta = choices[0].get("delta", {})
            content = delta.get("content") or delta.get("reasoning")
            if content:
                now_ns = time.perf_counter_ns()
                content_chunk_count += 1
                if ttft_ns is None:
                    ttft_ns = now_ns - start_ns
                else:
                    chunk_times.append(now_ns - prev_ts)
                if fc_ns is None and content_chunk_count >= first_chunk_threshold:
                    fc_ns = now_ns - start_ns
                prev_ts = now_ns
                generated_text += content

    end_ns = time.perf_counter_ns()
    latency_ns = end_ns - start_ns

    if ttft_ns is None:
        ttft_ns = latency_ns

    if fc_ns is None:
        fc_ns = latency_ns

    tpot_ns = mean(chunk_times) if chunk_times else 0.0

    return TurnResult(
        ttft_ms=ttft_ns / 1e6,
        fc_ms=fc_ns / 1e6,
        tpot_ms=tpot_ns / 1e6,
        latency_ms=latency_ns / 1e6,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        generated_text=generated_text,
    )


# ============================================================================
# CONVERSATION & TEXT GENERATION
# ============================================================================


class Conversation:
    """A single multi-turn conversation with a unique session ID."""

    def __init__(
        self,
        session_id: str,
        system_prompt: str,
        user_messages: list[str],
        num_turns: int,
    ):
        self.session_id = session_id
        self.system_prompt = system_prompt
        self.user_messages = user_messages
        self.num_turns = num_turns
        self._assistant_responses: list[str] = []

    def get_turn_messages(self, turn_idx: int) -> list[dict[str, str]]:
        """Build the messages list for turn `turn_idx` (0-indexed).

        Turn 0: [system, user_0]
        Turn 1: [system, user_0, assistant_0, user_1]
        Turn k: [system, user_0, asst_0, ..., user_k]
        """
        messages = []
        if self.system_prompt:
            messages.append({"role": "system", "content": self.system_prompt})

        for i in range(turn_idx + 1):
            messages.append({"role": "user", "content": self.user_messages[i]})
            if i < turn_idx:
                if i < len(self._assistant_responses):
                    messages.append(
                        {
                            "role": "assistant",
                            "content": self._assistant_responses[i],
                        }
                    )
                else:
                    # Shouldn't happen if turns are sequential
                    messages.append(
                        {
                            "role": "assistant",
                            "content": "(placeholder)",
                        }
                    )
        return messages

    def inject_assistant_response(self, turn_idx: int, content: str) -> None:
        """Record the server's response for turn `turn_idx`."""
        if turn_idx == len(self._assistant_responses):
            self._assistant_responses.append(content)
        elif turn_idx < len(self._assistant_responses):
            self._assistant_responses[turn_idx] = content
        else:
            raise ValueError(
                f"Cannot inject response for turn {turn_idx}: "
                f"only {len(self._assistant_responses)} responses recorded."
            )


class TextGenerator:
    """Generates random text with exact token counts using a tokenizer.

    Produces random token IDs from the tokenizer's vocabulary and decodes
    them to text, then verifies the round-trip token count matches the target.
    """

    def __init__(self, tokenizer: "PreTrainedTokenizerBase"):
        self._tokenizer = tokenizer
        self._vocab_size = tokenizer.vocab_size
        logger.info(
            "TextGenerator using tokenizer (vocab_size=%d) for exact token counts.",
            self._vocab_size,
        )

    def generate(self, num_tokens: int) -> str:
        """Generate text that tokenizes to exactly `num_tokens` tokens."""
        if num_tokens <= 0:
            return ""
        return self._generate_exact(num_tokens)

    def generate_token_ids(self, num_tokens: int) -> list[int]:
        """Generate exactly `num_tokens` random token IDs."""
        if num_tokens <= 0:
            return []
        return np.random.randint(0, self._vocab_size, size=num_tokens).tolist()

    def _generate_exact(self, target_tokens: int) -> str:
        """Generate text that round-trips to exactly `target_tokens` tokens."""
        tokenizer = self._tokenizer

        # Generate random token IDs, decode, re-encode, adjust until exact match
        token_ids = np.random.randint(
            0, self._vocab_size, size=target_tokens + 20
        ).tolist()

        text = tokenizer.decode(token_ids, skip_special_tokens=True)
        actual_ids = tokenizer.encode(text, add_special_tokens=False)
        actual_len = len(actual_ids)

        if actual_len == target_tokens:
            return text

        if actual_len > target_tokens:
            # Trim tokens from the end
            trimmed_ids = actual_ids[:target_tokens]
            text = tokenizer.decode(trimmed_ids, skip_special_tokens=True)
            final_len = len(tokenizer.encode(text, add_special_tokens=False))
            if final_len != target_tokens:
                text = self._binary_search_trim(actual_ids, target_tokens)
            return text

        # actual_len < target_tokens: generate more and append
        deficit = target_tokens - actual_len
        extra_ids = np.random.randint(0, self._vocab_size, size=deficit + 20).tolist()
        extra_text = tokenizer.decode(extra_ids, skip_special_tokens=True)
        combined = text + " " + extra_text
        combined_ids = tokenizer.encode(combined, add_special_tokens=False)

        if len(combined_ids) >= target_tokens:
            trimmed = combined_ids[:target_tokens]
            text = tokenizer.decode(trimmed, skip_special_tokens=True)
            final_len = len(tokenizer.encode(text, add_special_tokens=False))
            if final_len != target_tokens:
                text = self._binary_search_trim(combined_ids, target_tokens)
            return text

        # Very unlikely: still not enough. Pad with simple words.
        while len(tokenizer.encode(combined, add_special_tokens=False)) < target_tokens:
            combined += " hello"
        combined_ids = tokenizer.encode(combined, add_special_tokens=False)
        return self._binary_search_trim(combined_ids, target_tokens)

    def _binary_search_trim(self, token_ids: list[int], target: int) -> str:
        """Find the right number of token IDs to decode to get exactly `target` tokens."""
        tokenizer = self._tokenizer
        lo, hi = target, len(token_ids)

        best_text = tokenizer.decode(token_ids[:target], skip_special_tokens=True)

        while lo <= hi:
            mid = (lo + hi) // 2
            text = tokenizer.decode(token_ids[:mid], skip_special_tokens=True)
            actual = len(tokenizer.encode(text, add_special_tokens=False))
            if actual == target:
                return text
            elif actual < target:
                lo = mid + 1
            else:
                hi = mid - 1
                best_text = text

        # BPE merges can prevent the binary search from finding an exact match.
        # Scan prefix lengths from `target` upward (shortest plausible first);
        # the loop above already explored midpoints, so this covers edge cases.
        for n in range(target, len(token_ids) + 1):
            text = tokenizer.decode(token_ids[:n], skip_special_tokens=True)
            if len(tokenizer.encode(text, add_special_tokens=False)) == target:
                return text
        return best_text


def conversation_factory(
    session_idx: int,
    spec: WorkloadSpec,
    shared_system_text: str,
    text_gen: TextGenerator,
) -> Conversation:
    """Create a single conversation on-demand (lazy generation)."""
    session_id = f"session-{session_idx:06d}"

    # Build system prompt: shared prefix + unique suffix
    if spec.unique_s > 0:
        unique_text = text_gen.generate(spec.unique_s)
        system_prompt = shared_system_text + " " + unique_text
    else:
        system_prompt = shared_system_text

    # Generate all user messages
    user_messages = [text_gen.generate(spec.u) for _ in range(spec.num_turns)]

    return Conversation(
        session_id=session_id,
        system_prompt=system_prompt,
        user_messages=user_messages,
        num_turns=spec.num_turns,
    )


# ============================================================================
# BENCHMARK ENGINE
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
                # Add small staggered delay for first few sessions
                delay = 0.0 if session_idx >= spec.concurrency else session_idx * 0.1
                await turn_queue.put((session_idx, 0, time.perf_counter() + delay))
                return True
            return False

        # Initialize: fill queue with just enough sessions to saturate concurrency
        for _ in range(min(spec.concurrency, spec.num_sessions)):
            await enqueue_new_session()

        async def execute_turn(session_idx: int, turn_idx: int) -> None:
            """Execute a single turn for a session."""
            # Get or create conversation
            conv, is_warmup = await state.get_or_create_conversation(
                session_idx, spec, shared_system_text, text_gen
            )

            # Mark session as active at this turn
            await state.mark_session_active(conv.session_id, turn_idx)

            messages = conv.get_turn_messages(turn_idx)

            async with request_sem:
                # Track concurrency
                await state.track_inflight_start()

                req_start_ns = time.perf_counter_ns()
                try:
                    result = await send_chat_completion(
                        session=http_session,
                        base_url=base_url,
                        model=model,
                        messages=messages,
                        session_id=conv.session_id,
                        max_tokens=spec.osl,
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
                    # Clean up failed session
                    await state.mark_session_complete(session_idx, conv.session_id)
                    return

                current_inflight = await state.track_inflight_end()

            # Record metric
            metric = TurnMetric(
                session_id=conv.session_id,
                turn=turn_idx,
                ttft_ms=result.ttft_ms,
                fc_ms=result.fc_ms,
                tpot_ms=result.tpot_ms,
                latency_ms=result.latency_ms,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                start_time_ms=(req_start_ns - bench_start_ns) / 1e6,
            )

            await state.record_metric(metric)

            pbar.set_postfix_str(str(current_inflight))
            pbar.update(1)

            # Inject response for next turn — use generated_text directly
            conv.inject_assistant_response(turn_idx, result.generated_text)

            # Enqueue next turn or complete session
            if turn_idx < spec.num_turns - 1:
                next_turn = turn_idx + 1
                # During warm-up: add random jitter to desynchronize sessions
                # After warm-up: use configured think_time
                if is_warmup:
                    jitter = np.random.uniform(0.0, warmup_jitter_max_s)
                    delay_until = time.perf_counter() + jitter
                else:
                    delay_until = time.perf_counter() + spec.think_time
                await turn_queue.put((session_idx, next_turn, delay_until))
            else:
                # Session complete
                await state.mark_session_complete(session_idx, conv.session_id)
                # Enqueue a new session to fill this slot
                await enqueue_new_session()

            # Check for steady-state after each turn completes
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
# METRICS & REPORTING
# ============================================================================


def percentile(values: list[float], p: float) -> float:
    """Compute the p-th percentile (0-100)."""
    if not values:
        return 0.0
    return float(np.percentile(values, p))


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

        req_start_ns = time.perf_counter_ns()
        messages = conv.get_turn_messages(turn_idx)

        try:
            result = await send_chat_completion(
                session=http_session,
                base_url=base_url,
                model=model,
                messages=messages,
                session_id=conv.session_id,
                max_tokens=spec.osl,
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
            # On failure: push a fresh session back so the pacer has something ready
            if not stop_dispatching.is_set():
                await ready_queue.put((_next_conv(), 0))
            async with inflight_lock:
                if stop_dispatching.is_set() and inflight == 0:
                    all_done.set()
            return

        async with inflight_lock:
            inflight -= 1
            pbar_done.set_postfix_str(f"inflight={inflight}")

        metric = TurnMetric(
            session_id=conv.session_id,
            turn=turn_idx,
            ttft_ms=result.ttft_ms,
            fc_ms=result.fc_ms,
            tpot_ms=result.tpot_ms,
            latency_ms=result.latency_ms,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            start_time_ms=(req_start_ns - bench_start_ns) / 1e6,
        )
        async with metrics_lock:
            if req_start_ns >= warmup_cutoff_ns:
                metrics.append(metric)
            else:
                warmup_metrics.append(metric)
        pbar_done.update(1)

        # Inject response -- use generated_text directly
        conv.inject_assistant_response(turn_idx, result.generated_text)

        if not stop_dispatching.is_set():
            next_turn = turn_idx + 1
            if next_turn < spec.num_turns:
                # Turn-order guarantee: enqueue turn k+1 only now
                await ready_queue.put((conv, next_turn))
            else:
                # Session done all turns -- push a fresh turn-0 session for reuse
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


def report_results(
    metrics: list[TurnMetric],
    spec: WorkloadSpec,
    bench_elapsed_s: float,
    first_chunk_threshold: int = 16,
    save_path: Optional[str] = None,
    warmup_s: float = 0.0,
    discarded_warmup_requests: int = 0,
) -> None:
    """Print and optionally save benchmark results."""
    if not metrics:
        print("No metrics collected.")
        return

    # Aggregate stats
    all_ttft = [m.ttft_ms for m in metrics]
    all_fc = [m.fc_ms for m in metrics]
    all_tpot = [m.tpot_ms for m in metrics if m.tpot_ms > 0]
    all_latency = [m.latency_ms for m in metrics]
    all_input = [m.input_tokens for m in metrics]
    all_output = [m.output_tokens for m in metrics]

    total_output_tokens = sum(all_output)
    throughput = total_output_tokens / bench_elapsed_s if bench_elapsed_s > 0 else 0

    print()
    print("=" * 70)
    print("BENCHMARK RESULTS")
    print("=" * 70)
    print(f"  Total requests:       {len(metrics)}")
    print(f"  Unique sessions:      {len({m.session_id for m in metrics})}")
    print(f"  Duration:             {bench_elapsed_s:.1f}s")
    if warmup_s > 0:
        print(f"  Warm-up excluded:     {warmup_s:.1f}s")
    if discarded_warmup_requests > 0:
        print(f"  Warm-up requests:     {discarded_warmup_requests} (discarded)")
    print(f"  Throughput:           {throughput:.1f} output tok/s")
    print(f"  Request rate:         {len(metrics) / bench_elapsed_s:.1f} req/s")
    print(
        f"  Avg input tokens:     {mean(all_input):.0f}  "
        f"(target ISL: {spec.effective_isl:.0f})"
    )
    print(f"  Avg output tokens:    {mean(all_output):.0f}  (target OSL: {spec.osl})")
    print()

    # Latency stats
    fc_label = f"FC({first_chunk_threshold})"
    print("  Latency Statistics:")
    for name, values in [
        ("TTFT", all_ttft),
        (fc_label, all_fc),
        ("TPOT", all_tpot),
        ("Latency", all_latency),
    ]:
        if not values:
            continue
        print(
            f"    {name:>8}:  avg={mean(values):>8.1f}ms  "
            f"P50={percentile(values, 50):>8.1f}ms  "
            f"P90={percentile(values, 90):>8.1f}ms  "
            f"P99={percentile(values, 99):>8.1f}ms"
        )
    print()

    # Per-turn breakdown
    print("  Per-Turn Breakdown:")
    print(
        f"    {'Turn':<6} {'Count':<7} {'Avg ISL':<9} {'Avg TTFT':<10} "
        f"{'Avg FC':<10} {'Avg TPOT':<10} {'Avg Lat':<10}"
    )

    for t in range(spec.num_turns):
        turn_metrics = [m for m in metrics if m.turn == t]
        if not turn_metrics:
            continue
        t_ttft = mean([m.ttft_ms for m in turn_metrics])
        t_fc = mean([m.fc_ms for m in turn_metrics])
        t_tpot_vals = [m.tpot_ms for m in turn_metrics if m.tpot_ms > 0]
        t_tpot = mean(t_tpot_vals) if t_tpot_vals else 0.0
        t_lat = mean([m.latency_ms for m in turn_metrics])
        t_isl = mean([m.input_tokens for m in turn_metrics])
        print(
            f"    {t + 1:<6} {len(turn_metrics):<7} {t_isl:<9.0f} "
            f"{t_ttft:<10.1f} {t_fc:<10.1f} {t_tpot:<10.1f} {t_lat:<10.1f}"
        )
    print("=" * 70)

    # Save results
    if save_path:
        result = {
            "config": {
                "concurrency": spec.concurrency,
                "request_rate": spec.request_rate,
            },
            "spec": spec.summary(),
            "first_chunk_threshold": first_chunk_threshold,
            "benchmark": {
                "total_requests": len(metrics),
                "duration_s": round(bench_elapsed_s, 2),
                "warmup_s": round(warmup_s, 2),
                "discarded_warmup_requests": discarded_warmup_requests,
            },
            "stats": {
                "throughput_tok_s": round(throughput, 1),
                "measured_request_rate": round(len(metrics) / bench_elapsed_s, 2),
                "avg_input_tokens": round(mean(all_input), 1),
                "avg_output_tokens": round(mean(all_output), 1),
                "avg_ttft_ms": round(mean(all_ttft), 2),
                "p50_ttft_ms": round(percentile(all_ttft, 50), 2),
                "p90_ttft_ms": round(percentile(all_ttft, 90), 2),
                "p99_ttft_ms": round(percentile(all_ttft, 99), 2),
                "avg_fc_ms": round(mean(all_fc), 2),
                "p50_fc_ms": round(percentile(all_fc, 50), 2),
                "p90_fc_ms": round(percentile(all_fc, 90), 2),
                "p99_fc_ms": round(percentile(all_fc, 99), 2),
                "avg_tpot_ms": round(mean(all_tpot), 2) if all_tpot else 0,
                "p50_tpot_ms": (round(percentile(all_tpot, 50), 2) if all_tpot else 0),
                "p90_tpot_ms": (round(percentile(all_tpot, 90), 2) if all_tpot else 0),
                "p99_tpot_ms": (round(percentile(all_tpot, 99), 2) if all_tpot else 0),
                "avg_latency_ms": round(mean(all_latency), 2),
            },
            "per_turn": [],
            "raw_metrics": [
                {
                    "session_id": m.session_id,
                    "turn": m.turn,
                    "ttft_ms": round(m.ttft_ms, 2),
                    "fc_ms": round(m.fc_ms, 2),
                    "tpot_ms": round(m.tpot_ms, 2),
                    "latency_ms": round(m.latency_ms, 2),
                    "input_tokens": m.input_tokens,
                    "output_tokens": m.output_tokens,
                    "start_time_ms": round(m.start_time_ms, 2),
                }
                for m in metrics
            ],
        }

        # Per-turn summary
        for t in range(spec.num_turns):
            turn_metrics = [m for m in metrics if m.turn == t]
            if not turn_metrics:
                continue
            t_ttft = [m.ttft_ms for m in turn_metrics]
            t_fc = [m.fc_ms for m in turn_metrics]
            t_tpot = [m.tpot_ms for m in turn_metrics if m.tpot_ms > 0]
            t_isl = [m.input_tokens for m in turn_metrics]
            result["per_turn"].append(
                {
                    "turn": t + 1,
                    "count": len(turn_metrics),
                    "avg_isl": round(mean(t_isl), 1),
                    "avg_ttft_ms": round(mean(t_ttft), 2),
                    "avg_fc_ms": round(mean(t_fc), 2),
                    "avg_tpot_ms": round(mean(t_tpot), 2) if t_tpot else 0,
                    "p50_fc_ms": round(percentile(t_fc, 50), 2),
                    "p99_ttft_ms": round(percentile(t_ttft, 99), 2),
                    "p99_fc_ms": round(percentile(t_fc, 99), 2),
                    "p99_tpot_ms": (round(percentile(t_tpot, 99), 2) if t_tpot else 0),
                }
            )

        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w") as f:
            json.dump(result, f, indent=2)
        logger.info("Results saved to %s", save_path)


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
    spec = WorkloadSpec(
        num_sessions=args.num_sessions,
        num_turns=args.num_turns,
        osl=args.osl,
        think_time=args.think_time,
        concurrency=args.concurrency,
        request_rate=args.request_rate,
        ramp_interval=args.ramp_interval,
        cross_sharing=args.cross_sharing,
        isl=args.isl,
        hit_rate=args.hit_rate,
        duration_s=args.duration or 0.0,
    )
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
        elif spec.num_sessions is not None:
            effective_duration = spec.num_sessions * spec.num_turns / spec.request_rate
        else:
            effective_duration = 60.0
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
