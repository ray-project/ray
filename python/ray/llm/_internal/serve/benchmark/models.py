"""Data models for the multi-turn benchmark."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TurnResult:
    """Result of a single turn's HTTP request."""

    ttft_ms: float  # time to first token
    fc_ms: float  # first-chunk latency (time to N-th content chunk)
    itl_ms: float  # mean inter-token latency across output tokens
    latency_ms: float  # total request latency
    input_tokens: int  # reported by server (usage.prompt_tokens)
    output_tokens: int  # reported by server (usage.completion_tokens)
    generated_text: str  # generated text
    itl_ms_list: List[float] = field(default_factory=list)  # per-token ITL values


@dataclass
class TurnMetric:
    """Metrics for a single turn."""

    session_id: str
    turn: int  # 0-indexed
    ttft_ms: float
    fc_ms: float  # first-chunk latency
    itl_ms: float  # mean inter-token latency
    latency_ms: float
    input_tokens: int
    output_tokens: int
    start_time_ms: float  # relative to benchmark start
    itl_ms_list: List[float] = field(default_factory=list)  # per-token ITL values


@dataclass
class WorkloadSpec:
    """Workload specification for multi-turn session benchmarks.

    Supports simple mode: specify isl + hit_rate, derive user_tokens and sys_tokens.
    All parameters are scalar (fixed) values -- no distributions.
    """

    # Core parameters
    num_sessions: Optional[int] = None  # total unique sessions (None = duration-based)
    num_turns: int = 1  # turns per session
    osl: int = 1  # output sequence length per turn
    think_time: float = 0.0  # seconds between turns within a session

    # Traffic (use either concurrency or request_rate, not both)
    concurrency: Optional[int] = None  # max concurrent in-flight requests
    request_rate: Optional[float] = None  # requests per second (constant rate mode)
    ramp_interval: float = -1.0  # seconds between session launches (-1 = auto)

    # Duration-based mode (used with request_rate)
    duration_s: float = 0.0  # seconds to run benchmark (0 = use num_sessions)

    # Fraction of system prompt shared across all sessions
    # 1.0 = identical system prompt, 0.0 = all unique
    shared_system_prompt_ratio: float = 1.0

    # Simple mode inputs (derive user_tokens, sys_tokens)
    isl: Optional[int] = None
    hit_rate: Optional[float] = None

    # Resolved values (computed by resolve())
    _user_tokens: int = field(default=0, init=False, repr=False)
    _sys_tokens: int = field(default=0, init=False, repr=False)

    def resolve(self) -> "WorkloadSpec":
        """Resolve the spec: derive user_tokens and sys_tokens from inputs. Call after init."""
        if self.isl is None or self.hit_rate is None:
            raise ValueError("Simple mode requires both --isl and --hit-rate.")
        self._validate()
        self._derive_from_simple()
        return self

    def _derive_from_simple(self) -> None:
        """Derive user_tokens and sys_tokens from (ISL, hit_rate, num_turns, OSL, shared_system_prompt_ratio).

        Two equations, two unknowns (u = user_tokens, s = sys_tokens):

          (1) ISL  = s + (n+1)/2 · u + (n-1)/2 · a          [average input length]
          (2) (1-h)·ISL = (1-f)·s/n + u                      [average new-token fraction]

        where n = num_turns, a = osl, f = shared_system_prompt_ratio, h = hit_rate.

        Substituting s from (1) into (2) and solving for u:

          u = [ (1-h)·ISL - (1-f)/n · (ISL - (n-1)·a/2) ]
              / [ 1 - (1-f)·(n+1)/(2n) ]

        Then s = ISL - (n+1)/2 · u - (n-1)/2 · a.

        Special case: when n=1 and f=0, equations (1) and (2) collapse to
        s + u = ISL with h = s/(s+u), giving s = h·ISL and u = (1-h)·ISL.
        """
        isl = self.isl
        h = self.hit_rate
        n = self.num_turns
        a = self.osl
        f = self.shared_system_prompt_ratio

        denom = 1 - (1 - f) * (n + 1) / (2 * n)
        if abs(denom) < 1e-9:
            # n=1, f=0, h=0 (validated earlier): s=0, u=ISL.
            sys_tokens = 0.0
            user_tokens = float(isl)
        else:
            numer = (1 - h) * isl - (1 - f) / n * (isl - (n - 1) * a / 2)
            user_tokens = numer / denom
            sys_tokens = isl - (n + 1) / 2 * user_tokens - (n - 1) / 2 * a

        if user_tokens < 0.5 or sys_tokens < -0.5:
            suggestions = self._feasibility_suggestions()
            which = "user_tokens" if user_tokens < 0.5 else "sys_tokens"
            val = user_tokens if user_tokens < 0.5 else sys_tokens
            raise ValueError(
                f"Derived {which} = {val:.1f} is infeasible with "
                f"(ISL={isl}, hit_rate={h}, num_turns={n}, "
                f"OSL={a}, shared_system_prompt_ratio={f}).\n"
                f"To fix, try one of:\n{suggestions}"
            )

        self._user_tokens = max(1, int(round(user_tokens)))
        self._sys_tokens = max(0, int(round(sys_tokens)))

    def _feasibility_suggestions(self) -> str:
        """Compute feasible boundary values for each parameter and return suggestions."""
        isl, h, n, a, f = (
            self.isl,
            self.hit_rate,
            self.num_turns,
            self.osl,
            self.shared_system_prompt_ratio,
        )
        lines = []

        def _try_solve(isl_, h_, n_, a_, f_):
            """Return (u, s) or None if degenerate."""
            d = 1 - (1 - f_) * (n_ + 1) / (2 * n_)
            if abs(d) < 1e-9:
                if h_ > 1e-9:
                    return None
                return (float(isl_), 0.0)
            num = (1 - h_) * isl_ - (1 - f_) / n_ * (isl_ - (n_ - 1) * a_ / 2)
            u = num / d
            s = isl_ - (n_ + 1) / 2 * u - (n_ - 1) / 2 * a_
            return (u, s)

        def _feasible(isl_, h_, n_, a_, f_):
            r = _try_solve(isl_, h_, n_, a_, f_)
            return r is not None and r[0] >= 0.5 and r[1] >= -0.5

        # Min ISL (binary search)
        lo, hi = isl, isl * 20
        if _feasible(hi, h, n, a, f):
            while hi - lo > 1:
                mid = (lo + hi) // 2
                if _feasible(mid, h, n, a, f):
                    hi = mid
                else:
                    lo = mid
            lines.append(f"  - ISL >= {hi} (with current params)")

        # Max OSL
        lo, hi = 1, a
        if _feasible(isl, h, n, lo, f):
            while hi - lo > 1:
                mid = (lo + hi) // 2
                if _feasible(isl, h, n, mid, f):
                    lo = mid
                else:
                    hi = mid
            lines.append(f"  - OSL <= {lo} (with current ISL={isl})")

        # Min hit_rate / max hit_rate (search in 0.01 steps)
        for h_try in range(0, 100):
            h_val = h_try / 100.0
            if _feasible(isl, h_val, n, a, f):
                if h_val != h:
                    if h_val > h:
                        lines.append(
                            f"  - hit_rate >= {h_val:.2f} (with current ISL/OSL)"
                        )
                    else:
                        lines.append(
                            f"  - hit_rate <= {h_val:.2f} (with current ISL/OSL)"
                        )
                break

        # Max num_turns
        for n_try in range(n, 0, -1):
            if _feasible(isl, h, n_try, a, f):
                if n_try != n:
                    lines.append(f"  - num_turns <= {n_try} (with current ISL/OSL)")
                break

        # Min shared_system_prompt_ratio
        if f < 1.0:
            for f_try in range(int(f * 100), 101):
                f_val = f_try / 100.0
                if _feasible(isl, h, n, a, f_val):
                    if f_val != f:
                        lines.append(f"  - shared_system_prompt_ratio >= {f_val:.2f}")
                    break

        return "\n".join(lines) if lines else "  (no single-parameter fix found)"

    def _validate(self) -> None:
        """Validate resolved parameters."""
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
        if not (0 <= self.shared_system_prompt_ratio <= 1):
            raise ValueError("shared_system_prompt_ratio must be in [0, 1].")
        if self.think_time < 0:
            raise ValueError("think_time must be >= 0.")
        if (
            self.num_turns == 1
            and self.shared_system_prompt_ratio == 0
            and self.hit_rate is not None
            and self.hit_rate > 1e-9
        ):
            raise ValueError(
                f"Cannot achieve hit_rate={self.hit_rate} with num_turns=1 and "
                f"shared_system_prompt_ratio=0. There is no caching source "
                f"(no multi-turn history, no shared prefix). "
                f"Set shared_system_prompt_ratio > 0 to enable cross-session "
                f"prefix caching, or use num_turns > 1 for multi-turn caching."
            )

        if self.concurrency is None and self.request_rate is None:
            raise ValueError("Must specify either --concurrency or --request-rate.")
        if self.concurrency is not None and self.request_rate is not None:
            raise ValueError("Cannot specify both --concurrency and --request-rate.")
        if self.concurrency is not None and self.concurrency < 1:
            raise ValueError("concurrency must be >= 1.")
        if self.request_rate is not None and self.request_rate <= 0:
            raise ValueError("request_rate must be > 0.")

        if self.ramp_interval < 0:
            if self.concurrency is not None:
                if self.think_time > 0:
                    self.ramp_interval = self.think_time / self.concurrency
                else:
                    self.ramp_interval = 0.0
            else:
                self.ramp_interval = 0.0

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
    def user_tokens(self) -> int:
        return self._user_tokens

    @property
    def sys_tokens(self) -> int:
        return self._sys_tokens

    @property
    def shared_s(self) -> int:
        return int(round(self._sys_tokens * self.shared_system_prompt_ratio))

    @property
    def unique_s(self) -> int:
        return self._sys_tokens - self.shared_s

    def turn_input_tokens(self, k: int) -> int:
        """Total input tokens at turn k (1-indexed)."""
        return self._sys_tokens + k * self._user_tokens + (k - 1) * self.osl

    @property
    def effective_isl(self) -> float:
        n = self.num_turns
        return (
            self._sys_tokens + self._user_tokens * (n + 1) / 2 + self.osl * (n - 1) / 2
        )

    @property
    def effective_h(self) -> float:
        f = self.shared_system_prompt_ratio
        n = self.num_turns
        avg_new = (1 - f) * self._sys_tokens / n + self._user_tokens
        isl = self.effective_isl
        return 1.0 - avg_new / isl if isl > 0 else 0.0

    def summary(self) -> dict:
        per_turn = []
        for k in range(1, self.num_turns + 1):
            total = self.turn_input_tokens(k)
            if k == 1:
                cached = int(round(self._sys_tokens * self.shared_system_prompt_ratio))
            else:
                cached = (
                    self._sys_tokens + (k - 1) * self._user_tokens + (k - 1) * self.osl
                )
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
            "shared_system_prompt_ratio": self.shared_system_prompt_ratio,
            "user_tokens_per_turn": self._user_tokens,
            "system_prompt_tokens": self._sys_tokens,
            "shared_system_prompt": self.shared_s,
            "unique_system_prompt": self.unique_s,
            "effective_isl": round(self.effective_isl, 1),
            "effective_hit_rate": round(self.effective_h, 4),
            "per_turn": per_turn,
        }

    def print_summary(self) -> None:
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
        print(f"  Shared sys prompt ratio:  {s['shared_system_prompt_ratio']}")
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
