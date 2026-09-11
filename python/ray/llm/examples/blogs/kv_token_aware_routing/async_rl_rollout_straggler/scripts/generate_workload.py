#!/usr/bin/env python
"""Generate the long-context asynchronous RL-rollout cache/load workload.

The traffic is a single closed-loop pool of 80 independent, ten-turn rollouts.
Rollout 0 begins first, warming a small unseeded step-state prefix; rollout 4
begins later and therefore has a real cache-affinity reason to follow rollout 0.
Its terminal request then arrives while rollout 0 has an 8,192-token decode in progress.

KVAwareRouter can trade the warm step prefix for lower decode load.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

MODEL = "openai/gpt-oss-120b"
STEPS = 10  # 10 steps x 8 = 80 rollouts.
ROLLOUTS_PER_STEP = 8
TURNS_PER_ROLLOUT = 10
GLOBAL_SYSTEM_TOKENS = 2048
STEP_STATE_TOKENS = 512
UNIQUE_BRIEF_TOKENS = 1024
DEFAULT_INTERMEDIATE_OUTPUT = 128
DEFAULT_REGULAR_TERMINAL_OUTPUT = 256
DEFAULT_STRAGGLER_TERMINAL_OUTPUT = 8192
SEED_SESSIONS = 32


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("x") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def exact_token_text(tokenizer: Any, prefix: str, count: int) -> str:
    for candidate in (" shared", " rollout", " policy", " trajectory", " context"):
        token_ids = tokenizer.encode(candidate, add_special_tokens=False)
        if len(token_ids) != 1:
            continue
        current = len(tokenizer.encode(prefix, add_special_tokens=False))
        for n_tokens in range(max(0, count - current - 8), count + 9):
            text = prefix + tokenizer.decode(
                token_ids * n_tokens, skip_special_tokens=True
            )
            if len(tokenizer.encode(text, add_special_tokens=False)) == count:
                return text
    raise RuntimeError(f"could not construct exactly {count} content tokens")


def request_turn(
    messages: list[dict[str, str]],
    output_tokens: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "messages": messages,
        "max_tokens": output_tokens,
        "extra": {"ignore_eos": True, "min_tokens": output_tokens, "temperature": 0},
    }
    if timestamp_ms is not None:
        row["timestamp"] = timestamp_ms
    return row


def rollout_id(step: int, rollout: int) -> str:
    return f"rl-rollout-step-{step}-rollout-{rollout}"


def role(rollout: int) -> str:
    if rollout == 0:
        return "anchor_straggler"
    if rollout == 4:
        return "follower_straggler"
    return "regular"


def rollout_row(
    step: int,
    rollout: int,
    *,
    global_system: str,
    step_state: str,
    unique_brief: str,
    anchor_start_ms: int,
    follower_gap_ms: int,
    regular_start_lag_ms: int,
    intermediate_output: int,
    regular_terminal_output: int,
    straggler_terminal_output: int,
) -> dict[str, Any]:
    session_id = rollout_id(step, rollout)
    rollout_role = role(rollout)
    if rollout_role == "anchor_straggler":
        start_ms = anchor_start_ms
        terminal_output = straggler_terminal_output
    elif rollout_role == "follower_straggler":
        start_ms = anchor_start_ms + follower_gap_ms
        terminal_output = straggler_terminal_output
    else:
        start_ms = anchor_start_ms + regular_start_lag_ms
        terminal_output = regular_terminal_output

    turns = [
        request_turn(
            [
                {"role": "system", "content": global_system},
                {
                    "role": "user",
                    "content": (
                        step_state
                        + unique_brief
                        + f"Independent RL rollout {session_id}. Take the first action for this "
                        "shared simulated state. Use plain-language reasoning only; do not call "
                        "tools, inspect files, or emit code."
                    ),
                },
            ],
            intermediate_output,
            timestamp_ms=start_ms,
        )
    ]
    for turn_index in range(1, TURNS_PER_ROLLOUT - 1):
        turns.append(
            request_turn(
                [
                    {
                        "role": "user",
                        "content": (
                            f"Continue independent rollout {session_id} from its actual preceding "
                            f"responses. This is rollout turn {turn_index + 1}; evaluate the new "
                            "observation and return the next action in plain text only."
                        ),
                    }
                ],
                intermediate_output,
            )
        )
    turns.append(
        request_turn(
            [
                {
                    "role": "user",
                    "content": (
                        f"Finish independent rollout {session_id} using its actual preceding responses. "
                        "Return the terminal sampled trajectory, final action, and reward rationale in "
                        "plain text only."
                    ),
                }
            ],
            terminal_output,
        )
    )
    return {"session_id": session_id, "turns": turns}


def seed_row(index: int, global_system: str) -> dict[str, Any]:
    return {
        "session_id": f"rl-rollout-global-seed-{index:03d}",
        "turns": [
            request_turn(
                [
                    {"role": "system", "content": global_system},
                    {
                        "role": "user",
                        "content": f"Warm the shared RL policy prefix on cache shard {index:03d}. Reply plainly.",
                    },
                ],
                64,
            )
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--follower-gap-ms", type=int, default=2200)
    parser.add_argument("--step-stagger-ms", type=int, default=250)
    parser.add_argument("--regular-start-lag-ms", type=int, default=3200)
    parser.add_argument(
        "--intermediate-output-tokens", type=int, default=DEFAULT_INTERMEDIATE_OUTPUT
    )
    parser.add_argument(
        "--regular-terminal-output-tokens",
        type=int,
        default=DEFAULT_REGULAR_TERMINAL_OUTPUT,
    )
    parser.add_argument(
        "--straggler-terminal-output-tokens",
        type=int,
        default=DEFAULT_STRAGGLER_TERMINAL_OUTPUT,
    )
    args = parser.parse_args()
    if args.follower_gap_ms < 1800:
        raise SystemExit(
            "--follower-gap-ms must allow live decode-block publication (>=1800)"
        )
    if args.regular_start_lag_ms <= args.follower_gap_ms:
        raise SystemExit("--regular-start-lag-ms must follow rollout 4")
    if args.step_stagger_ms < 0:
        raise SystemExit("--step-stagger-ms must be non-negative")
    if (
        min(
            args.intermediate_output_tokens,
            args.regular_terminal_output_tokens,
            args.straggler_terminal_output_tokens,
        )
        <= 0
    ):
        raise SystemExit("all output-token counts must be positive")
    if args.straggler_terminal_output_tokens <= args.regular_terminal_output_tokens:
        raise SystemExit(
            "straggler terminal output must exceed the regular terminal output"
        )
    out = args.out.resolve()
    if out.exists():
        raise SystemExit(f"refusing to overwrite {out}")
    out.mkdir(parents=True)

    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(MODEL, trust_remote_code=True)
    global_system = exact_token_text(
        tokenizer,
        "You are a policy in a text-only reinforcement-learning environment. "
        "Respond only with plain-language actions and rationale. Do not call tools, inspect files, "
        "or emit code. Preserve simulated state across turns.\n",
        GLOBAL_SYSTEM_TOKENS,
    )
    step_states = [
        exact_token_text(
            tokenizer,
            f"Step {step:02d} shared environment state and policy constraints.\n",
            STEP_STATE_TOKENS,
        )
        for step in range(STEPS)
    ]
    briefs = [
        exact_token_text(
            tokenizer,
            f"Step {step:02d}, rollout {rollout:02d}: unique observation, sampled action seed, and reward context.\n",
            UNIQUE_BRIEF_TOKENS,
        )
        for step in range(STEPS)
        for rollout in range(ROLLOUTS_PER_STEP)
    ]

    rows: list[dict[str, Any]] = []
    output_tiers: Counter[int] = Counter()
    # Ordering has no admission meaning under fixed-schedule replay; retaining
    # each step together makes the serialized workload auditable.
    brief_index = 0
    for step in range(STEPS):
        for rollout in range(ROLLOUTS_PER_STEP):
            rows.append(
                rollout_row(
                    step,
                    rollout,
                    global_system=global_system,
                    step_state=step_states[step],
                    unique_brief=briefs[brief_index],
                    anchor_start_ms=step * args.step_stagger_ms,
                    follower_gap_ms=args.follower_gap_ms,
                    regular_start_lag_ms=args.regular_start_lag_ms,
                    intermediate_output=args.intermediate_output_tokens,
                    regular_terminal_output=args.regular_terminal_output_tokens,
                    straggler_terminal_output=args.straggler_terminal_output_tokens,
                )
            )
            brief_index += 1
            output_tiers[args.intermediate_output_tokens] += TURNS_PER_ROLLOUT - 1
            output_tiers[
                (
                    args.straggler_terminal_output_tokens
                    if role(rollout) != "regular"
                    else args.regular_terminal_output_tokens
                )
            ] += 1

    dag = out / "async_rl_rollouts.dag.jsonl"
    seed = out / "global_seed.dag.jsonl"
    write_jsonl(dag, rows)
    write_jsonl(
        seed, [seed_row(index, global_system) for index in range(SEED_SESSIONS)]
    )
    shutil.copyfile(dag, out / "async_rl_rollouts.base.jsonl")
    shutil.copyfile(seed, out / "global_seed.base.jsonl")

    total_output = sum(tokens * count for tokens, count in output_tiers.items())
    manifest = {
        "schema": "async-rl-rollout-cache-load-causal-v1",
        "model": MODEL,
        "steps": STEPS,
        "rollouts_per_step": ROLLOUTS_PER_STEP,
        "rollouts_total": len(rows),
        "turns_per_rollout": TURNS_PER_ROLLOUT,
        "profile_requests_total": len(rows) * TURNS_PER_ROLLOUT,
        "seed_sessions": SEED_SESSIONS,
        "cache_contract": {
            "global_prefix_content_tokens": GLOBAL_SYSTEM_TOKENS,
            "step_shared_prefix_content_tokens": STEP_STATE_TOKENS,
            "unique_rollout_brief_content_tokens": UNIQUE_BRIEF_TOKENS,
            "global_prefix_seeded_on_every_replica": True,
            "step_prefix_seeded_on_every_replica": False,
            "cpu_kv_offload": False,
            "reason": "The 2K global prefix remains reusable everywhere. Rollout 0 and delayed rollout 4 share a 512-token step state, giving rollout 4 cache affinity before its terminal decode decision.",
        },
        "session_contract": {
            "routing_header": "x-correlation-id",
            "session_id": "one globally unique, stable session ID per rollout",
            "all_turns_serial": True,
            "parent_or_step_header": "absent",
            "session_affinity": "all ten turns stay statically pinned; the 20 unique stragglers are calibrated to one ConsistentHash replica",
            "kv_token_aware": "SelectionService scores live cache overlap and active prefill/decode blocks for every request",
        },
        "async_contract": {
            "pool": "all rollout sessions are one asynchronous workload; there is no step completion barrier",
            "client_concurrency": "AIPerf holds exactly 16 global HTTP-turn credits through streaming completion",
            "replacement": "a released credit immediately permits a ready continuation or another released rollout",
        },
        "timing_contract": {
            "mode": "AIPerf fixed_schedule; root turn-0 timestamps are absolute and auto-offset to zero",
            "step_stagger_ms": args.step_stagger_ms,
            "follower_after_anchor_ms": args.follower_gap_ms,
            "regular_after_anchor_ms": args.regular_start_lag_ms,
            "reason": "Rollout 0 warms the step prefix and enters its terminal 8K decode before rollout 4's terminal admission. The follower can therefore retain its cache-rich owner or trade that cache for a less loaded worker.",
        },
        "straggler_contract": {
            "per_step": 2,
            "anchor_rollout_index": 0,
            "follower_rollout_index": 4,
            "terminal_turn_index": TURNS_PER_ROLLOUT - 1,
            "terminal_output_tokens": args.straggler_terminal_output_tokens,
            "expected_straggler_turns": STEPS * 2,
        },
        "output_contract": {
            "intermediate_turn_output_tokens": args.intermediate_output_tokens,
            "regular_terminal_output_tokens": args.regular_terminal_output_tokens,
            "straggler_terminal_output_tokens": args.straggler_terminal_output_tokens,
        },
        "expected_output_tier_counts": {
            str(key): value for key, value in sorted(output_tiers.items())
        },
        "expected_total_output_tokens": total_output,
        "files": {
            "dag": {"path": dag.name, "sha256": sha256(dag)},
            "seed": {"path": seed.name, "sha256": sha256(seed)},
        },
    }
    text = json.dumps(manifest, indent=2) + "\n"
    (out / "manifest.json").write_text(text)
    (out / "manifest.base.json").write_text(text)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
