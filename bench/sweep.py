"""TTFT benchmark harness for PD disaggregation.

Drives an OpenAI-compatible endpoint with streaming requests and records
time-to-first-token. Used against both sides of the comparison:

  side A: Ray Serve LLM + SGLang PD   (bench/launch_ray_pd.py)
  side B: native sglang_router --pd-disaggregation (bench/launch_native_pd.sh)

Both sides are hit by this same client so no harness skew enters the delta.

Measurement rules that make the numbers mean something:
  * streaming only — TTFT is the arrival of the first content chunk, not the
    full response;
  * fixed input *length*, randomized input *content* — the length has to be
    constant or its variance swamps the routing delta being measured, but a
    literally identical prompt makes every request after the first a
    prefix-cache hit on the prefill replica, which is exactly the work P/D
    disaggregation exists to move;
  * warmup requests are discarded — cold CUDA graphs and lazy NIXL rendezvous
    otherwise dominate p99;
  * every request's TTFT is kept, not just the mean, so tail latency survives
    into the analysis.

Writes newline-delimited JSON, one record per request, for offline analysis.
"""

import argparse
import asyncio
import json
import random
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Optional

import aiohttp

# Workload shapes (input tokens, output tokens), named to match the shapes Ray
# published for its own P/D benchmarks so results are directly comparable.
# "balanced" is this harness's original shape, kept so earlier runs stay
# reproducible.
WORKLOADS = {
    "prefill-heavy": (8000, 50),
    "decode-heavy": (50, 500),
    "balanced": (1024, 128),
}

# Drawn from to build prompts. Common short English words are ~1 BPE token
# each, so the word count approximates the token count without loading a
# tokenizer into the client.
_VOCAB = (
    "time year people way day man thing woman life child world school state "
    "family student group country problem hand part place case week company "
    "system program question work government number night point home water "
    "room mother area money story fact month lot right study book eye job "
    "word business issue side kind head house service friend father power "
    "hour game line end member law car city community name president team "
    "minute idea kid body information back parent face others level office "
    "door health person art war history party result change morning reason "
    "research girl guy moment air teacher force education foot boy age policy "
    "process music market sense nation plan college interest death experience "
    "effect use class control care field development role effort rate heart"
).split()


@dataclass
class RequestResult:
    """One request's timing. ``ttft_s`` is None if the request failed."""

    concurrency: int
    rep: int
    ttft_s: Optional[float]
    total_s: Optional[float]
    output_tokens: int
    error: Optional[str] = None


def build_prompt(tokenizer_hint_tokens: int, rng: random.Random) -> str:
    """Produce a randomized prompt of roughly a fixed token count.

    Every request gets a *different* prompt. A fixed prompt (the previous
    behaviour) makes every request after the first a prefix-cache hit on the
    prefill side, which erases most of the prefill cost -- exactly the work P/D
    disaggregation exists to move onto its own replicas. Randomizing keeps
    prefill honest while the word count keeps its size constant.

    ``rng`` is passed in so a run can be seeded and replayed.
    """
    return " ".join(rng.choice(_VOCAB) for _ in range(tokenizer_hint_tokens))


async def one_request(
    session: aiohttp.ClientSession,
    url: str,
    model: str,
    prompt: str,
    max_tokens: int,
    concurrency: int,
    rep: int,
) -> RequestResult:
    """Issue one streaming completion and time the first content chunk."""
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "stream": True,
        # Greedy: sampling variance would show up as TTFT noise across sides.
        "temperature": 0.0,
    }

    start = time.perf_counter()
    ttft: Optional[float] = None
    output_tokens = 0

    try:
        async with session.post(f"{url}/v1/chat/completions", json=payload) as resp:
            resp.raise_for_status()
            # readline() buffers internally across TCP reads, so a frame split
            # or coalesced mid-line by the network still yields one complete
            # line per call; iterating resp.content directly relies on the
            # same buffering but obscures EOF handling, so drive it explicitly.
            while True:
                raw_line = await resp.content.readline()
                if not raw_line:
                    break
                line = raw_line.decode("utf-8").strip()
                if not line.startswith("data: "):
                    continue
                data = line[len("data: ") :]
                if data == "[DONE]":
                    break
                try:
                    chunk = json.loads(data)
                except json.JSONDecodeError:
                    # A line split across reads despite readline()'s buffering
                    # (e.g. a proxy coalescing frames) -- skip it rather than
                    # fail the whole request over one malformed SSE line.
                    continue
                delta = chunk["choices"][0].get("delta", {})
                if not delta.get("content"):
                    # Role-only preamble chunk carries no generated token.
                    continue
                if ttft is None:
                    ttft = time.perf_counter() - start
                output_tokens += 1
    except Exception as exc:  # noqa: BLE001 - a failed request is a data point
        return RequestResult(
            concurrency=concurrency,
            rep=rep,
            ttft_s=None,
            total_s=None,
            output_tokens=0,
            error=f"{type(exc).__name__}: {exc}",
        )

    return RequestResult(
        concurrency=concurrency,
        rep=rep,
        ttft_s=ttft,
        total_s=time.perf_counter() - start,
        output_tokens=output_tokens,
    )


async def run_block(
    url: str,
    model: str,
    input_tokens: int,
    max_tokens: int,
    concurrency: int,
    num_requests: int,
    rep: int,
    rng: random.Random,
) -> list[RequestResult]:
    """Run ``num_requests`` at a fixed concurrency, keeping the level saturated."""
    results: list[RequestResult] = []
    sem = asyncio.Semaphore(concurrency)

    # Built up front rather than inside the gather: generating an 8000-word
    # prompt takes real time, and doing it while requests are in flight would
    # charge that to the event loop the timings are measured on.
    prompts = [build_prompt(input_tokens, rng) for _ in range(num_requests)]

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=600)
    ) as session:

        async def guarded(prompt: str) -> None:
            async with sem:
                results.append(
                    await one_request(
                        session, url, model, prompt, max_tokens, concurrency, rep
                    )
                )

        await asyncio.gather(*(guarded(p) for p in prompts))

    return results


async def warmup(
    url: str, model: str, input_tokens: int, count: int, rng: random.Random
) -> None:
    """Discarded requests that pay for CUDA graph capture and NIXL rendezvous."""
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=600)
    ) as session:
        for _ in range(count):
            await one_request(
                session, url, model, build_prompt(input_tokens, rng), 16, 1, -1
            )


def summarize(results: list[RequestResult], wall_s: Optional[float] = None) -> dict:
    """TTFT/TPOT percentiles over successful requests; failures counted separately.

    Three metrics, because one workload shape cannot expose all of them:
    prefill-heavy work shows up in TTFT, decode-heavy work in TPOT, and
    throughput is what a serving system is actually bought for.

    ``wall_s`` is the block's elapsed time. Throughput needs it because
    per-request durations overlap under concurrency -- summing them would count
    the same wall-clock second once per in-flight request.
    """
    succeeded = [r for r in results if r.ttft_s is not None]
    failed = sum(1 for r in results if r.ttft_s is None)
    if not succeeded:
        return {"n": 0, "failed": failed}

    ok = sorted(r.ttft_s for r in succeeded)

    # Time per output token, excluding the first: TPOT is the steady-state
    # inter-token cost, and folding TTFT into it would just re-measure prefill.
    tpots = [
        (r.total_s - r.ttft_s) / (r.output_tokens - 1)
        for r in succeeded
        if r.output_tokens > 1 and r.total_s is not None
    ]
    tpots.sort()

    def pct(vals: list, p: float) -> float:
        # Nearest-rank: no interpolation, so small-n tails stay honest.
        idx = min(int(len(vals) * p), len(vals) - 1)
        return vals[idx]

    out = {
        "n": len(ok),
        "failed": failed,
        "ttft_mean_s": statistics.fmean(ok),
        "ttft_p50_s": pct(ok, 0.50),
        "ttft_p95_s": pct(ok, 0.95),
        "ttft_p99_s": pct(ok, 0.99),
        "ttft_min_s": ok[0],
        "ttft_max_s": ok[-1],
    }

    if tpots:
        out.update(
            {
                "tpot_mean_s": statistics.fmean(tpots),
                "tpot_p50_s": pct(tpots, 0.50),
                "tpot_p95_s": pct(tpots, 0.95),
                "tpot_p99_s": pct(tpots, 0.99),
            }
        )

    if wall_s:
        total_out = sum(r.output_tokens for r in succeeded)
        out.update(
            {
                "wall_s": wall_s,
                "output_tok_per_s": total_out / wall_s,
                "requests_per_s": len(succeeded) / wall_s,
            }
        )

    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="Endpoint base.")
    parser.add_argument("--model", required=True, help="Served model name.")
    parser.add_argument(
        "--label",
        required=True,
        help="Tag for this run, e.g. ray_pd_1p1d or native_pd_1p1d.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        nargs="+",
        default=[1, 8, 32, 64],
        help="Concurrency levels to sweep.",
    )
    parser.add_argument(
        "--requests-per-level",
        type=int,
        default=100,
        help="Requests at each concurrency level, per rep.",
    )
    parser.add_argument("--reps", type=int, default=3, help="Repetitions per level.")
    parser.add_argument(
        "--workload",
        choices=sorted(WORKLOADS),
        help=(
            "Named (input, output) token shape; sets --input-tokens and "
            "--max-tokens. Explicit flags still win if both are given."
        ),
    )
    parser.add_argument(
        "--input-tokens", type=int, default=None, help="Approx prompt length."
    )
    parser.add_argument(
        "--max-tokens", type=int, default=None, help="Generated tokens per request."
    )
    parser.add_argument(
        "--warmup", type=int, default=20, help="Discarded warmup requests."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Prompt-randomization seed, so a run can be replayed.",
    )
    parser.add_argument("--out", required=True, help="Output .jsonl path.")
    args = parser.parse_args()

    # A workload supplies defaults; an explicit flag overrides it.
    wl_in, wl_out = WORKLOADS.get(args.workload, WORKLOADS["balanced"])
    if args.input_tokens is None:
        args.input_tokens = wl_in
    if args.max_tokens is None:
        args.max_tokens = wl_out
    return args


async def main_async() -> None:
    args = parse_args()
    # One generator for the whole run: seeded, so the prompt sequence replays
    # identically across systems and every side sees the same inputs.
    rng = random.Random(args.seed)

    print(
        f"[{args.label}] workload={args.workload or 'custom'} "
        f"ISL={args.input_tokens} OSL={args.max_tokens} seed={args.seed}",
        flush=True,
    )
    print(f"[{args.label}] warmup: {args.warmup} requests", flush=True)
    await warmup(args.url, args.model, args.input_tokens, args.warmup, rng)

    all_results: list[RequestResult] = []
    with open(args.out, "w") as fh:
        for concurrency in args.concurrency:
            for rep in range(args.reps):
                block_start = time.perf_counter()
                block = await run_block(
                    url=args.url,
                    model=args.model,
                    input_tokens=args.input_tokens,
                    max_tokens=args.max_tokens,
                    concurrency=concurrency,
                    num_requests=args.requests_per_level,
                    rep=rep,
                    rng=rng,
                )
                block_wall = time.perf_counter() - block_start
                all_results.extend(block)
                for result in block:
                    record = asdict(result)
                    record["label"] = args.label
                    # Stamped per row so a results file is self-describing:
                    # comparisons across workloads read these instead of
                    # relying on the filename.
                    record["workload"] = args.workload or "custom"
                    record["input_tokens"] = args.input_tokens
                    record["max_tokens"] = args.max_tokens
                    fh.write(json.dumps(record) + "\n")
                fh.flush()

                summary = summarize(block, wall_s=block_wall)
                print(
                    f"[{args.label}] c={concurrency} rep={rep} "
                    f"ttft_p50={summary.get('ttft_p50_s', float('nan')):.4f}s "
                    f"ttft_p99={summary.get('ttft_p99_s', float('nan')):.4f}s "
                    f"tpot_p50={summary.get('tpot_p50_s', float('nan')):.4f}s "
                    f"tok/s={summary.get('output_tok_per_s', float('nan')):.1f} "
                    f"failed={summary['failed']}",
                    flush=True,
                )

    # No wall_s across the whole run: the blocks are sequential and separated by
    # bookkeeping, so a single elapsed number would understate throughput.
    # Per-block throughput above is the honest measure.
    print(f"[{args.label}] overall: {json.dumps(summarize(all_results))}", flush=True)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
