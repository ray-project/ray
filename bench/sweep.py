"""TTFT benchmark harness for PD disaggregation.

Drives an OpenAI-compatible endpoint with streaming requests and records
time-to-first-token. Used against both sides of the comparison:

  side A: Ray Serve LLM + SGLang PD   (bench/launch_ray_pd.py)
  side B: native sglang_router --pd-disaggregation (bench/launch_native_pd.sh)

Both sides are hit by this same client so no harness skew enters the delta.

Measurement rules that make the numbers mean something:
  * streaming only — TTFT is the arrival of the first content chunk, not the
    full response;
  * fixed input length — TTFT is prefill-dominated, so variable prompts turn
    into variance that swamps a 5-15% routing delta;
  * warmup requests are discarded — cold CUDA graphs and lazy NIXL rendezvous
    otherwise dominate p99;
  * every request's TTFT is kept, not just the mean, so tail latency survives
    into the analysis.

Writes newline-delimited JSON, one record per request, for offline analysis.
"""

import argparse
import asyncio
import json
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Optional

import aiohttp


@dataclass
class RequestResult:
    """One request's timing. ``ttft_s`` is None if the request failed."""

    concurrency: int
    rep: int
    ttft_s: Optional[float]
    total_s: Optional[float]
    output_tokens: int
    error: Optional[str] = None


def build_prompt(tokenizer_hint_tokens: int) -> str:
    """Produce a prompt of roughly a fixed token count.

    Uses a repeated common word: ~1 token each for most BPE vocabularies, which
    keeps prefill work constant across requests without needing the tokenizer.
    """
    return "word " * tokenizer_hint_tokens


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
            async for raw_line in resp.content:
                line = raw_line.decode("utf-8").strip()
                if not line.startswith("data: "):
                    continue
                data = line[len("data: ") :]
                if data == "[DONE]":
                    break
                chunk = json.loads(data)
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
    prompt: str,
    max_tokens: int,
    concurrency: int,
    num_requests: int,
    rep: int,
) -> list[RequestResult]:
    """Run ``num_requests`` at a fixed concurrency, keeping the level saturated."""
    results: list[RequestResult] = []
    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=600)
    ) as session:

        async def guarded() -> None:
            async with sem:
                results.append(
                    await one_request(
                        session, url, model, prompt, max_tokens, concurrency, rep
                    )
                )

        await asyncio.gather(*(guarded() for _ in range(num_requests)))

    return results


async def warmup(url: str, model: str, prompt: str, count: int) -> None:
    """Discarded requests that pay for CUDA graph capture and NIXL rendezvous."""
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=600)
    ) as session:
        for _ in range(count):
            await one_request(session, url, model, prompt, 16, 1, -1)


def summarize(results: list[RequestResult]) -> dict:
    """Percentiles over successful requests only; failures are counted separately."""
    ok = [r.ttft_s for r in results if r.ttft_s is not None]
    failed = sum(1 for r in results if r.ttft_s is None)
    if not ok:
        return {"n": 0, "failed": failed}

    ok.sort()

    def pct(p: float) -> float:
        # Nearest-rank: no interpolation, so small-n tails stay honest.
        idx = min(int(len(ok) * p), len(ok) - 1)
        return ok[idx]

    return {
        "n": len(ok),
        "failed": failed,
        "ttft_mean_s": statistics.fmean(ok),
        "ttft_p50_s": pct(0.50),
        "ttft_p95_s": pct(0.95),
        "ttft_p99_s": pct(0.99),
        "ttft_min_s": ok[0],
        "ttft_max_s": ok[-1],
    }


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
        "--input-tokens", type=int, default=1024, help="Approx prompt length."
    )
    parser.add_argument(
        "--max-tokens", type=int, default=128, help="Generated tokens per request."
    )
    parser.add_argument(
        "--warmup", type=int, default=20, help="Discarded warmup requests."
    )
    parser.add_argument("--out", required=True, help="Output .jsonl path.")
    return parser.parse_args()


async def main_async() -> None:
    args = parse_args()
    prompt = build_prompt(args.input_tokens)

    print(f"[{args.label}] warmup: {args.warmup} requests", flush=True)
    await warmup(args.url, args.model, prompt, args.warmup)

    all_results: list[RequestResult] = []
    with open(args.out, "w") as fh:
        for concurrency in args.concurrency:
            for rep in range(args.reps):
                block = await run_block(
                    url=args.url,
                    model=args.model,
                    prompt=prompt,
                    max_tokens=args.max_tokens,
                    concurrency=concurrency,
                    num_requests=args.requests_per_level,
                    rep=rep,
                )
                all_results.extend(block)
                for result in block:
                    record = asdict(result)
                    record["label"] = args.label
                    fh.write(json.dumps(record) + "\n")
                fh.flush()

                summary = summarize(block)
                print(
                    f"[{args.label}] c={concurrency} rep={rep} "
                    f"p50={summary.get('ttft_p50_s', float('nan')):.4f}s "
                    f"p99={summary.get('ttft_p99_s', float('nan')):.4f}s "
                    f"failed={summary['failed']}",
                    flush=True,
                )

    print(f"[{args.label}] overall: {json.dumps(summarize(all_results))}", flush=True)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
