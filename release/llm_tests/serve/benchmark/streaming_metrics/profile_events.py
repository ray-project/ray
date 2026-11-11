"""Per-event replica event-loop time by frame, alternating abtest arms.

At 18 req/s vLLM merges no tokens, so each streamed event is one chunk. py-spy
samples one replica's busiest (event-loop) thread; inclusive samples of frames of
interest, scaled by the thread's CPU per event, give microseconds per event.

Usage: python profile_events.py --out DIR [--arms copies on copies on] [--seconds 40]
"""
import argparse
import collections
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import abtest  # noqa: E402

FRAMES = (
    "completion_stream_generator",
    "with_counts",
    "with_identity",
    "model_dump_json",
    "stream_response",
    "process_outputs",
    "output_handler",
    "send",
    "write",
    "_null",
    "__init__ (pydantic/main.py",
    "request_response",
    "generate (vllm",
)


def loop_thread_stacks(raw_path):
    """{thread: Counter(stack)} from py-spy raw output recorded with --threads."""
    threads = collections.defaultdict(collections.Counter)
    for line in open(raw_path, errors="replace"):
        stack, _, count = line.rstrip().rpartition(" ")
        if not count.isdigit():
            continue
        frames = stack.split(";")
        threads[frames[0]][stack] += int(count)
    return threads


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--arms", nargs="+", default=["copies", "on", "copies", "on"])
    parser.add_argument("--seconds", type=int, default=40)
    args = parser.parse_args()
    import ray

    os.environ["RAY_RUNTIME_ENV_HOOK"] = "sm_noop_hook.env_hook"
    ray.init(address="auto", namespace="serve", log_to_driver=False)
    replicas, proxy = abtest.actors()
    pid = ray.get(replicas[0].__ray_call__.remote(abtest.replica_pid))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    results = []
    try:
        for i, arm in enumerate(args.arms):
            run = out / f"{arm}_{i}"
            sides = {
                "copies": ("copies", False),
                "on": (True, True),
                "py": (True, False),
            }[arm]
            abtest.set_mode(arm == "on", replicas, proxy, run, "dist_final", *sides)
            bench = subprocess.Popen(
                [
                    abtest.PY,
                    "bench.py",
                    "run",
                    "--out",
                    str(run / "bench"),
                    "--rate",
                    "18",
                    "--conc",
                    "128",
                    "--num",
                    "1260",
                    "--clients",
                    "16",
                    "--seed",
                    "1000",
                ],
                stdout=open(run / "bench.txt", "w"),
                stderr=subprocess.STDOUT,
            )
            time.sleep(15)
            before = abtest.thread_sched([pid])
            raw = run / "profile.txt"
            subprocess.run(
                [
                    "py-spy",
                    "record",
                    "-p",
                    str(pid),
                    "-d",
                    str(args.seconds),
                    "-r",
                    "500",
                    "--threads",
                    "-f",
                    "raw",
                    "-o",
                    str(raw),
                    "--nonblocking",
                ],
                check=True,
                capture_output=True,
            )
            after = abtest.thread_sched([pid])
            bench.wait()
            loop = abtest.sched_delta(before, after)[0]
            threads = loop_thread_stacks(raw)
            busiest = max(threads, key=lambda t: sum(threads[t].values()))
            stacks = threads[busiest]
            total = sum(stacks.values())
            inclusive = collections.Counter()
            for stack, count in stacks.items():
                seen = set()
                for frame in stack.split(";")[1:]:
                    for key in FRAMES:
                        if key in frame and key not in seen:
                            seen.add(key)
                            inclusive[key] += count
            # Events in the profiled window: client chunks scaled by the window's share.
            a = abtest_events(run / "bench")
            row = {
                "arm": arm,
                "i": i,
                "thread": busiest,
                "samples": total,
                "loop_run_s": loop["run_s"],
                "events_per_s_replica": a / 2,
                "inclusive_frac": {k: v / total for k, v in inclusive.items()},
            }
            results.append(row)
            print(json.dumps(row), flush=True)
    finally:
        abtest.set_mode(True, replicas, proxy, out / "restored", "dist_final")
    (out / "results.json").write_text(json.dumps(results, indent=1))


def abtest_events(bench_dir):
    """Streamed events per second across both replicas, from the client logs."""
    import glob
    import gzip

    n, t0, t1 = 0, None, None
    for f in glob.glob(str(bench_dir / "client[0-9]*.json*")):
        j = json.load(gzip.open(f, "rt") if f.endswith(".gz") else open(f))
        for s, itl, lat, e in zip(
            j["start_times"], j["itls"], j["latencies"], j["errors"]
        ):
            if not e:
                n += len(itl) + 1
                t0 = s if t0 is None else min(t0, s)
                t1 = s + lat if t1 is None else max(t1, s + lat)
    return n / (t1 - t0)


if __name__ == "__main__":
    main()
