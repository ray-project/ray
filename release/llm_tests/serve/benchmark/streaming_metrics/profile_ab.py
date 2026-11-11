"""py-spy profiles of one replica's vLLM frontend under identical load, OFF vs ON.

Usage: python profile_ab.py --out DIR [--modes off on] [--seconds 30]
"""
import argparse
import collections
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import abtest  # noqa: E402

PY = abtest.PY


def replica_pids():
    from ray.util.state import list_actors

    alive = list_actors(filters=[("state", "=", "ALIVE")], limit=1000, detail=True)
    return [
        a.pid for a in alive if a.class_name.startswith("ServeReplica:sm:LLMServer")
    ]


def summarize(raw_path, top=25):
    """Aggregate py-spy raw (collapsed) stacks: self time by frame and inclusive
    time for frames matching our hooks or vLLM's stream generator."""
    leaf = collections.Counter()
    inclusive = collections.Counter()
    total = 0
    for line in open(raw_path, errors="replace"):
        stack, _, count = line.rstrip().rpartition(" ")
        if not count.isdigit():
            continue  # A frame string containing stray bytes or newlines.
        count = int(count)
        total += count
        frames = stack.split(";")
        leaf[frames[-1]] += count
        seen = set()
        for frame in frames:
            key = frame.split(" (")[0]
            if key not in seen:
                seen.add(key)
                inclusive[key] += count
    return total, leaf.most_common(top), inclusive


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--modes", nargs="+", default=["off", "on"])
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    import ray

    os.environ["RAY_RUNTIME_ENV_HOOK"] = "sm_noop_hook.env_hook"
    ray.init(address="auto", namespace="serve")
    replicas, proxy = abtest.actors()
    pid = replica_pids()[0]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    try:
        for mode in args.modes:
            run = out / mode
            abtest.set_mode(mode == "on", replicas, proxy, run)
            bench = subprocess.Popen(
                [
                    PY,
                    "bench.py",
                    "run",
                    "--out",
                    str(run / "bench"),
                    "--rate",
                    "36",
                    "--conc",
                    "256",
                    "--num",
                    "2048",
                    "--clients",
                    "32",
                    "--seed",
                    "1000",
                ],
                stdout=open(run / "bench.txt", "w"),
                stderr=subprocess.STDOUT,
            )
            time.sleep(35)  # Reach steady state.
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
                    "200",
                    "-f",
                    "raw",
                    "-o",
                    str(raw),
                    "--nonblocking",
                ],
                check=True,
                capture_output=True,
            )
            bench.wait()
            total, leaves, inclusive = summarize(raw)
            print(f"\n=== {mode}: {total} samples (pid {pid})")
            for frame, count in leaves:
                print(f"  {count / total * 100:6.2f}%  {frame[:150]}")
            for key in sorted(inclusive):
                if any(
                    s in key
                    for s in (
                        "completion_stream_generator",
                        "with_counts",
                        "_null",
                        "model_dump_json",
                        "stream_response",
                        "process_outputs",
                        "output_handler",
                        "detokeniz",
                    )
                ):
                    print(f"  incl {inclusive[key] / total * 100:6.2f}%  {key[:150]}")
    finally:
        abtest.set_mode(True, replicas, proxy, out / "restored")


if __name__ == "__main__":
    main()
