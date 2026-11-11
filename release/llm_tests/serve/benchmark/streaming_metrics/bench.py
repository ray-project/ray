"""Steady-state vllm bench serve driver and analyzer for ON/OFF comparisons.

run:     K parallel `vllm bench serve` clients (identical seeds across modes),
         Poisson arrivals at total rate R capped at total concurrency C.
analyze: keep only steady-state requests (started after the ramp and before the
         tail), then report TTFT / TPOT / client ITL / E2E percentiles.
"""
import argparse
import glob
import json
import os
import subprocess
import time
from pathlib import Path

import numpy as np

VLLM = "/mnt/local_storage/sm/sm_env/bin/vllm"
MODEL = "Qwen/Qwen3.5-0.8B"
TOKENIZER = (
    "/home/ray/.cache/huggingface/hub/models--Qwen--Qwen3.5-0.8B/snapshots/"
    "2fc06364715b967f1860aea9cf38778875588b17"
)


METRICS_URL = "http://127.0.0.1:8085/metrics"
HISTOGRAMS = [
    "time_to_first_token",
    "inter_token_latency",
    "router_overhead",
    "request_duration",
]


def scrape():
    """Per-series values of the HAProxy stream histograms and error counters.

    HAProxy reloads start new generations (new series) and old generations'
    series expire, so callers compare series by series, never global sums.
    """
    import urllib.request

    text = urllib.request.urlopen(METRICS_URL, timeout=10).read().decode()
    series = {}
    for line in text.splitlines():
        if not line.startswith("ray_serve_llm_"):
            continue
        name, value = line.rsplit(" ", 1)
        metric = name.split("{", 1)[0]
        wanted = "collection_errors" in metric or any(
            metric
            in (f"ray_serve_llm_{h}_seconds_count", f"ray_serve_llm_{h}_seconds_sum")
            for h in HISTOGRAMS
        )
        if wanted:
            series[name] = float(value)
    return series


def series_delta(before, after):
    """Totals of per-series increases; vanished series contribute nothing."""
    totals = {}
    for name, value in after.items():
        metric = name.split("{", 1)[0]
        key = "errors" if "collection_errors" in metric else metric
        totals[key] = totals.get(key, 0.0) + max(0.0, value - before.get(name, 0.0))
    return totals


def run(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    before_metrics = scrape()
    procs = []
    for k in range(args.clients):
        conc = args.conc // args.clients
        num = args.num // args.clients
        rate = args.rate / args.clients if args.rate > 0 else "inf"
        cmd = [
            VLLM,
            "bench",
            "serve",
            "--backend",
            args.backend,
            "--base-url",
            args.url,
            "--endpoint",
            args.endpoint,
            "--model",
            MODEL,
            "--tokenizer",
            TOKENIZER,
            "--dataset-name",
            "random",
            "--random-input-len",
            str(args.isl),
            "--random-output-len",
            str(args.osl),
            "--random-range-ratio",
            "0",
            "--ignore-eos",
            "--num-prompts",
            str(num),
            "--max-concurrency",
            str(conc),
            "--request-rate",
            str(rate),
            "--seed",
            str(args.seed + k),
            "--num-warmups",
            str(args.warmups // args.clients),
            "--percentile-metrics",
            "ttft,tpot,itl,e2el",
            "--metric-percentiles",
            "50,90,99",
            "--disable-tqdm",
            "--save-result",
            "--save-detailed",
            "--result-dir",
            str(out),
            "--result-filename",
            f"client{k}.json",
        ]
        log = open(out / f"client{k}.log", "w")
        env = dict(
            os.environ,
            HF_HUB_OFFLINE="1",
            TOKENIZERS_PARALLELISM="false",
            RAYON_NUM_THREADS="1",
            OMP_NUM_THREADS="1",
            MKL_NUM_THREADS="1",
            OPENBLAS_NUM_THREADS="1",
        )
        # Clients get their own cores so their bursts never exceed the cgroup quota
        # alongside the server; Python asyncio clients need one core each at most.
        cmd = ["taskset", "-c", args.client_cpus] + cmd
        procs.append(
            subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
        )
    (out / "run.json").write_text(json.dumps(vars(args), indent=2))
    # Sample each client's CPU once per second (utime+stime ticks from /proc).
    hz = os.sysconf("SC_CLK_TCK")

    def ticks(pid):
        try:
            f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
            return int(f[11]) + int(f[12])
        except (FileNotFoundError, ProcessLookupError, IndexError):
            return None

    def cgroup():
        stat = dict(
            line.split() for line in open("/sys/fs/cgroup/cpu.stat").read().splitlines()
        )
        return {
            k: int(stat[k]) for k in ("usage_usec", "nr_throttled", "throttled_usec")
        }

    cg0, t_cg0 = cgroup(), time.time()
    last = {p.pid: ticks(p.pid) for p in procs}
    samples = []
    timeline = []
    while any(p.poll() is None for p in procs):
        time.sleep(1.0)
        timeline.append((time.monotonic(), cgroup()["nr_throttled"]))
        for p in procs:
            t = ticks(p.pid)
            if t is not None and last.get(p.pid) is not None:
                samples.append((t - last[p.pid]) / hz)
            last[p.pid] = t
    codes = [p.wait() for p in procs]
    time.sleep(3)  # HAProxy exports cumulative snapshots every second.
    after_metrics = scrape()
    (out / "haproxy_metrics.json").write_text(
        json.dumps({"before": before_metrics, "after": after_metrics})
    )
    busy = sorted(s for s in samples if s > 0.02)
    cg1, t_cg1 = cgroup(), time.time()
    cpu = {
        "p50_cores": busy[len(busy) // 2] if busy else 0,
        "p90_cores": busy[int(len(busy) * 0.9)] if busy else 0,
        "cgroup_cores": (cg1["usage_usec"] - cg0["usage_usec"]) / 1e6 / (t_cg1 - t_cg0),
        "throttled_periods": cg1["nr_throttled"] - cg0["nr_throttled"],
        "throttled_s": (cg1["throttled_usec"] - cg0["throttled_usec"]) / 1e6,
    }
    (out / "cpu.json").write_text(json.dumps(cpu))
    (out / "throttle_timeline.json").write_text(json.dumps(timeline))
    print(f"client cores per process: {cpu}")
    if any(codes):
        raise SystemExit(f"client failures: {codes} in {out}")
    print(json.dumps(analyze_dir(out, args.skip_frac)))


def pct(a, q):
    return float(np.percentile(a, q)) if len(a) else float("nan")


def analyze_dir(out, skip_frac=0.2):
    out = Path(out)
    reqs = []
    firsts, lasts = [], []
    files = sorted(
        glob.glob(str(out / "client[0-9]*.json"))
        + glob.glob(str(out / "client[0-9]*.json.gz"))
    )
    for f in files:
        import gzip

        d = json.load(gzip.open(f, "rt") if f.endswith(".gz") else open(f))
        firsts.append(min(d["start_times"]))
        lasts.append(max(d["start_times"]))
        for i in range(len(d["ttfts"])):
            if d["errors"][i]:
                reqs.append(None)
                continue
            reqs.append(
                dict(
                    start=d["start_times"][i],
                    ttft=d["ttfts"][i],
                    itls=d["itls"][i],
                    lat=d["latencies"][i],
                    olen=d["output_lens"][i],
                )
            )
    failed = sum(r is None for r in reqs)
    reqs = [r for r in reqs if r is not None]
    starts = np.array([r["start"] for r in reqs])
    # Only the interval in which every client is issuing requests, then trimmed.
    t0, t1 = max(firsts), min(lasts)
    # Steady state: drop the ramp and the tail by start time.
    lo, hi = t0 + skip_frac * (t1 - t0), t1 - skip_frac * (t1 - t0)
    ss = [r for r in reqs if lo <= r["start"] <= hi]
    ends = np.array([r["start"] + r["lat"] for r in reqs])
    # Mean in-flight requests during the steady window.
    grid = np.linspace(lo, hi, 400)
    inflight = np.mean([np.sum((starts <= g) & (ends > g)) for g in grid])
    throttled_in_window = None
    tl = out / "throttle_timeline.json"
    if tl.exists():
        series = json.loads(tl.read_text())
        inside = [n for t, n in series if lo <= t <= hi + 30]
        throttled_in_window = (inside[-1] - inside[0]) if len(inside) > 1 else 0
    ttft = np.array([r["ttft"] for r in ss]) * 1e3
    tpot = np.array([(r["lat"] - r["ttft"]) / (r["olen"] - 1) for r in ss]) * 1e3
    itl = np.concatenate([np.array(r["itls"]) for r in ss]) * 1e3
    e2e = np.array([r["lat"] for r in ss])
    toks = sum(r["olen"] for r in ss)
    haproxy = None
    hm = out / "haproxy_metrics.json"
    if hm.exists():
        m = json.loads(hm.read_text())
        if any("{" in k for k in m["after"]):
            delta = series_delta(m["before"], m["after"])
        else:  # Older runs saved global totals.
            delta = {
                k: m["after"].get(k, 0) - m["before"].get(k, 0) for k in m["after"]
            }
        n_all = len(reqs) + failed
        tok_all = sum(r["olen"] - 1 for r in reqs)

        def mean(h):
            c = delta.get(f"ray_serve_llm_{h}_seconds_count", 0)
            return (
                delta.get(f"ray_serve_llm_{h}_seconds_sum", 0) / c
                if c
                else float("nan")
            )

        haproxy = {
            "ttft_count": delta.get(
                "ray_serve_llm_time_to_first_token_seconds_count", 0
            ),
            "itl_count": delta.get(
                "ray_serve_llm_inter_token_latency_seconds_count", 0
            ),
            "request_count": delta.get(
                "ray_serve_llm_request_duration_seconds_count", 0
            ),
            "routing_count": delta.get(
                "ray_serve_llm_router_overhead_seconds_count", 0
            ),
            "errors": delta.get("errors", 0),
            "client_requests": n_all,
            "client_itl_tokens": tok_all,
            "ttft_mean_ms": mean("time_to_first_token") * 1e3,
            "itl_mean_ms": mean("inter_token_latency") * 1e3,
            "request_mean_s": mean("request_duration"),
            "routing_mean_ms": mean("router_overhead") * 1e3,
            "client_ttft_mean_ms": float(np.mean([r["ttft"] for r in reqs])) * 1e3,
            "client_tpot_mean_ms": float(
                np.mean([(r["lat"] - r["ttft"]) / (r["olen"] - 1) for r in reqs])
            )
            * 1e3,
            "client_e2e_mean_s": float(np.mean([r["lat"] for r in reqs])),
        }
    return {
        "haproxy": haproxy,
        "dir": str(out),
        "requests": len(reqs),
        "failed": failed,
        "steady_requests": len(ss),
        "steady_inflight": round(float(inflight), 1),
        "throttled_in_window": throttled_in_window,
        "output_lens_ok": all(r["olen"] for r in ss),
        "ttft_ms": {
            "mean": ttft.mean(),
            "p50": pct(ttft, 50),
            "p90": pct(ttft, 90),
            "p99": pct(ttft, 99),
        },
        "tpot_ms": {"mean": tpot.mean(), "p50": pct(tpot, 50), "p90": pct(tpot, 90)},
        "itl_ms": {
            "mean": itl.mean(),
            "p50": pct(itl, 50),
            "p90": pct(itl, 90),
            "p99": pct(itl, 99),
        },
        "e2e_s": {"mean": e2e.mean(), "p50": pct(e2e, 50), "p90": pct(e2e, 90)},
        "chunks_per_token": len(itl) / max(1, sum(r["olen"] - 1 for r in ss)),
        "steady_tokens": toks,
    }


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--out", required=True)
    r.add_argument("--conc", type=int, required=True)
    r.add_argument("--rate", type=float, default=0.0)
    r.add_argument("--num", type=int, required=True)
    r.add_argument("--clients", type=int, default=4)
    r.add_argument("--seed", type=int, default=1000)
    r.add_argument("--warmups", type=int, default=0)
    r.add_argument("--isl", type=int, default=1024)
    r.add_argument("--osl", type=int, default=1024)
    r.add_argument("--backend", default="openai")
    r.add_argument("--endpoint", default="/v1/completions")
    r.add_argument("--url", default="http://127.0.0.1:8000")
    r.add_argument("--skip-frac", type=float, default=0.2)
    r.add_argument("--client-cpus", default="128-191")
    a = sub.add_parser("analyze")
    a.add_argument("dirs", nargs="+")
    a.add_argument("--skip-frac", type=float, default=0.2)
    args = p.parse_args()
    if args.cmd == "run":
        run(args)
    else:
        for d in args.dirs:
            print(json.dumps(analyze_dir(d, args.skip_frac)))


if __name__ == "__main__":
    main()
