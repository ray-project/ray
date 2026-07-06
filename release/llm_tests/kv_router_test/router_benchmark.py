"""LLM request-router benchmark: KVAware vs PrefixCacheAffinity vs ConsistentHash vs RoundRobin.

Deploys one model on N fixed replicas (direct streaming + HAProxy) under each
router, drives an ``aiperf`` workload against the OpenAI endpoint, and compares:
- TTFT / TPOT (ITL) percentiles and throughput (from aiperf's summary export)
- per-replica vLLM prefix-cache hit rate (scraped from engine logs; the direct
  evidence of routing-driven KV reuse)

Workloads (see BENCHMARK.md):
- same-session: homogeneous ISL/OSL multi-turn; prefix shared only within a
  conversation. KVAware/PrefixAffinity/ConsistentHash should tie; RoundRobin worse.
- cross-session: adds a pool of long prefixes shared across conversations.
  KVAware/PrefixAffinity co-locate sessions sharing a prefix; ConsistentHash only
  pins per-session; RoundRobin scatters everything.
- token-load: cross-session shared prefixes + heterogeneous decode lengths.
  PrefixAffinity concentrates all prefix-matching traffic; KVAware trades KV
  overlap against per-worker prefill/decode load and should win. KVAware scoring
  is tunable via --select-override (dynamo RouterConfigOverride fields, e.g.
  prefill_load_scale, overlap_score_weight).

Usage (manual, on a Ray cluster with GPUs; one command per benchmark):
    python router_benchmark.py --task cross-session -r kv -r prefix -r hash -r rr
    python router_benchmark.py --task token-load -r kv -r prefix \\
        --osl-stddev 150 -o /tmp/token_load.json
"""

import glob
import json
import logging
import os
import re
import subprocess
import time
import urllib.request
from typing import Any, Dict, List, Optional

import click

# Read at import time by ray.serve / ray.serve.llm; must be set before those import.
os.environ.setdefault("RAY_SERVE_ENABLE_HA_PROXY", "1")
os.environ.setdefault("RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING", "1")
# Body-aware routers (KVAware, PrefixCacheAffinity) need the request body at
# /internal/route; HAProxy does not forward it by default.
os.environ.setdefault("RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY", "1")
os.environ.setdefault("RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S", "30")

import ray  # noqa: E402
from ray import serve  # noqa: E402
from ray.serve.llm import LLMConfig, LLMServingArgs, build_openai_app  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:8000"
RAY_LOG_DIR = "/tmp/ray/session_latest/logs"


def _propagated_env_vars() -> Dict[str, str]:
    """Env vars every engine/replica process needs.

    RAY_RUNTIME_ENV_HOOK is forwarded when set so environments with a
    working-dir snapshot hook (e.g. workspaces) can neutralize it for the
    nested ray.init inside vLLM engine cores.
    """
    env = {
        "VLLM_DISABLE_COMPILE_CACHE": "1",
        # Serve reads these at import time in the controller/proxy/replica
        # processes, not just the driver; they must ride the job runtime env.
        "RAY_SERVE_ENABLE_HA_PROXY": "1",
        "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
        "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY": "1",
        "RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S": "30",
    }
    for key in ("RAY_RUNTIME_ENV_HOOK", "HF_TOKEN"):
        if key in os.environ:
            env[key] = os.environ[key]
    return env


ROUTERS: Dict[str, Dict[str, Any]] = {
    "kv": {
        "cls": "ray.serve.llm.request_router.KVAwareRouter",
        "kwargs": {},
        "label": "KVAwareRouter",
    },
    "prefix": {
        "cls": "ray.serve.llm.request_router.PrefixCacheAffinityRouter",
        "kwargs": {},
        "label": "PrefixCacheAffinityRouter",
    },
    "hash": {
        "cls": "ray.serve.experimental.consistent_hash_router.ConsistentHashRouter",
        "kwargs": {"num_virtual_nodes": 100, "num_fallback_replicas": 2},
        "label": "ConsistentHashRouter",
    },
    "rr": {
        "cls": "ray.serve.experimental.round_robin_router.RoundRobinRouter",
        "kwargs": {},
        "label": "RoundRobinRouter",
    },
}

# aiperf workload per BENCHMARK.md task. All tasks are multi-turn with a fixed
# turn count (prefix reuse within a conversation) and pinned OSL via aiperf's
# max_completion_tokens (osl_mismatch is asserted 0 by the summary).
TASK_WORKLOADS: Dict[str, Dict[str, Any]] = {
    "same-session": dict(
        num_conversations=64,
        turns=6,
        isl=1200,
        isl_stddev=0,
        osl=80,
        osl_stddev=0,
        concurrency=64,
        warmup=16,
    ),
    "cross-session": dict(
        num_conversations=128,
        turns=3,
        isl=400,
        isl_stddev=0,
        osl=80,
        osl_stddev=0,
        concurrency=64,
        warmup=16,
        # Pool of long prefixes shared across conversations (4 conversations per
        # prefix). Sized so the whole pool (~96k tokens) plus session histories
        # exceeds one replica's KV capacity: routers that scatter prefix-sharing
        # sessions (round-robin, per-session hash) evict, while KVAware /
        # PrefixAffinity consolidate each prefix onto one replica and keep it hot.
        num_prefix_prompts=32,
        prefix_length=4000,
    ),
    "token-load-prefill": dict(
        num_conversations=64,
        turns=4,
        isl=2000,
        # Heterogeneous prefill: prompt sizes vary widely.
        isl_stddev=600,
        osl=32,
        osl_stddev=0,
        concurrency=64,
        warmup=16,
        # Two shared prefixes for four replicas: PrefixCacheAffinity funnels all
        # matching sessions onto the owning replicas (it only checks queue
        # imbalance, unbounded by default); KVAware books prefill token load and
        # spills to idle replicas once overlap stops paying for the queueing.
        num_prefix_prompts=2,
        prefix_length=3000,
    ),
    "token-load-decode": dict(
        num_conversations=64,
        turns=4,
        isl=300,
        isl_stddev=0,
        osl=250,
        # Heterogeneous decode: some sessions generate 5-10x more tokens, so
        # queue length is a poor load proxy; KVAware's decode-block booking is
        # the differentiator.
        osl_stddev=150,
        concurrency=64,
        warmup=16,
        num_prefix_prompts=2,
        prefix_length=3000,
    ),
}


# ===================================================================
# Deploy / teardown
# ===================================================================


def build_app(
    router_key: str,
    model_id: str,
    model_source: str,
    num_replicas: int,
    accelerator_type: Optional[str],
    select_overrides: Optional[Dict[str, Any]],
):
    spec = ROUTERS[router_key]
    experimental_configs = {}
    if select_overrides and router_key == "kv":
        experimental_configs["KV_SELECT_OVERRIDES"] = select_overrides
    llm_config = LLMConfig(
        model_loading_config=dict(model_id=model_id, model_source=model_source),
        accelerator_type=accelerator_type,
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=num_replicas, max_replicas=num_replicas
            ),
            request_router_config=dict(
                request_router_class=spec["cls"],
                request_router_kwargs=spec["kwargs"],
            ),
        ),
        engine_kwargs=dict(
            max_model_len=8192,
            enable_prefix_caching=True,
            enforce_eager=True,
            gpu_memory_utilization=0.9,
        ),
        experimental_configs=experimental_configs,
        placement_group_config={"bundles": [{"GPU": 1, "CPU": 1}]},
        runtime_env=dict(env_vars=_propagated_env_vars()),
    )
    return build_openai_app(LLMServingArgs(llm_configs=[llm_config]))


def wait_ready(timeout_s: float = 900.0) -> bool:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            resp = urllib.request.urlopen(f"{BASE_URL}/v1/models", timeout=3)
            if b'"id"' in resp.read():
                return True
        except Exception:
            pass
        time.sleep(4)
    return False


def gpus_free(threshold_mib: int = 800) -> bool:
    out = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"]
    ).decode()
    return sum(int(x) for x in out.split()) < threshold_mib


def teardown(timeout_s: float = 300.0) -> None:
    serve.shutdown()
    start = time.time()
    while time.time() - start < timeout_s and not gpus_free():
        time.sleep(3)
    time.sleep(3)


# ===================================================================
# aiperf load generation + metric parsing
# ===================================================================


def build_aiperf_cmd(
    artifact_dir: str, model_id: str, tokenizer: str, wl: Dict[str, Any]
) -> List[str]:
    cmd = [
        "aiperf",
        "profile",
        "--model",
        model_id,
        "--url",
        BASE_URL,
        "--endpoint-type",
        "chat",
        "--streaming",
        "--tokenizer",
        tokenizer,
        "--artifact-dir",
        artifact_dir,
        "--num-conversations",
        str(wl["num_conversations"]),
        "--conversation-turn-mean",
        str(wl["turns"]),
        "--conversation-turn-stddev",
        "0",
        "--conversation-turn-delay-mean",
        "0",
        "--isl",
        str(wl["isl"]),
        "--isl-stddev",
        str(wl["isl_stddev"]),
        "--osl",
        str(wl["osl"]),
        "--osl-stddev",
        str(wl["osl_stddev"]),
        "--concurrency",
        str(wl["concurrency"]),
        "--request-count",
        str(wl["num_conversations"] * wl["turns"]),
        # One stable session id per conversation on Serve's session header, so
        # ConsistentHashRouter pins all turns of a conversation to one replica.
        "--session-header",
        "x-session-id",
        "--random-seed",
        "100",
    ]
    if wl.get("warmup"):
        cmd += ["--warmup-request-count", str(wl["warmup"])]
    if wl.get("num_prefix_prompts"):
        cmd += [
            "--num-prefix-prompts",
            str(wl["num_prefix_prompts"]),
            "--prefix-prompt-length",
            str(wl["prefix_length"]),
        ]
    return cmd


def parse_aiperf_summary(artifact_dir: str) -> Dict[str, Any]:
    paths = glob.glob(
        os.path.join(artifact_dir, "**", "profile_export_aiperf.json"), recursive=True
    )
    if not paths:
        raise FileNotFoundError(f"no aiperf summary under {artifact_dir}")
    with open(paths[0]) as f:
        summary = json.load(f)

    def metric(tag: str, stat: str) -> Optional[float]:
        block = summary.get(tag)
        return block.get(stat) if isinstance(block, dict) else None

    return {
        "ttft_avg_ms": metric("time_to_first_token", "avg"),
        "ttft_p50_ms": metric("time_to_first_token", "p50"),
        "ttft_p90_ms": metric("time_to_first_token", "p90"),
        "ttft_p99_ms": metric("time_to_first_token", "p99"),
        "tpot_avg_ms": metric("time_to_second_token", "avg"),
        "itl_avg_ms": metric("inter_token_latency", "avg"),
        "itl_p99_ms": metric("inter_token_latency", "p99"),
        "e2e_avg_ms": metric("request_latency", "avg"),
        "e2e_p99_ms": metric("request_latency", "p99"),
        "output_tok_per_s": metric("output_token_throughput", "avg"),
        "req_per_s": metric("request_throughput", "avg"),
        "requests": metric("request_count", "avg"),
        "isl_avg": metric("input_sequence_length", "avg"),
        "osl_avg": metric("output_sequence_length", "avg"),
        "osl_mismatch": metric("osl_mismatch_count", "avg"),
        "duration_s": metric("benchmark_duration", "avg"),
    }


def read_prefix_cache_hit_rates(num_replicas: int) -> List[float]:
    """Last 'Prefix cache hit rate: X%' per engine, newest engines first.

    Each deployment starts fresh engine-core processes, so right after a run the
    N most recently modified worker logs with a hit-rate line belong to it.
    """
    candidates = sorted(
        glob.glob(os.path.join(RAY_LOG_DIR, "worker-*.out")),
        key=lambda p: os.path.getmtime(p),
        reverse=True,
    )
    rates: List[float] = []
    pattern = re.compile(r"Prefix cache hit rate: ([0-9.]+)%")
    for path in candidates:
        try:
            with open(path, "rb") as f:
                data = f.read()
        except OSError:
            continue
        idx = data.rfind(b"Prefix cache hit rate:")
        if idx < 0:
            continue
        match = pattern.search(data[idx : idx + 64].decode("utf-8", "ignore"))
        if match:
            rates.append(float(match.group(1)))
        if len(rates) >= num_replicas:
            break
    return rates


# ===================================================================
# Orchestration
# ===================================================================


def run_router(
    router_key: str,
    task: str,
    wl: Dict[str, Any],
    model_id: str,
    model_source: str,
    num_replicas: int,
    accelerator_type: Optional[str],
    select_overrides: Optional[Dict[str, Any]],
    artifact_root: str,
) -> Dict[str, Any]:
    label = ROUTERS[router_key]["label"]
    logger.info("=== task=%s router=%s (%s) ===", task, router_key, label)
    result: Dict[str, Any] = {"router": router_key, "label": label}
    try:
        app = build_app(
            router_key,
            model_id,
            model_source,
            num_replicas,
            accelerator_type,
            select_overrides,
        )
        serve.run(app, name="router-benchmark")
        if not wait_ready():
            raise RuntimeError("deployment not ready")
        time.sleep(5)

        artifact_dir = os.path.join(artifact_root, f"{task}_{router_key}")
        cmd = build_aiperf_cmd(artifact_dir, model_id, model_source, wl)
        logger.info("aiperf: %s", " ".join(cmd))
        with open(os.path.join(artifact_root, f"{task}_{router_key}.log"), "w") as f:
            proc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
        if proc.returncode != 0:
            raise RuntimeError(f"aiperf failed rc={proc.returncode}")

        result.update(parse_aiperf_summary(artifact_dir))
        hit_rates = read_prefix_cache_hit_rates(num_replicas)
        result["cache_hit_rates"] = hit_rates
        result["cache_hit_mean"] = (
            sum(hit_rates) / len(hit_rates) if hit_rates else None
        )
        logger.info(
            "result %s: ttft_avg=%.0fms ttft_p99=%.0fms out_tps=%.0f cache_hit=%s",
            router_key,
            result["ttft_avg_ms"],
            result["ttft_p99_ms"],
            result["output_tok_per_s"],
            result["cache_hit_rates"],
        )
    except Exception as e:
        logger.exception("router %s failed", router_key)
        result["error"] = f"{type(e).__name__}: {e}"
    finally:
        teardown()
    return result


TABLE_ROWS = [
    ("TTFT avg (ms)", "ttft_avg_ms"),
    ("TTFT p50 (ms)", "ttft_p50_ms"),
    ("TTFT p90 (ms)", "ttft_p90_ms"),
    ("TTFT p99 (ms)", "ttft_p99_ms"),
    ("TPOT avg (ms)", "tpot_avg_ms"),
    ("ITL avg (ms)", "itl_avg_ms"),
    ("E2E p99 (ms)", "e2e_p99_ms"),
    ("Output tok/s", "output_tok_per_s"),
    ("Req/s", "req_per_s"),
    ("Cache hit rate %", "cache_hit_mean"),
    ("Requests", "requests"),
    ("ISL avg (tok)", "isl_avg"),
    ("OSL avg (tok)", "osl_avg"),
    ("OSL mismatch", "osl_mismatch"),
    ("Duration (s)", "duration_s"),
]


def print_table(task: str, wl: Dict[str, Any], results: List[Dict[str, Any]]) -> None:
    ok = [r for r in results if "error" not in r]
    width = 16
    line = "=" * (20 + width * max(len(ok), 1))
    print(f"\n{line}\nTASK {task}: " + ", ".join(f"{k}={v}" for k, v in wl.items()))
    print(line)
    print(f"{'metric':<20}" + "".join(f"{r['label'][:width-1]:>{width}}" for r in ok))
    print("-" * len(line))
    for name, key in TABLE_ROWS:
        cells = ""
        for r in ok:
            value = r.get(key)
            cells += f"{value:>{width}.1f}" if value is not None else f"{'-':>{width}}"
        print(f"{name:<20}{cells}")
    print(line)
    for r in results:
        if "error" in r:
            print(f"{r['router']}: ERROR {r['error']}")


@click.command()
@click.option(
    "--task",
    type=click.Choice(sorted(TASK_WORKLOADS)),
    required=True,
    help="Benchmark workload (see BENCHMARK.md).",
)
@click.option(
    "--router",
    "-r",
    "router_keys",
    multiple=True,
    type=click.Choice(sorted(ROUTERS)),
    default=("kv", "prefix", "hash", "rr"),
    help="Routers to benchmark. Repeat flag for multiple.",
)
@click.option("--model-id", default="qwen3-0.6b")
@click.option("--model-source", default="Qwen/Qwen3-0.6B")
@click.option("--num-replicas", default=4, type=int)
@click.option("--accelerator-type", default=None)
@click.option(
    "--concurrency", default=None, type=int, help="Override workload concurrency."
)
@click.option(
    "--osl-stddev", default=None, type=int, help="Override workload OSL stddev."
)
@click.option(
    "--select-override",
    multiple=True,
    help="KVAware dynamo RouterConfigOverride field as key=value "
    "(e.g. prefill_load_scale=2.0). Repeatable.",
)
@click.option("--output-path", "-o", default=None, help="Write results JSON here.")
@click.option(
    "--artifact-root", default="/tmp/router_benchmark", help="aiperf artifact dir."
)
def main(
    task: str,
    router_keys: tuple,
    model_id: str,
    model_source: str,
    num_replicas: int,
    accelerator_type: Optional[str],
    concurrency: Optional[int],
    osl_stddev: Optional[int],
    select_override: tuple,
    output_path: Optional[str],
    artifact_root: str,
):
    wl = dict(TASK_WORKLOADS[task])
    if concurrency is not None:
        wl["concurrency"] = concurrency
    if osl_stddev is not None:
        wl["osl_stddev"] = osl_stddev
    select_overrides = None
    if select_override:
        select_overrides = {
            k: json.loads(v) for k, v in (s.split("=", 1) for s in select_override)
        }

    os.makedirs(artifact_root, exist_ok=True)
    ray.init(
        address="auto",
        ignore_reinit_error=True,
        runtime_env={"env_vars": _propagated_env_vars()},
    )
    serve.shutdown()

    results = [
        run_router(
            key,
            task,
            wl,
            model_id,
            model_source,
            num_replicas,
            accelerator_type,
            select_overrides,
            artifact_root,
        )
        for key in router_keys
    ]

    print_table(task, wl, results)
    if output_path:
        with open(output_path, "w") as f:
            json.dump({"task": task, "workload": wl, "results": results}, f, indent=2)
        logger.info("results -> %s", output_path)


if __name__ == "__main__":
    main()
