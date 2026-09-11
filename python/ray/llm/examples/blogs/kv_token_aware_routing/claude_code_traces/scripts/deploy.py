#!/usr/bin/env python
"""Deploy or tear down one CC router variant."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

import routers

import ray
from ray import serve
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app

MODEL = "openai/gpt-oss-120b"

# EngineCore reconnects to Ray and must not inherit the cluster working_dir.
WORKER_ENV_KEYS = (
    "PYTHONPATH",
    "RAY_RUNTIME_ENV_HOOK",
    "HF_HOME",
    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
    "RAY_SERVE_SESSION_ID_HEADER_KEY",
    "RAY_SERVE_LLM_ENABLE_DECODE_BLOCK_PROGRESS",
)

CONTROLLER_ENV_KEYS = (
    "RAY_SERVE_ENABLE_HA_PROXY",
    "RAY_SERVE_SESSION_ID_HEADER_KEY",
    "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY",
    "RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S",
    "RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE",
)


def env_vars(keys: tuple[str, ...]) -> dict[str, str]:
    """Read environment variables that must reach a Ray process."""
    missing = [key for key in keys if key not in os.environ]
    if missing:
        sys.exit("missing environment: " + ", ".join(missing) + "\n" "source scripts/env.sh first.")
    return {key: os.environ[key] for key in keys}


def deployment_runtime_env(arm_env: dict[str, str]) -> dict[str, object]:
    """Return the environment needed by model and ingress actors."""
    return {
        "env_vars": {**env_vars(WORKER_ENV_KEYS), **arm_env},
    }


def gpu_mem_used_mb() -> list[int]:
    out = subprocess.run(
        ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [int(x) for x in out.split()]


def wait_for_gpu_memory_to_clear(threshold_mb: int = 1000, timeout_s: int = 600) -> None:
    """Wait for a completed Serve shutdown to release GPU memory."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        used = gpu_mem_used_mb()
        if max(used) <= threshold_mb:
            print(f"[deploy] GPU memory clear: {used}")
            return
        print(f"[deploy] waiting for GPU memory to clear: {used}")
        time.sleep(10)
    raise TimeoutError(f"GPUs still busy after {timeout_s}s: {gpu_mem_used_mb()}")


def build_config(
    router_variant: str,
    replicas: int,
    tp: int,
    max_model_len: int,
    gpu_mem_util: float,
    routing_log_dir: str | None,
    runtime_env: dict[str, object],
) -> LLMConfig:
    router_cls, router_kwargs, _variant_env = routers.ROUTER_VARIANTS[router_variant]

    # Serialized with the router class for the ingress processes.
    routers.RoutingLogMixin.LOG_DIR = routing_log_dir

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id=MODEL, model_source=MODEL),
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=replicas, max_replicas=replicas),
            # Long streaming requests need a relaxed health-check timeout.
            health_check_period_s=30,
            health_check_timeout_s=120,
            request_router_config=RequestRouterConfig(
                request_router_class=router_cls,
                request_router_kwargs=router_kwargs,
            ),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=tp,
            max_model_len=max_model_len,
            gpu_memory_utilization=gpu_mem_util,
            # Required by KVAwareRouter and the cache-hit metric.
            enable_prefix_caching=True,
            # Needed for the response-level prefix-cache metric.
            enable_prompt_tokens_details=True,
        ),
        runtime_env=runtime_env,
    )

    return llm_config


def validate_official_kv_wiring(router_variant: str, llm_config: LLMConfig) -> None:
    """Check KV events added by Ray's official app builder."""
    if not router_variant.startswith("kv-token-aware"):
        return
    kv_events = llm_config.engine_kwargs.get("kv_events_config")
    if not isinstance(kv_events, dict) or kv_events.get("enable_kv_cache_events") is not True:
        raise RuntimeError("build_openai_app did not enable KV events for KVAwareRouter")


def wait_ready(url: str, timeout_s: int, model: str) -> float:
    """Poll until the app serves a real completion, not just /health."""
    t0 = time.time()
    deadline = t0 + timeout_s
    payload = json.dumps(
        {
            "model": model,
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 1,
            "stream": False,
        }
    ).encode()
    last = ""
    while time.time() < deadline:
        try:
            # Keep KVAware lifecycle events on one request ID.
            request_id = f"agentic-readiness-{uuid.uuid4()}"
            req = urllib.request.Request(
                f"{url}/v1/chat/completions",
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Request-ID": request_id,
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                response = json.loads(resp.read())
                if resp.status == 200 and response.get("choices"):
                    elapsed = time.time() - t0
                    print(f"[deploy] completion ready after {elapsed:.0f}s")
                    return elapsed
                last = f"status {resp.status} without completion choices"
        except Exception as exc:  # noqa: BLE001 - report whatever kept us waiting
            last = f"{type(exc).__name__}: {exc}"
        print(f"[deploy] not ready yet ({last}); {deadline - time.time():.0f}s left")
        time.sleep(15)
    raise TimeoutError(f"app not ready within {timeout_s}s; last: {last}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--router-variant", choices=sorted(routers.ROUTER_VARIANTS), default=None)
    ap.add_argument("--replicas", type=int, default=4)
    ap.add_argument("--tp", type=int, default=2)
    ap.add_argument("--max-model-len", type=int, default=131072)
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.85)
    ap.add_argument("--routing-log-dir", default=None)
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--ready-timeout", type=int, default=2400)
    ap.add_argument(
        "--shutdown", action="store_true", help="tear down and wait for GPU memory to clear"
    )
    ap.add_argument("--meta-out", default=None)
    args = ap.parse_args()

    if args.shutdown:
        ray.init(address="auto", ignore_reinit_error=True)
        serve.shutdown()
        print("[deploy] serve.shutdown() returned; waiting for GPU memory")
        wait_for_gpu_memory_to_clear()
        return 0

    if not args.router_variant:
        ap.error("--router-variant is required unless --shutdown")

    # Ingress processes need an absolute route-log path.
    routing_log_dir = os.path.abspath(args.routing_log_dir) if args.routing_log_dir else None
    if routing_log_dir:
        os.makedirs(routing_log_dir, exist_ok=True)

    # Propagate import-time settings into the existing Ray cluster.
    router_cls, router_kwargs, variant_env = routers.ROUTER_VARIANTS[args.router_variant]
    runtime_env = deployment_runtime_env(variant_env)
    ray.init(
        address="auto",
        ignore_reinit_error=True,
        runtime_env=runtime_env,
    )

    # Send the local router definitions to ingress processes.
    ray.cloudpickle.register_pickle_by_value(sys.modules["routers"])

    llm_config = build_config(
        args.router_variant,
        args.replicas,
        args.tp,
        args.max_model_len,
        args.gpu_memory_utilization,
        routing_log_dir,
        runtime_env,
    )
    print(
        "[deploy] router_variant="
        f"{args.router_variant} class={router_cls.__name__} kwargs={router_kwargs}"
    )
    print(f"[deploy] replicas={args.replicas} tp={args.tp} max_model_len={args.max_model_len}")
    print(f"[deploy] engine_kwargs={llm_config.engine_kwargs}")

    app = build_openai_app({"llm_configs": [llm_config]})
    validate_official_kv_wiring(args.router_variant, llm_config)
    if args.router_variant.startswith("kv-token-aware"):
        print("[deploy] build_openai_app configured KV events")
    # The detached HAProxy system actor needs the same ingress settings.
    controller_env = env_vars(CONTROLLER_ENV_KEYS)
    serve.run(
        app,
        blocking=False,
        controller_options={"runtime_env": {"env_vars": controller_env}},
    )
    ready_s = wait_ready(args.url, args.ready_timeout, MODEL)

    if args.meta_out:
        meta = {
            "router_variant": args.router_variant,
            "router_class": f"{router_cls.__module__}.{router_cls.__name__}",
            "router_kwargs": router_kwargs,
            "router_scoring_env": variant_env,
            "replicas": args.replicas,
            "tp": args.tp,
            "gpus_total": args.replicas * args.tp,
            "model": MODEL,
            "engine_kwargs": dict(llm_config.engine_kwargs),
            "kv_events_enabled": bool(
                (llm_config.engine_kwargs.get("kv_events_config") or {}).get(
                    "enable_kv_cache_events"
                )
            ),
            "health_check_period_s": 30,
            "health_check_timeout_s": 120,
            "ready_seconds": round(ready_s, 1),
            "ray_version": ray.__version__,
            "ray_commit": ray.__commit__,
            "worker_env": env_vars(WORKER_ENV_KEYS),
            "controller_env": controller_env,
        }
        with open(args.meta_out, "w") as fh:
            json.dump(meta, fh, indent=2)
        print(f"[deploy] wrote {args.meta_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
