#!/usr/bin/env python
"""Deploy or tear down one router variant.

Only the router differs between variants: every other knob here is identical
across router variants and is echoed to stdout so run_cell.sh can record it in meta.json.

    python deploy.py --router session-affinity --replicas 4 --tp 2 --routing-log-dir DIR
    python deploy.py --shutdown

Run from the benchmark directory or its parent.
"""

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
from ray.serve.config import ControllerOptions, RequestRouterConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app

MODEL = "openai/gpt-oss-120b"

CONTROLLER_ENV = {
    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
    "RAY_SERVE_ENABLE_HA_PROXY": "1",
    "RAY_SERVE_SESSION_ID_HEADER_KEY": "x-correlation-id",
    "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY": "1",
}

REPLICA_ENV_KEYS = (
    "RAY_RUNTIME_ENV_HOOK",
    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
    "RAY_SERVE_SESSION_ID_HEADER_KEY",
    "RAY_SERVE_LLM_ENABLE_DECODE_BLOCK_PROGRESS",
    "VLLM_MEMORY_PROFILER_ESTIMATE_CUDAGRAPHS",
    "VLLM_DISABLE_COMPILE_CACHE",
    "HF_HOME",
    "PYTHONPATH",
)


def deployment_runtime_env(variant_env: dict[str, str]) -> dict[str, object]:
    """Return the per-router environment needed by model and ingress actors.

    Ray does not copy a head process environment into application workers.
    Preserve only the model/ingress settings that they consume; controller
    settings are supplied separately through ``ControllerOptions``.
    ``LLMConfig.runtime_env`` also propagates the model-side interpreter to
    RayExecutorV2's TP workers.
    """
    env_vars = {key: os.environ[key] for key in REPLICA_ENV_KEYS if key in os.environ}
    env_vars.update(variant_env)
    runtime_env: dict[str, object] = {"env_vars": env_vars}
    runtime_python = os.environ.get("AGENTIC_RUNTIME_PYTHON")
    if runtime_python:
        if not os.path.isabs(runtime_python) or not os.path.isfile(runtime_python):
            sys.exit(
                "AGENTIC_RUNTIME_PYTHON must name an existing absolute Python "
                f"executable, got {runtime_python!r}"
            )
        if not os.access(runtime_python, os.X_OK):
            sys.exit(f"AGENTIC_RUNTIME_PYTHON is not executable: {runtime_python}")
        runtime_env["py_executable"] = runtime_python
    return runtime_env


def gpu_mem_used_mb() -> list[int]:
    out = subprocess.run(
        ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [int(x) for x in out.split()]


def wait_for_gpu_memory_to_clear(
    threshold_mb: int = 1000, timeout_s: int = 600
) -> None:
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
    variant: str,
    replicas: int,
    tp: int,
    max_model_len: int,
    gpu_mem_util: float,
    routing_log_dir: str | None,
    runtime_env: dict[str, object],
) -> LLMConfig:
    router_cls, router_kwargs, _variant_env = routers.ROUTER_VARIANTS[variant]

    # The serialized mixin carries this class attribute to the ingress process.
    routers.RoutingLogMixin.LOG_DIR = routing_log_dir

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id=MODEL, model_source=MODEL),
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=replicas, max_replicas=replicas),
            # Long streams need a relaxed health-check timeout.
            health_check_period_s=30,
            health_check_timeout_s=120,
            request_router_config=RequestRouterConfig(
                request_router_class=router_cls,  # type: ignore[arg-type]
                request_router_kwargs=router_kwargs,
            ),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=tp,
            max_model_len=max_model_len,
            gpu_memory_utilization=gpu_mem_util,
            # Keep the measured decode-block metric aligned with the engine.
            block_size=16,
            # Required by all three routing variants and the cache-hit metric.
            enable_prefix_caching=True,
            # Provide server token accounting for each streamed response.
            enable_force_include_usage=True,
            # Required for response-level prefix-cache hit rate.
            enable_prompt_tokens_details=True,
            # Avoid the model image's FP4 fusion warmup failure.
            compilation_config={
                "pass_config": {"fuse_allreduce_rms": False},
            },
        ),
        runtime_env=runtime_env,
    )
    return llm_config


def validate_official_kv_wiring(variant: str, llm_config: LLMConfig) -> None:
    """Check KV events added by Ray's official app builder."""
    if variant not in {"pure-kv-cache", "kv-token-aware"}:
        return
    kv_events = llm_config.engine_kwargs.get("kv_events_config")
    if (
        not isinstance(kv_events, dict)
        or kv_events.get("enable_kv_cache_events") is not True
    ):
        raise RuntimeError(
            "build_openai_app did not enable KV events for KVAwareRouter"
        )


def configure_tokenizing_router_python(
    app: object, runtime_env: dict[str, object]
) -> None:
    """Run the tokenizing ingress under the patched Dynamo interpreter."""
    runtime_python = runtime_env.get("py_executable")
    if runtime_python is None:
        return
    ingress = getattr(app, "_ingress_request_router", None)
    if ingress is None:
        raise RuntimeError(
            "build_openai_app did not create the tokenizing ingress router"
        )
    deployment = getattr(ingress, "_bound_deployment", None)
    if deployment is None:
        raise RuntimeError("tokenizing ingress router deployment is unavailable")
    actor_options = dict(deployment._replica_config.ray_actor_options)  # noqa: SLF001
    ingress_runtime_env = dict(actor_options.get("runtime_env") or {})
    ingress_runtime_env["py_executable"] = runtime_python
    actor_options["runtime_env"] = ingress_runtime_env
    ingress._bound_deployment = deployment.options(ray_actor_options=actor_options)


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
            # KVAware's selection reservation and the engine's completion
            # lifecycle must use the same key even for this deployment probe.
            # Without it, a successful session-less readiness request leaves a
            # four-block phantom decode load on one replica before profiling.
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
    ap.add_argument("--router", choices=sorted(routers.ROUTER_VARIANTS), default=None)
    ap.add_argument("--replicas", type=int, default=4)
    ap.add_argument("--tp", type=int, default=2)
    ap.add_argument("--max-model-len", type=int, default=131072)
    # CUDA graph profiling is disabled, so 0.85 leaves sampler-warmup headroom.
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.85)
    ap.add_argument("--routing-log-dir", default=None)
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--ready-timeout", type=int, default=2400)
    ap.add_argument(
        "--shutdown",
        action="store_true",
        help="tear down and wait for GPU memory to clear",
    )
    ap.add_argument("--meta-out", default=None)
    args = ap.parse_args()

    if args.shutdown:
        ray.init(address="auto", ignore_reinit_error=True)
        serve.shutdown()
        print("[deploy] serve.shutdown() returned; waiting for GPU memory")
        wait_for_gpu_memory_to_clear()
        return 0

    if not args.router:
        ap.error("--router is required unless --shutdown")

    # Ingress has a different working directory, so routing logs need an absolute path.
    routing_log_dir = (
        os.path.abspath(args.routing_log_dir) if args.routing_log_dir else None
    )
    if routing_log_dir:
        os.makedirs(routing_log_dir, exist_ok=True)

    # The runtime hook strips an inherited cluster working directory before engine startup.
    router_cls, router_kwargs, variant_env = routers.ROUTER_VARIANTS[args.router]
    runtime_env = deployment_runtime_env(variant_env)
    ray.init(address="auto", ignore_reinit_error=True)

    # Send the local router classes by value to the ingress process.
    ray.cloudpickle.register_pickle_by_value(sys.modules["routers"])

    llm_config = build_config(
        args.router,
        args.replicas,
        args.tp,
        args.max_model_len,
        args.gpu_memory_utilization,
        routing_log_dir,
        runtime_env,
    )
    print(
        f"[deploy] variant={args.router} class={router_cls.__name__} kwargs={router_kwargs}"
    )
    print(
        f"[deploy] replicas={args.replicas} tp={args.tp} max_model_len={args.max_model_len}"
    )
    print(f"[deploy] engine_kwargs={llm_config.engine_kwargs}")

    # Controller settings must be supplied before application creation.
    serve.start(
        controller_options=ControllerOptions(runtime_env={"env_vars": CONTROLLER_ENV})
    )
    app = build_openai_app({"llm_configs": [llm_config]})
    validate_official_kv_wiring(args.router, llm_config)
    # Only the demonstration-only pure-KV-cache scorer needs the patched
    # Dynamo wheel in SelectionService. The official KVAwareRouter does not.
    if args.router == "pure-kv-cache":
        configure_tokenizing_router_python(app, runtime_env)
        print(
            "[deploy] build_openai_app configured KV events and patched cache-only scoring"
        )
    serve.run(app, blocking=False)
    ready_s = wait_ready(args.url, args.ready_timeout, MODEL)

    if args.meta_out:
        meta = {
            "variant": args.router,
            "router_class": f"{router_cls.__module__}.{router_cls.__name__}",
            "router_kwargs": router_kwargs,
            "router_scoring_env": variant_env,
            "replicas": args.replicas,
            "tp": args.tp,
            "gpus_total": args.replicas * args.tp,
            "model": MODEL,
            "engine_kwargs": dict(llm_config.engine_kwargs),
            "health_check_period_s": 30,
            "health_check_timeout_s": 120,
            "ready_seconds": round(ready_s, 1),
            "ray_version": ray.__version__,
            "ray_commit": ray.__commit__,
            "runtime_python": runtime_env.get("py_executable"),
            "tokenizing_router_runtime_python": (
                runtime_env.get("py_executable")
                if args.router == "pure-kv-cache"
                else None
            ),
            "kv_events_enabled": bool(
                (llm_config.engine_kwargs.get("kv_events_config") or {}).get(
                    "enable_kv_cache_events"
                )
            ),
            "runtime_environment": "scoped through Ray Serve runtime environments",
        }
        with open(args.meta_out, "w") as fh:
            json.dump(meta, fh, indent=2)
        print(f"[deploy] wrote {args.meta_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
