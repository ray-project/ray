"""Deploy Qwen3.5-0.8B with direct streaming + HAProxy, metrics ON or OFF.

Runs on the existing cluster (never stops Ray). Every Serve actor (controller,
HAProxy proxy, router, replicas) runs from the vLLM 0.33 venv, whose ``ray``
package overlays this branch's ``serve``/``llm`` sources on the installed core.
"""
import argparse
import json
import os
import time
from pathlib import Path

VENV_PY = "/mnt/local_storage/sm/sm_env/bin/python"
SITE = "/mnt/local_storage/sm/sm_env/lib/python3.12/site-packages"
MODEL = "Qwen/Qwen3.5-0.8B"
REVISION = "2fc06364715b967f1860aea9cf38778875588b17"
APP = "sm"
COMPILE_ROOT = "/mnt/local_storage/sm/compile_cache"

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["on", "off"], required=True)
parser.add_argument("--out", required=True)
parser.add_argument("--replicas", type=int, default=2)
parser.add_argument("--extra-env", default="{}", help="JSON env overrides")
args = parser.parse_args()
out = Path(args.out)
out.mkdir(parents=True, exist_ok=True)

env = {
    "RAY_SERVE_ENABLE_HA_PROXY": "1",
    "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
    "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY": "1",
    "RAY_SERVE_HAPROXY_METRICS_ENABLED": "1",
    "RAY_SERVE_INGRESS_REQUEST_ROUTER_METRICS_ENABLED": "0",
    "RAY_SERVE_ENABLE_LLM_STREAMING_METRICS": "1" if args.mode == "on" else "0",
    "RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S": "600",
    "RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S": "0",
    "PYTHONPATH": SITE,
    "PATH": "/mnt/local_storage/sm/sm_env/bin:" + os.environ["PATH"],
    "HF_HUB_OFFLINE": "1",
    "OMP_NUM_THREADS": "1",
    "RAYLLM_ENABLE_REQUEST_PROMPT_LOGS": "0",
    "VLLM_USE_FLASHINFER_SAMPLER": "0",
    # Nested ray.init calls (vLLM's engine-core Ray executor) would otherwise run
    # the workspace hook and zip the whole workspace into /tmp/ray per deploy.
    "RAY_RUNTIME_ENV_HOOK": "sm_noop_hook.env_hook",
    # Reloading cached compiled kernels in a new engine fails with
    # CUDA_ERROR_INVALID_HANDLE; compile fresh, identically for ON and OFF.
    "VLLM_DISABLE_COMPILE_CACHE": "1",
    "VLLM_CACHE_ROOT": COMPILE_ROOT,
    "TORCHINDUCTOR_CACHE_DIR": COMPILE_ROOT + "/inductor",
    "TRITON_CACHE_DIR": COMPILE_ROOT + "/triton",
}
env.update(json.loads(args.extra_env))
os.environ.update(env)

import ray  # noqa: E402
import requests  # noqa: E402
from huggingface_hub import snapshot_download  # noqa: E402
from ray import serve  # noqa: E402
from ray.serve.experimental.round_robin_router import RoundRobinRouter  # noqa: E402
from ray.serve.llm import build_openai_app  # noqa: E402

runtime = {"env_vars": env, "py_executable": VENV_PY}
ray.init(address="auto", runtime_env=runtime, namespace="serve")
serve.shutdown()
time.sleep(2)
# Old replicas are gone; drop their compile artifacts before compiling fresh.
import shutil  # noqa: E402

shutil.rmtree(COMPILE_ROOT, ignore_errors=True)
os.makedirs(COMPILE_ROOT)
cfg = {
    "model_loading_config": {
        "model_id": MODEL,
        "model_source": snapshot_download(
            MODEL, revision=REVISION, local_files_only=True
        ),
    },
    "runtime_env": runtime,
    "engine_kwargs": {
        "tensor_parallel_size": 1,
        "max_model_len": 4096,
        "max_num_seqs": 256,
        "enable_prefix_caching": False,
    },
    "deployment_config": {
        "num_replicas": args.replicas,
        "request_router_config": {"request_router_class": RoundRobinRouter},
    },
}
meta = {
    "mode": args.mode,
    "env": env,
    "replicas": args.replicas,
    "started": time.time(),
}
t0 = time.time()
serve.run(
    build_openai_app({"llm_configs": [cfg]}),
    name=APP,
    route_prefix="/",
    blocking=False,
    controller_options={"runtime_env": {"env_vars": env}},
)
deadline = time.monotonic() + 1800
while time.monotonic() < deadline:
    st = serve.status()
    app = st.applications.get(APP)
    if app is not None and app.status == "DEPLOY_FAILED":
        (out / "failed_status.txt").write_text(str(app))
        raise RuntimeError(app.message)
    if app is not None and app.status == "RUNNING":
        try:
            r = requests.get("http://127.0.0.1:8000/v1/models", timeout=10)
            if r.ok and any(m["id"] == MODEL for m in r.json()["data"]):
                break
        except requests.RequestException:
            pass
    time.sleep(3)
else:
    raise TimeoutError("deployment not ready")
meta["ready_s"] = time.time() - t0
meta["deployments"] = {
    name: {"status": str(d.status), "replica_states": dict(d.replica_states)}
    for name, d in serve.status().applications[APP].deployments.items()
}
(out / "deploy.json").write_text(json.dumps(meta, indent=2, default=str))
print(f"READY mode={args.mode} in {meta['ready_s']:.0f}s", flush=True)
