"""Launch the Ray Serve LLM SGLang PD-disaggregation deployment.

Mirrors release/llm_tests/serve/test_llm_serve_sglang_pd.py: prefill and decode
replicas with real NIXL KV transport, serving an OpenAI-compatible endpoint on
http://localhost:8000/v1.

Parametrized for benchmarking: model, prefill/decode replica counts, tensor
parallel size and GPU layout are CLI flags, so one node can sweep 1P/1D, 2P/2D
and 4P/4D without editing source.

Runs blocking so a driver script can background it, wait for the readiness
sentinel on stdout, benchmark against it, then kill the process.
"""

import argparse
import os
import time
from typing import Optional

from ray import serve
from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
    build_pd_openai_app,
)
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig
from ray.serve.schema import ApplicationStatus

READY_SENTINEL = "RAY_PD_READY"


def _sglang_config(
    model_source: str,
    model_id: str,
    num_replicas: int,
    tp_size: int,
    base_gpu_id: int,
    mem_fraction_static: float,
    stream_batching_interval_ms: Optional[int] = None,
) -> dict:
    """Build one side (prefill or decode) of the P/D pair.

    ``base_gpu_id`` pins this side to its own GPU range so prefill and decode
    never share a device; the caller lays out the whole node.

    ``stream_batching_interval_ms`` controls Ray Serve LLM's streaming batcher.
    It defaults to 50ms, and that batcher waits out its interval *before*
    draining its queue with no exemption for the first token -- so it adds up
    to 50ms to TTFT. SGLang's own batching is counted in decode steps and
    always flushes the first token, so leaving Ray's default in place measures
    a buffering-policy difference on top of the orchestration difference. Ray's
    published benchmark config sets this to 0; pass 0 here to match it.
    """
    return LLMConfig(
        model_loading_config={
            "model_id": model_id,
            "model_source": model_source,
        },
        deployment_config={
            "autoscaling_config": {
                "min_replicas": num_replicas,
                "max_replicas": num_replicas,
            },
            # Replicas are actors in their own processes, so they don't inherit
            # this shell's environment. Forward the trace switch explicitly, or
            # bench/pd_trace.py reads it as unset and every replica silently
            # traces nothing. Only forwarded when set, so untraced runs carry
            # no runtime_env at all.
            **(
                {
                    "ray_actor_options": {
                        "runtime_env": {
                            "env_vars": {
                                "RAY_PD_TRACE": os.environ["RAY_PD_TRACE"],
                                # Replicas cwd elsewhere; pd_server.py imports
                                # bench.pd_trace by name, so the repo root has
                                # to be importable inside the actor.
                                "PYTHONPATH": os.path.dirname(
                                    os.path.dirname(os.path.abspath(__file__))
                                ),
                            }
                        }
                    }
                }
                if os.environ.get("RAY_PD_TRACE")
                else {}
            ),
        },
        engine_kwargs={
            # disaggregation_mode is set automatically by the builder.
            "disaggregation_transfer_backend": "nixl",
            "tp_size": tp_size,
            "mem_fraction_static": mem_fraction_static,
            "base_gpu_id": base_gpu_id,
        },
        llm_engine="SGLang",
        # Read by LLMServer._get_batch_interval_ms(); omitted (None) falls
        # through to Ray's own 50ms default, so this only appears when the
        # caller explicitly asked to override it.
        experimental_configs=(
            {"stream_batching_interval_ms": stream_batching_interval_ms}
            if stream_batching_interval_ms is not None
            else {}
        ),
    ).model_dump()


def _app_is_running() -> bool:
    try:
        return (
            serve.status().applications[SERVE_DEFAULT_APP_NAME].status
            == ApplicationStatus.RUNNING
        )
    except (KeyError, AttributeError):
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--model",
        default="Qwen/Qwen2.5-7B-Instruct",
        help="HuggingFace model source.",
    )
    parser.add_argument(
        "--model-id",
        default=None,
        help="Served model name. Defaults to a slug derived from --model.",
    )
    parser.add_argument(
        "--prefill-replicas", type=int, default=1, help="Number of prefill replicas."
    )
    parser.add_argument(
        "--decode-replicas", type=int, default=1, help="Number of decode replicas."
    )
    parser.add_argument(
        "--tp-size", type=int, default=1, help="Tensor parallel size per replica."
    )
    parser.add_argument(
        "--mem-fraction-static",
        type=float,
        default=0.85,
        help="SGLang static memory fraction per replica.",
    )
    parser.add_argument(
        "--startup-timeout",
        type=int,
        default=1200,
        help="Seconds to wait for the app to reach RUNNING.",
    )
    parser.add_argument(
        "--stream-batching-interval-ms",
        type=int,
        default=None,
        help=(
            "Override Ray Serve LLM's streaming batcher (default 50ms; it "
            "waits out the interval before draining, with no first-token "
            "exemption, so the default adds up to 50ms to TTFT). Ray's own "
            "published benchmark config sets this to 0. Omit to keep the "
            "framework default."
        ),
    )
    return parser.parse_args()


def main() -> None:
    os.environ["RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"] = "1"
    args = parse_args()
    model_id = args.model_id or args.model.split("/")[-1].lower() + "-sglang-pd"

    # Lay out the node: prefill takes the low GPUs, decode takes the rest.
    prefill_gpus = args.prefill_replicas * args.tp_size
    prefill = _sglang_config(
        model_source=args.model,
        model_id=model_id,
        num_replicas=args.prefill_replicas,
        tp_size=args.tp_size,
        base_gpu_id=0,
        mem_fraction_static=args.mem_fraction_static,
        stream_batching_interval_ms=args.stream_batching_interval_ms,
    )
    decode = _sglang_config(
        model_source=args.model,
        model_id=model_id,
        num_replicas=args.decode_replicas,
        tp_size=args.tp_size,
        base_gpu_id=prefill_gpus,
        mem_fraction_static=args.mem_fraction_static,
        stream_batching_interval_ms=args.stream_batching_interval_ms,
    )

    app = build_pd_openai_app({"prefill_config": prefill, "decode_config": decode})
    serve.run(app, blocking=False)

    deadline = time.time() + args.startup_timeout
    while time.time() < deadline:
        if _app_is_running():
            print(READY_SENTINEL, flush=True)
            break
        time.sleep(2)
    else:
        raise RuntimeError(
            f"Ray PD app did not reach RUNNING within {args.startup_timeout}s"
        )

    # Stay alive so the benchmark can hit the endpoint; the driver kills this PID.
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
