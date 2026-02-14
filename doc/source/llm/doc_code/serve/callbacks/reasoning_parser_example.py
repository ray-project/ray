"""
This file serves as a documentation example and CI test.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking and removes accelerator requirements for CI testing.
2. Docs example (between __reasoning_parser_example_start/end__): Embedded in Sphinx docs via literalinclude.
3. Test validation (deployment status polling + cleanup)
"""

import time
from ray import serve
from ray.serve.schema import ApplicationStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve import llm

_original_serve_run = serve.run
_original_build_openai_app = llm.build_openai_app


def _non_blocking_serve_run(app, **kwargs):
    """Forces blocking=False for testing"""
    kwargs["blocking"] = False
    return _original_serve_run(app, **kwargs)


def _testing_build_openai_app(llm_serving_args):
    """Removes accelerator requirements for testing"""
    for config in llm_serving_args["llm_configs"]:
        config.accelerator_type = None
        # Disable compile cache to avoid cache corruption in CI
        if not config.runtime_env:
            config.runtime_env = {}
        if "env_vars" not in config.runtime_env:
            config.runtime_env["env_vars"] = {}
        config.runtime_env["env_vars"]["VLLM_DISABLE_COMPILE_CACHE"] = "1"

    return _original_build_openai_app(llm_serving_args)


serve.run = _non_blocking_serve_run
llm.build_openai_app = _testing_build_openai_app

# __reasoning_parser_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.llm.callbacks import CallbackBase


class ReasoningParserCallback(CallbackBase):
    """Registers a custom reasoning parser after node initialization.

    vLLM supports custom reasoning parsers for models that produce
    structured reasoning output (e.g., chain-of-thought). This callback
    registers the parser so it is available when the engine starts.

    Because vLLM uses spawn multiprocessing, parser registrations in
    the parent process don't transfer to spawned worker processes.
    Use this callback to register the parser inside the replica process.
    """

    async def on_after_node_init(self) -> None:
        from vllm.reasoning import ReasoningParserManager

        parser_plugin = self.kwargs["reasoning_parser_plugin"]
        ReasoningParserManager.import_reasoning_parser(parser_plugin)


llm_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen-0.5b",
        "model_source": "Qwen/Qwen2.5-0.5B-Instruct",
    },
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 1,
            "max_replicas": 2,
        }
    },
    accelerator_type="A10G",
    callback_config={
        "callback_class": ReasoningParserCallback,
        "callback_kwargs": {
            "reasoning_parser_plugin": "my_reasoning_parser_plugin",
        },
    },
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __reasoning_parser_example_end__

status = ApplicationStatus.NOT_STARTED
timeout_seconds = 300
start_time = time.time()

while (
    status != ApplicationStatus.RUNNING and time.time() - start_time < timeout_seconds
):
    status = serve.status().applications[SERVE_DEFAULT_APP_NAME].status

    if status in [ApplicationStatus.DEPLOY_FAILED, ApplicationStatus.UNHEALTHY]:
        raise AssertionError(f"Deployment failed with status: {status}")

    time.sleep(1)

if status != ApplicationStatus.RUNNING:
    raise AssertionError(
        f"Deployment failed to reach RUNNING status within {timeout_seconds}s. Current status: {status}"
    )

serve.shutdown()
