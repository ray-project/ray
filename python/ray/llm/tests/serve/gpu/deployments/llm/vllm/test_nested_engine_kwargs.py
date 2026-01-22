"""Regression test for nested dict in engine_kwargs.

See: https://github.com/ray-project/ray/pull/60380

When engine_kwargs contains nested dicts (e.g., structured_outputs_config),
they must be converted to argparse.Namespace so vLLM's init_app_state() can
access nested attributes via dot notation.

Without the fix, starting the engine would fail with:
    AttributeError: 'dict' object has no attribute 'backend'
"""
import sys

import pytest

import ray
from ray.llm._internal.serve.engines.vllm.vllm_engine import VLLMEngine
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.util.placement_group import (
    PlacementGroupSchedulingStrategy,
    placement_group,
)


@pytest.mark.asyncio
async def test_vllm_engine_with_nested_structured_outputs_config(model_smolvlm_256m):
    """Regression test: engine starts with nested structured_outputs_config.

    This test verifies that nested dicts in engine_kwargs are properly converted
    to Namespace objects, allowing vLLM to access nested attributes.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="smolvlm-256m",
            model_source=model_smolvlm_256m,
        ),
        engine_kwargs=dict(
            gpu_memory_utilization=0.4,
            use_tqdm_on_load=False,
            enforce_eager=True,
            max_model_len=2048,
            # Nested dict that caused AttributeError before the fix
            structured_outputs_config={
                "backend": "xgrammar",
            },
        ),
        placement_group_config={"bundles": [{"GPU": 0.49}]},
        runtime_env=dict(
            env_vars={
                "VLLM_DISABLE_COMPILE_CACHE": "1",
            },
        ),
    )

    pg = placement_group(bundles=[{"GPU": 1, "CPU": 1}])

    strategy = PlacementGroupSchedulingStrategy(
        pg, placement_group_capture_child_tasks=True, placement_group_bundle_index=0
    )

    @ray.remote(num_cpus=1, scheduling_strategy=strategy)
    class Actor:
        def __init__(self):
            self.engine = VLLMEngine(llm_config)

        async def start(self):
            await self.engine.start()

        async def check_health(self):
            await self.engine.check_health()

        async def shutdown(self):
            self.engine.shutdown()

    actor = Actor.remote()
    await actor.start.remote()
    await actor.check_health.remote()
    await actor.shutdown.remote()
    del pg


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
