import sys

import pytest
import requests

import ray
from ray.llm._internal.serve.engines.vllm.vllm_engine import VLLMEngine
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.fixture(scope="session")
def model_smolvlm_256m(tmp_path_factory):
    checkpoint_dir = tmp_path_factory.mktemp("smolvlm-256m")
    remote_url = (
        "https://air-example-data.s3.amazonaws.com/rayllm-ossci/smolvlm-256m-instruct/"
    )
    for file_name in [
        "added_tokens.json",
        "chat_template.json",
        "config.json",
        "generation_config.json",
        "merges.txt",
        "model.safetensors",
        "preprocessor_config.json",
        "processor_config.json",
        "special_tokens_map.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "vocab.json",
    ]:
        response = requests.get(remote_url + file_name)
        response.raise_for_status()
        (checkpoint_dir / file_name).write_bytes(response.content)
    return str(checkpoint_dir)


@pytest.mark.asyncio
async def test_vllm_engine_start_with_custom_resource_bundle(
    # defined in conftest.py
    model_smolvlm_256m,
):
    """vLLM engine starts with custom resource bundle."""
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
        ),
        placement_group_config={"bundles": [{"GPU": 0.49}]},
        runtime_env=dict(
            env_vars={
                "VLLM_DISABLE_COMPILE_CACHE": "1",
            },
        ),
    )

    pg = placement_group(
        bundles=[{"GPU": 1, "CPU": 1}],
    )

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
            await self.engine.shutdown()

    actor = Actor.remote()
    await actor.start.remote()
    await actor.check_health.remote()
    await actor.shutdown.remote()
    ray.kill(actor)
    ray.util.remove_placement_group(pg)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
