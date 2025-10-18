import asyncio
import os
from typing import Optional

import ray
from ray.llm._internal.common.utils.download_utils import (
    download_model_files,
)
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig, LLMEngine
from ray.llm._internal.serve.observability.logging import get_logger

torch = try_import("torch")
transformers = try_import("transformers")

logger = get_logger(__name__)


def initialize_remote_node(llm_config: LLMConfig) -> Optional[str]:

    callback = llm_config.get_or_create_callback()
    engine_config = llm_config.get_engine_config()

    local_path = download_model_files(
        model_id=engine_config.actual_hf_model_id,
        mirror_config=engine_config.mirror_config,
        download_model=callback.ctx.worker_node_download_model,
        download_extra_files=True,
        callback=callback,
    )

    # Validate that the binary exists
    if local_path and local_path != engine_config.actual_hf_model_id:
        engine_config.hf_model_id = local_path

    # Download the tokenizer if it isn't a local file path
    if not isinstance(local_path, str) or not os.path.exists(local_path):
        logger.info(f"Downloading the tokenizer for {engine_config.actual_hf_model_id}")

    if llm_config.llm_engine == LLMEngine.vLLM:
        from vllm.transformers_utils.tokenizer import get_tokenizer

        _ = get_tokenizer(
            engine_config.actual_hf_model_id,
            tokenizer_mode=engine_config.engine_kwargs.get("tokenizer_mode", None),
            trust_remote_code=engine_config.trust_remote_code,
        )
    else:
        _ = transformers.AutoTokenizer.from_pretrained(
            engine_config.actual_hf_model_id,
            trust_remote_code=engine_config.trust_remote_code,
        )

    return local_path


async def initialize_node(llm_config: LLMConfig):
    """Implements node initialization for LLM engines.

    Downloads model, tokenizer, and extra files as necessary.
    """
    # Get callback instance (if configured) with context information
    callback = llm_config.get_or_create_callback()
    ctx = callback.ctx
    pg_table = ray.util.placement_group_table(ctx.placement_group)

    node_set = set(pg_table["bundles_to_node_id"].values())
    download_tasks = []
    for node_id in node_set:
        node_affinity_strategy = (
            ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=node_id,
                soft=False,
            )
        )
        download_tasks.append(
            ray.remote(initialize_remote_node).options(
                num_cpus=1,
                scheduling_strategy=node_affinity_strategy,
                runtime_env=ctx.runtime_env,
            )
        )

    logger.info("Running tasks to download model files on worker nodes")
    paths = await asyncio.gather(
        *[download_task.remote(llm_config) for download_task in download_tasks]
    )

    # assume that all paths are the same
    assert paths, "No paths returned from download_model_files"
    assert (
        len(set(paths)) == 1
    ), "Paths returned from download_model_files are not the same"
    llm_config.get_engine_config().hf_model_id = paths[0]
