import asyncio
from typing import Optional

import ray
from ray.llm._internal.common.utils.download_utils import (
    download_model_files,
)
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.observability.logging import get_logger

torch = try_import("torch")
transformers = try_import("transformers")

logger = get_logger(__name__)


def initialize_remote_node(llm_config: LLMConfig) -> Optional[str]:

    callback = llm_config.get_or_create_callback()
    engine_config = llm_config.get_engine_config()

    # The engine config is not updated with the result: `resolve_model_path()`
    # reads this node's disk on demand, so the download only has to happen, not
    # be recorded. The return value is used to check that every node agrees.
    return download_model_files(
        model_id=engine_config.cache_id,
        mirror_config=engine_config.mirror_config,
        download_model=callback.ctx.worker_node_download_model,
        download_extra_files=True,
        callback=callback,
    )


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
                num_cpus=0,
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
