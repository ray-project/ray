import asyncio
import os
from typing import Any, Dict, NamedTuple

import ray
from ray.llm._internal.common.utils.download_utils import (
    NodeModelDownloadable,
    download_model_files,
)
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.configs.server_models import LLMConfig, LLMEngine
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import VLLMEngineConfig
from ray.llm._internal.serve.deployments.utils.server_utils import make_async
from ray.llm._internal.serve.observability.logging import get_logger
from ray.util.placement_group import PlacementGroup

torch = try_import("torch")
transformers = try_import("transformers")

logger = get_logger(__name__)


async def initialize_worker_nodes(
    llm_config: LLMConfig,
    *,
    placement_group: PlacementGroup,
    runtime_env: Dict[str, Any],
    download_model: NodeModelDownloadable,
    download_extra_files: bool,
):
    """Runs the download tasks across all the nodes in the placement groups.

    To this we obtain the nodes that the placement groups are spread across.
    Then we create a node affinity scheduling strategy for each node and
    run the download_model_files task for each node in a separate ray.remote call.
    This ensures that we call download_model_files once per node all in parallel.
    """
    engine_config = VLLMEngineConfig.from_llm_config(llm_config)
    pg_table = ray.util.placement_group_table(placement_group)

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
            ray.remote(download_model_files).options(
                num_cpus=1,
                scheduling_strategy=node_affinity_strategy,
                runtime_env=runtime_env,
            )
        )

    logger.info("Running tasks to download model files on worker nodes")
    await asyncio.gather(
        *[
            download_task.remote(
                engine_config.actual_hf_model_id,
                engine_config.mirror_config,
                download_model=download_model,
                download_extra_files=download_extra_files,
            )
            for download_task in download_tasks
        ]
    )


class InitializeNodeOutput(NamedTuple):
    placement_group: PlacementGroup
    runtime_env: Dict[str, Any]
    extra_init_kwargs: Dict[str, Any]


async def initialize_node(llm_config: LLMConfig) -> InitializeNodeOutput:
    """Implements node initialization for LLM engines.

    Downloads model, tokenizer, and extra files as necessary.

    If the placement strategy is STRICT_PACK, all of the initialization will be run locally
    (as all of the workers must be colocated with this process). Else, the initialization
    will be run across the placement group bundles.
    """
    local_node_download_model = NodeModelDownloadable.TOKENIZER_ONLY
    worker_node_download_model = NodeModelDownloadable.MODEL_AND_TOKENIZER
    extra_init_kwargs = {}

    engine_config = llm_config.get_engine_config()
    assert engine_config is not None
    pg = engine_config.get_or_create_pg()
    runtime_env = engine_config.get_runtime_env_with_local_env_vars()

    if engine_config.placement_strategy == "STRICT_PACK":
        # If the placement strategy is STRICT_PACK, we know that all the
        # workers run on the same node as the engine. Therefore, we can run
        # all initialization steps directly instead of in tasks in the PG.
        # This removes the task launching overhead reducing the initialization
        # time.
        local_node_download_model = local_node_download_model.union(
            worker_node_download_model
        )

        await _initialize_local_node(
            llm_config,
            download_model=local_node_download_model,
            download_extra_files=True,
        )
    else:
        await initialize_worker_nodes(
            llm_config,
            placement_group=pg,
            runtime_env=runtime_env,
            download_model=worker_node_download_model,
            download_extra_files=True,
        )

    return InitializeNodeOutput(
        placement_group=pg, runtime_env=runtime_env, extra_init_kwargs=extra_init_kwargs
    )


@make_async
def _initialize_local_node(
    llm_config: LLMConfig,
    *,
    download_model: NodeModelDownloadable,
    download_extra_files: bool,
):
    engine_config = llm_config.get_engine_config()
    local_path = download_model_files(
        model_id=engine_config.actual_hf_model_id,
        mirror_config=engine_config.mirror_config,
        download_model=download_model,
        download_extra_files=download_extra_files,
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
