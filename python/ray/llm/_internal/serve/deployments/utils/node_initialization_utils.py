import asyncio
import enum
import os
from typing import Any, Dict, NamedTuple, Optional

import ray
from ray.util.placement_group import PlacementGroup

from ray.llm._internal.utils import try_import


from ray.llm._internal.serve.observability.logging import get_logger

from ray.llm._internal.common.utils.cloud_utils import CloudMirrorConfig
from ray.llm._internal.common.utils.download_utils import CloudModelDownloader
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import VLLMEngineConfig
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.deployments.utils.server_utils import make_async

torch = try_import("torch")
transformers = try_import("transformers")

logger = get_logger(__name__)


class NodeModelDownloadable(enum.Enum):
    """Defines which files to download from cloud storage."""

    MODEL_AND_TOKENIZER = enum.auto()
    TOKENIZER_ONLY = enum.auto()
    NONE = enum.auto()

    def __bool__(self):
        return self != NodeModelDownloadable.NONE

    def union(self, other: "NodeModelDownloadable") -> "NodeModelDownloadable":
        """Return a NodeModelDownloadable that is a union of this and the other."""
        if (
            self == NodeModelDownloadable.MODEL_AND_TOKENIZER
            or other == NodeModelDownloadable.MODEL_AND_TOKENIZER
        ):
            return NodeModelDownloadable.MODEL_AND_TOKENIZER

        if (
            self == NodeModelDownloadable.TOKENIZER_ONLY
            or other == NodeModelDownloadable.TOKENIZER_ONLY
        ):
            return NodeModelDownloadable.TOKENIZER_ONLY

        return NodeModelDownloadable.NONE


def _log_download_info(
    *, source: str, download_model: NodeModelDownloadable, download_extra_files: bool
):
    if download_model == NodeModelDownloadable.NONE:
        if download_extra_files:
            logger.info("Downloading extra files from %s", source)
        else:
            logger.info("Not downloading anything from %s", source)
    elif download_model == NodeModelDownloadable.TOKENIZER_ONLY:
        if download_extra_files:
            logger.info("Downloading tokenizer and extra files from %s", source)
        else:
            logger.info("Downloading tokenizer from %s", source)
    elif download_model == NodeModelDownloadable.MODEL_AND_TOKENIZER:
        if download_extra_files:
            logger.info("Downloading model, tokenizer, and extra files from %s", source)
        else:
            logger.info("Downloading model and tokenizer from %s", source)


def download_model_files(
    model_id: Optional[str] = None,
    mirror_config: Optional[CloudMirrorConfig] = None,
    download_model: NodeModelDownloadable = NodeModelDownloadable.MODEL_AND_TOKENIZER,
    download_extra_files: bool = True,
) -> Optional[str]:
    """
    Perform initialization for a node.

    Currently, that means downloading the model from cloud storage.

    Args:
        model_id: The model id.
        mirror_config: Config for downloading model from cloud storage.
        download_model: What parts of the model to download.
        download_extra_files: Whether to download extra files specified in the mirror config.

    Returns path to downloaded model, if any.
    """

    # Create the torch cache kernels directory if it doesn't exist.
    # This is a workaround for a torch issue, where the kernels directory
    # cannot be created by torch if the parent directory doesn't exist.
    torch_cache_home = torch.hub._get_torch_home()
    os.makedirs(os.path.join(torch_cache_home, "kernels"), exist_ok=True)
    model_path_or_id = None

    if model_id is None:
        return model_path_or_id

    if mirror_config is None:
        logger.info("No cloud storage mirror configured")
        return model_path_or_id

    storage_type = mirror_config.storage_type
    source = (
        f"{storage_type.upper()} mirror" if storage_type else "Cloud storage mirror"
    )

    _log_download_info(
        source=source,
        download_model=download_model,
        download_extra_files=download_extra_files,
    )

    downloader = CloudModelDownloader(model_id, mirror_config)

    if download_model != NodeModelDownloadable.NONE:
        model_path_or_id = downloader.get_model(
            tokenizer_only=download_model == NodeModelDownloadable.TOKENIZER_ONLY
        )

    if download_extra_files:
        downloader.get_extra_files()

    return model_path_or_id


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
            engine_config,
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

    llm_config.apply_checkpoint_info(
        engine_config.actual_hf_model_id,
        trust_remote_code=engine_config.trust_remote_code,
    )

    return InitializeNodeOutput(
        placement_group=pg, runtime_env=runtime_env, extra_init_kwargs=extra_init_kwargs
    )


@make_async
def _initialize_local_node(
    engine_config: VLLMEngineConfig,
    *,
    download_model: NodeModelDownloadable,
    download_extra_files: bool,
):
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

    _ = transformers.AutoTokenizer.from_pretrained(
        engine_config.actual_hf_model_id,
        trust_remote_code=engine_config.trust_remote_code,
    )
