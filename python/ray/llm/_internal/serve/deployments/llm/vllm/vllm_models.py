import os
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pydantic import ConfigDict, Field
from ray import serve
from ray.util.placement_group import (
    PlacementGroup,
    get_current_placement_group,
    placement_group,
    placement_group_table,
)
from ray.llm._internal.utils import try_import

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.utils.cloud_utils import CloudMirrorConfig
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    GenerationRequest,
    GPUType,
    LLMConfig,
    SamplingParams,
)
from ray.llm._internal.serve.configs.constants import (
    ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT,
    ENV_VARS_TO_PROPAGATE,
)


vllm = try_import("vllm")

if TYPE_CHECKING:
    from vllm.lora.request import LoRARequest

logger = get_logger(__name__)


class VLLMEngineConfig(BaseModelExtended):
    model_config = ConfigDict(
        use_enum_values=True,
        extra="forbid",
    )

    model_id: str = Field(
        description="The identifier for the model. This is the id that will be used to query the model.",
    )
    hf_model_id: Optional[str] = Field(
        None, description="The Hugging Face model identifier."
    )
    mirror_config: Optional[CloudMirrorConfig] = Field(
        None,
        description="Configuration for cloud storage mirror. This is for where the weights are downloaded from.",
    )
    accelerator_type: Optional[GPUType] = Field(
        None,
        description="The type of accelerator to use. This is used to determine the placement group strategy.",
    )
    runtime_env: Optional[Dict[str, Any]] = None
    engine_kwargs: Dict[str, Any] = {}

    @property
    def actual_hf_model_id(self) -> str:
        return self.hf_model_id or self.model_id

    @property
    def trust_remote_code(self) -> bool:
        return self.engine_kwargs.get("trust_remote_code", False)

    @property
    def sampling_params_model(self):
        return VLLMSamplingParams

    def get_initialization_kwargs(self) -> dict:
        """
        Get kwargs that will be actually passed to the LLMInitializer
        constructor.
        """
        return self.engine_kwargs.copy()

    def get_runtime_env_with_local_env_vars(self) -> dict:
        runtime_env = self.runtime_env or {}
        runtime_env.setdefault("env_vars", {})

        # Propagate env vars to the runtime env
        for env_var in ENV_VARS_TO_PROPAGATE:
            if env_var in os.environ:
                runtime_env["env_vars"][env_var] = os.getenv(env_var)
        return runtime_env

    @classmethod
    def from_llm_config(cls, llm_config: LLMConfig) -> "VLLMEngineConfig":
        """Converts the LLMConfig to a VLLMEngineConfig."""
        # Set up the model downloading configuration.
        hf_model_id, mirror_config = None, None
        if llm_config.model_loading_config.model_source is None:
            hf_model_id = llm_config.model_id
        elif isinstance(llm_config.model_loading_config.model_source, str):
            hf_model_id = llm_config.model_loading_config.model_source
        else:
            # If it's a CloudMirrorConfig (or subtype)
            mirror_config = llm_config.model_loading_config.model_source

        return VLLMEngineConfig(
            model_id=llm_config.model_id,
            hf_model_id=hf_model_id,
            mirror_config=mirror_config,
            accelerator_type=llm_config.accelerator_type,
            engine_kwargs=llm_config.engine_kwargs,
            runtime_env=llm_config.runtime_env,
        )

    def ray_accelerator_type(self) -> str:
        """Converts the accelerator type to the Ray Core format."""
        return f"accelerator_type:{self.accelerator_type}"

    @property
    def tensor_parallel_degree(self) -> int:
        return self.engine_kwargs.get("tensor_parallel_size", 1)

    @property
    def pipeline_parallel_degree(self) -> int:
        return self.engine_kwargs.get("pipeline_parallel_size", 1)

    @property
    def num_gpu_workers(self) -> int:
        return self.tensor_parallel_degree * self.pipeline_parallel_degree

    @property
    def placement_strategy(self) -> str:
        # If pp <= 1, it's TP so we should make sure all replicas are on the same node.
        if self.pipeline_parallel_degree > 1:
            return "PACK"
        return "STRICT_PACK"

    @property
    def placement_bundles(self) -> List[Dict[str, float]]:
        bundle = {"GPU": 1}
        if self.accelerator_type:
            bundle[self.ray_accelerator_type()] = 0.001
        bundles = [bundle for _ in range(self.num_gpu_workers)]

        return bundles

    def get_or_create_pg(self) -> PlacementGroup:
        """Gets or a creates a placement group.

        If we are already in a placement group, return the existing placement group.
        Else, create a new placement group based on the scaling config.
        """
        pg = get_current_placement_group()
        if pg:
            logger.debug(
                "Using existing placement group %s, details: %s",
                pg.id,
                placement_group_table(pg),
            )
        else:
            if not ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT:
                raise RuntimeError(
                    "Creating new placement groups is not allowed. "
                    "Change RAYLLM_ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT "
                    "if this is not intended."
                )
            pg = placement_group(
                self.placement_bundles, strategy=self.placement_strategy
            )

            logger.info(f"Using new placement group {pg}. {placement_group_table(pg)}")
        return pg


class VLLMSamplingParams(SamplingParams):
    """
    Args:
        top_k: The number of highest probability vocabulary tokens to keep for top-k-filtering.
        seed: Seed for deterministic sampling with temperature>0.
    """

    _ignored_fields = {"best_of", "n", "logit_bias"}

    top_k: Optional[int] = None
    seed: Optional[int] = None


class VLLMGenerationRequest(GenerationRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sampling_params: VLLMSamplingParams
    multi_modal_data: Optional[Dict[str, Any]] = None
    serve_request_context: Optional[serve.context._RequestContext] = None
    disk_multiplex_config: Optional[DiskMultiplexConfig] = None

    @property
    def lora_request(self) -> "LoRARequest":

        disk_vllm_config = self.disk_multiplex_config
        if not disk_vllm_config:
            return None
        else:
            return vllm.lora.request.LoRARequest(
                lora_name=disk_vllm_config.model_id,
                lora_int_id=disk_vllm_config.lora_assigned_int_id,
                lora_local_path=disk_vllm_config.local_path,
                long_lora_max_len=disk_vllm_config.max_total_tokens,
            )
