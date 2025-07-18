import dataclasses
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict, Field
from vllm.engine.arg_utils import AsyncEngineArgs

from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.utils.cloud_utils import CloudMirrorConfig
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.configs.constants import (
    ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT,
    ENV_VARS_TO_PROPAGATE,
)
from ray.llm._internal.serve.configs.server_models import (
    GPUType,
    LLMConfig,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.util.placement_group import (
    PlacementGroup,
    get_current_placement_group,
    placement_group,
    placement_group_table,
)


# TODO (Kourosh): Temprary until this abstraction lands in vllm upstream.
# https://github.com/vllm-project/vllm/pull/20206
@dataclass
class FrontendArgs:
    """Mirror of default values for FrontendArgs in vllm."""

    host: Optional[str] = None
    port: int = 8000
    uvicorn_log_level: str = "info"
    disable_uvicorn_access_log: bool = False
    allow_credentials: bool = False
    allowed_origins: list[str] = field(default_factory=lambda: ["*"])
    allowed_methods: list[str] = field(default_factory=lambda: ["*"])
    allowed_headers: list[str] = field(default_factory=lambda: ["*"])
    api_key: Optional[str] = None
    lora_modules: Optional[list[str]] = None
    prompt_adapters: Optional[list[str]] = None
    chat_template: Optional[str] = None
    chat_template_content_format: str = "auto"
    response_role: str = "assistant"
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_ca_certs: Optional[str] = None
    enable_ssl_refresh: bool = False
    ssl_cert_reqs: int = 0
    root_path: Optional[str] = None
    middleware: list[str] = field(default_factory=lambda: [])
    return_tokens_as_token_ids: bool = False
    disable_frontend_multiprocessing: bool = False
    enable_request_id_headers: bool = False
    enable_auto_tool_choice: bool = False
    tool_call_parser: Optional[str] = None
    tool_parser_plugin: str = ""
    log_config_file: Optional[str] = None
    max_log_len: Optional[int] = None
    disable_fastapi_docs: bool = False
    enable_prompt_tokens_details: bool = False
    enable_server_load_tracking: bool = False
    enable_force_include_usage: bool = False
    expand_tools_even_if_tool_choice_none: bool = False


# The key for the kv_transfer_params in the internal metadata.
KV_TRANSFER_PARAMS_KEY = "kv_transfer_params"
vllm = try_import("vllm")
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
    resources_per_bundle: Optional[Dict[str, float]] = Field(
        default=None,
        description="This overrides the vLLM engine worker's default resource configuration, "
        "the number of resources returned by `placement_bundles`.",
    )
    accelerator_type: Optional[GPUType] = Field(
        None,
        description="The type of accelerator to use. This is used to determine the placement group strategy.",
    )
    runtime_env: Optional[Dict[str, Any]] = None
    engine_kwargs: Dict[str, Any] = {}
    frontend_kwargs: Dict[str, Any] = {}

    @property
    def actual_hf_model_id(self) -> str:
        return self.hf_model_id or self.model_id

    @property
    def trust_remote_code(self) -> bool:
        return self.engine_kwargs.get("trust_remote_code", False)

    def get_initialization_kwargs(self) -> dict:
        """
        Get kwargs that will be actually passed to the LLMInitializer
        constructor.
        """
        engine_kwargs = self.engine_kwargs.copy()

        if "model" in engine_kwargs or "served_model_name" in engine_kwargs:
            raise ValueError(
                "model or served_model_name is not allowed in engine_kwargs when using Ray Serve LLM. Please use `model_loading_config` in LLMConfig instead."
            )

        engine_kwargs["model"] = self.actual_hf_model_id
        engine_kwargs["served_model_name"] = [self.model_id]

        if (
            "distributed_executor_backend" in engine_kwargs
            and engine_kwargs["distributed_executor_backend"] != "ray"
        ):
            raise ValueError(
                "distributed_executor_backend != 'ray' is not allowed in engine_kwargs when using Ray Serve LLM Configs."
            )
        else:
            engine_kwargs["distributed_executor_backend"] = "ray"

        if (
            "disable_log_stats" in engine_kwargs
            and not engine_kwargs["disable_log_stats"]
        ):
            logger.warning(
                "disable_log_stats = False is not allowed in engine_kwargs when using Ray Serve LLM Configs. Setting it to True."
            )
        engine_kwargs["disable_log_stats"] = True

        return engine_kwargs

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

        all_engine_kwargs = llm_config.engine_kwargs.copy()
        engine_kwargs = {}
        frontend_kwargs = {}

        # Get field names from dataclasses
        frontend_field_names = {
            field.name for field in dataclasses.fields(FrontendArgs)
        }
        async_engine_field_names = {
            field.name for field in dataclasses.fields(AsyncEngineArgs)
        }

        for key, value in all_engine_kwargs.items():
            if key in frontend_field_names:
                frontend_kwargs[key] = value
            elif key in async_engine_field_names:
                engine_kwargs[key] = value
            else:
                raise ValueError(f"Unknown engine argument: {key}")

        return VLLMEngineConfig(
            model_id=llm_config.model_id,
            hf_model_id=hf_model_id,
            mirror_config=mirror_config,
            resources_per_bundle=llm_config.resources_per_bundle,
            accelerator_type=llm_config.accelerator_type,
            engine_kwargs=engine_kwargs,
            frontend_kwargs=frontend_kwargs,
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
    def num_devices(self) -> int:
        return self.tensor_parallel_degree * self.pipeline_parallel_degree

    @property
    def placement_strategy(self) -> str:
        # If pp <= 1, it's TP so we should make sure all replicas are on the same node.
        if self.pipeline_parallel_degree > 1:
            return "PACK"
        return "STRICT_PACK"

    @property
    def placement_bundles(self) -> List[Dict[str, float]]:
        if self.resources_per_bundle:
            bundle = self.resources_per_bundle
        else:
            bundle = {"GPU": 1}
        if self.accelerator_type:
            bundle[self.ray_accelerator_type()] = 0.001
        bundles = [bundle for _ in range(self.num_devices)]

        return bundles

    @property
    def use_gpu(self) -> bool:
        """
        Returns True if vLLM is configured to use GPU resources.
        """
        if self.resources_per_bundle and self.resources_per_bundle.get("GPU", 0) > 0:
            return True
        if not self.accelerator_type:
            # By default, GPU resources are used
            return True

        return self.accelerator_type in (
            GPUType.NVIDIA_TESLA_V100.value,
            GPUType.NVIDIA_TESLA_P100.value,
            GPUType.NVIDIA_TESLA_T4.value,
            GPUType.NVIDIA_TESLA_P4.value,
            GPUType.NVIDIA_TESLA_K80.value,
            GPUType.NVIDIA_TESLA_A10G.value,
            GPUType.NVIDIA_L4.value,
            GPUType.NVIDIA_L40S.value,
            GPUType.NVIDIA_A100.value,
            GPUType.NVIDIA_H100.value,
            GPUType.NVIDIA_H200.value,
            GPUType.NVIDIA_H20.value,
            GPUType.NVIDIA_A100_40G.value,
            GPUType.NVIDIA_A100_80G.value,
        )

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
