import dataclasses
import os
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict, Field, PrivateAttr, field_validator, model_validator
from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.entrypoints.openai.cli_args import FrontendArgs

from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.placement import PlacementGroupConfig
from ray.llm._internal.common.utils.cloud_utils import CloudMirrorConfig
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.constants import (
    ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT,
    ENV_VARS_TO_PROPAGATE,
)
from ray.llm._internal.serve.core.configs.accelerators import (
    TPU_ACCELERATOR_VALUES,
    AcceleratorBackend,
    CPUAccelerator,
    GPUAccelerator,
    TPUAccelerator,
    _compute_use_gpu,
    _compute_use_tpu,
)
from ray.llm._internal.serve.core.configs.llm_config import (
    AcceleratorType,
    LLMConfig,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.util.placement_group import (
    PlacementGroup,
    get_current_placement_group,
    placement_group_table,
)

# The key for the kv_transfer_params in the internal metadata.
KV_TRANSFER_PARAMS_KEY = "kv_transfer_params"
vllm = try_import("vllm")
logger = get_logger(__name__)

# Executor backend constants
EXECUTOR_BACKEND_RAY = "ray"
EXECUTOR_BACKEND_MP = "mp"


class VLLMEngineConfig(BaseModelExtended):
    model_config = ConfigDict(
        use_enum_values=True,
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
    accelerator_type: Optional[AcceleratorType] = Field(
        None,
        description="The type of accelerator to use. This is used to determine the placement group strategy.",
    )
    topology: Optional[str] = Field(
        default=None,
        description=(
            "The physical topology of the TPU slice. "
            "Required when deploying on multi-host TPU configurations."
        ),
    )
    use_cpu: Optional[bool] = Field(
        default=None,
        description=(
            "Whether to use CPU for model inference. If not set, Ray will try to infer based on the available GPU resources. If set to True the model will run on CPU."
        ),
    )

    placement_group_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Ray placement group configuration for scheduling vLLM engine workers. "
            "Defines resource bundles and placement strategy for multi-node deployments. "
            "Defaults to PACK strategy with automatic bundle generation based on TP/PP sizes."
        ),
    )

    @field_validator("placement_group_config")
    @classmethod
    def validate_placement_group_config(cls, value):
        if value is None:
            return None
        # Validate through PlacementGroupConfig, then dump back to dict
        validated = PlacementGroupConfig(**value)
        return validated.model_dump(exclude_unset=True)

    @model_validator(mode="after")
    def _validate_accelerator_type_with_hardware_mode(self):
        """Validate that accelerator_type is not set when use_gpu or use_tpu resolve to False.

        This catches the case where accelerator_type is silently ignored because
        the configuration resolves to CPU-only mode (via use_cpu=True or
        placement_group_config with no GPUs).
        """
        if self.accelerator_type and not (self.use_gpu or self.use_tpu):
            raise ValueError(
                f"accelerator_type='{self.accelerator_type}' cannot be used with "
                "CPU-only configurations. Either remove accelerator_type, set "
                "use_cpu=False, or ensure placement_group_config bundles include GPUs or TPUs."
            )
        return self

    runtime_env: Optional[Dict[str, Any]] = None
    engine_kwargs: Dict[str, Any] = {}
    frontend_kwargs: Dict[str, Any] = {}

    _tpu_slice_pg_wrapper: Any = PrivateAttr(default=None)
    _accelerator_backend: AcceleratorBackend = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _resolve_accelerator_backend(self):
        """Validates topology requirements and sets up the accelerator backend."""
        requested_topology = self.topology
        if requested_topology:
            if not self.accelerator_type:
                raise ValueError(
                    "`accelerator_type` must be specified when `topology` is set "
                    "so Ray Serve can correctly provision the multi-host slice."
                )
            if isinstance(self.use_cpu, bool) and self.use_cpu:
                raise ValueError("Cannot specify a `topology` when `use_cpu=True`.")

            if self.accelerator_type.value not in TPU_ACCELERATOR_VALUES:
                raise ValueError(
                    f"Multi-host `topology` is currently only supported for TPU deployments. "
                    f"Received accelerator_type: '{self.accelerator_type.value}'"
                )

        if isinstance(self.use_cpu, bool) and self.use_cpu:
            self._accelerator_backend = CPUAccelerator(self)
        elif self.use_tpu:
            self._accelerator_backend = TPUAccelerator(self)
        elif self.use_gpu:
            self._accelerator_backend = GPUAccelerator(self)
        else:
            # Implicit CPU configurations (via placement_group_config) and custom
            # non-GPU/TPU accelerators rely on the CPU backend to pass bundles natively.
            self._accelerator_backend = CPUAccelerator(self)

        return self

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

        # Handle distributed_executor_backend based on GPU/TPU/CPU mode
        if self.use_tpu or self.use_gpu:
            executor_backend = EXECUTOR_BACKEND_RAY
        else:
            executor_backend = EXECUTOR_BACKEND_MP

        if (
            "distributed_executor_backend" in engine_kwargs
            and engine_kwargs["distributed_executor_backend"] != executor_backend
            and executor_backend == EXECUTOR_BACKEND_RAY
        ):
            raise ValueError(
                "distributed_executor_backend != 'ray' is not allowed in engine_kwargs when using Ray Serve LLM Configs."
            )

        engine_kwargs["distributed_executor_backend"] = executor_backend

        if "enable_log_requests" not in engine_kwargs:
            engine_kwargs["enable_log_requests"] = False

        return engine_kwargs

    def get_runtime_env_with_local_env_vars(self) -> dict:
        runtime_env = self.runtime_env or {}
        runtime_env.setdefault("env_vars", {})
        env_vars = runtime_env["env_vars"]

        # Propagate env vars to the runtime env
        for env_var in ENV_VARS_TO_PROPAGATE:
            if env_var in os.environ:
                env_vars[env_var] = os.getenv(env_var)

        if "VLLM_RAY_PER_WORKER_GPUS" not in env_vars:
            fractional_gpu = self._detect_fractional_gpu_from_pg(
                self.placement_group_config
            )
            if fractional_gpu is not None:
                env_vars["VLLM_RAY_PER_WORKER_GPUS"] = str(fractional_gpu)
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

        # placement_group_config is already validated and stored as dict in LLMConfig
        placement_group_config = llm_config.placement_group_config
        return VLLMEngineConfig(
            model_id=llm_config.model_id,
            hf_model_id=hf_model_id,
            mirror_config=mirror_config,
            accelerator_type=llm_config.accelerator_type,
            use_cpu=llm_config.use_cpu,
            topology=llm_config.topology,
            engine_kwargs=engine_kwargs,
            frontend_kwargs=frontend_kwargs,
            runtime_env=llm_config.runtime_env,
            placement_group_config=placement_group_config,
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
        # Use custom strategy if placement_group_config is provided
        if self.placement_group_config:
            return self.placement_group_config.get("strategy", "PACK")
        # Default to PACK (cross-node best-effort placement)
        # DP deployments overridden to STRICT_PACK in Serve config
        return "PACK"

    @property
    def placement_bundles(self) -> List[Dict[str, float]]:
        if self.placement_group_config:
            # Check if bundle_per_worker is specified inside placement_group_config
            bundle_per_worker = self.placement_group_config.get("bundle_per_worker")
            if bundle_per_worker:
                # Expand bundle_per_worker to num_devices bundles
                bundles = []
                for _ in range(self.num_devices):
                    bundle = bundle_per_worker.copy()
                    if self.accelerator_type and (self.use_gpu or self.use_tpu):
                        bundle.setdefault(self.ray_accelerator_type(), 0.001)
                    bundles.append(bundle)
                return bundles

            # Otherwise use explicit bundles list
            bundles = []
            for bundle_dict in self.placement_group_config["bundles"]:
                bundle = bundle_dict.copy()
                if self.accelerator_type and (self.use_gpu or self.use_tpu):
                    # Use setdefault to add accelerator hint WITHOUT overriding explicit user values
                    bundle.setdefault(self.ray_accelerator_type(), 0.001)
                bundles.append(bundle)
            return bundles

        # Default bundles: generated based on accelerator backend.
        return self._accelerator_backend.get_placement_bundles()

    @property
    def use_gpu(self) -> bool:
        """Returns True if vLLM is configured to use GPU resources."""
        return _compute_use_gpu(
            self.use_cpu, self.placement_group_config, self.accelerator_type
        )

    @property
    def use_tpu(self) -> bool:
        """Returns True if vLLM is configured to use TPU resources."""
        return _compute_use_tpu(
            self.use_cpu, self.placement_group_config, self.accelerator_type
        )

    def get_or_create_pg(self) -> PlacementGroup:
        """Gets or a creates a placement group.

        If we are already in a placement group, return the existing placement group.
        Else, create a new placement group based on the scaling config.
        """
        dp_rank = self.engine_kwargs.get("data_parallel_rank", None)
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
            name = "" if dp_rank is None else f"dp_{dp_rank}"

            # Delegate the placement group creation to the active hardware backend.
            pg = self._accelerator_backend.create_placement_group(name)

            logger.info(f"Using new placement group {pg}. {placement_group_table(pg)}")

        return pg

    @staticmethod
    def _detect_fractional_gpu_from_pg(
        placement_group_config: Optional[Dict[str, Any]]
    ) -> Optional[float]:
        if not placement_group_config:
            return None

        # Check bundle_per_worker first
        bundle_per_worker = placement_group_config.get("bundle_per_worker")
        if bundle_per_worker:
            gpu_value = bundle_per_worker.get("GPU", 0)
            if 0 < gpu_value < 1:
                return gpu_value
            return None

        # Fall back to bundles list
        bundles = placement_group_config.get("bundles") or []

        for bundle in bundles:
            if "GPU" not in bundle:
                continue

            gpu_value = bundle["GPU"]
            if gpu_value <= 0 or gpu_value >= 1:
                return None

            return gpu_value

        return None
