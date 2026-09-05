import dataclasses
import os
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict, Field, PrivateAttr, field_validator, model_validator
from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.entrypoints.openai.cli_args import FrontendArgs

from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.placement import PlacementGroupConfig
from ray.llm._internal.common.utils.cloud_utils import CloudMirrorConfig, is_remote_path
from ray.llm._internal.common.utils.download_utils import (
    STREAMING_LOAD_FORMATS,
    STREAMING_URI_SCHEMES,
    get_model_location_on_disk,
)
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.constants import (
    ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT,
    ENV_VARS_TO_PROPAGATE,
)
from ray.llm._internal.serve.core.configs.accelerators import (
    AcceleratorBackend,
    AnyAcceleratorConfig,
    CPUAccelerator,
    CPUConfig,
    GPUAccelerator,
    TPUAccelerator,
    TPUConfig,
    format_ray_accelerator_resource,
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
    model_source: Optional[str] = Field(
        None,
        description=(
            "Where the weights come from: a HuggingFace repo id, a local path, "
            "or a remote URI. Set once from the user's config and never "
            "rewritten; call `resolve_model_path()` for the path to hand to "
            "the engine."
        ),
    )
    mirror_config: Optional[CloudMirrorConfig] = Field(
        None,
        description="Configuration for cloud storage mirror. This is for where the weights are downloaded from.",
    )
    accelerator_type: Optional[AcceleratorType] = Field(
        None,
        description="The type of accelerator to use. This is used to determine the placement group strategy.",
    )
    accelerator_config: Optional[AnyAcceleratorConfig] = Field(
        default=None,
        description=(
            "Hardware-specific configuration parameters for the chosen accelerator. "
            "The expected schema is dynamically typed based on the 'kind' discriminator."
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

    runtime_env: Optional[Dict[str, Any]] = None
    engine_kwargs: Dict[str, Any] = {}
    frontend_kwargs: Dict[str, Any] = {}

    _accelerator: AcceleratorBackend = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _build_accelerator(self):
        """Instantiates the accelerator backend based on the resolved config."""
        cfg = self.accelerator_config

        if self.accelerator_type and isinstance(cfg, CPUConfig):
            raise ValueError(
                f"accelerator_type='{self.accelerator_type}' cannot be used with "
                "CPU-only configurations. Either remove accelerator_type, or provide an accelerator_config."
            )

        # LLMConfig has already resolved and validated accelerator_config
        if isinstance(cfg, TPUConfig):
            self._accelerator = TPUAccelerator(cfg)
        elif isinstance(cfg, CPUConfig):
            self._accelerator = CPUAccelerator()
        else:
            # Default to GPU if it's GPUConfig or isn't set
            self._accelerator = GPUAccelerator()
        return self

    @model_validator(mode="after")
    def _validate_streaming_scheme(self):
        """Reject a streaming load_format aimed at a scheme it cannot read.

        Ray downloads from more schemes than the streamers can stream from.
        Without this check the URI reaches vLLM, which reports it as a missing
        HuggingFace repo -- an error that quotes the URI but not the problem.
        """
        load_format = self.engine_kwargs.get("load_format")
        if load_format not in STREAMING_LOAD_FORMATS:
            return self

        source = (
            self.mirror_config.bucket_uri
            if self.mirror_config is not None
            else self.model_source
        )
        if not source or not is_remote_path(source):
            return self

        if not source.startswith(STREAMING_URI_SCHEMES):
            raise ValueError(
                f"load_format={load_format!r} streams the weights directly out "
                f"of object storage, but model_source {source!r} uses a scheme "
                f"its readers do not support (supported: "
                f"{', '.join(STREAMING_URI_SCHEMES)}). Either point "
                f"model_source at a supported scheme, or drop load_format so "
                f"Ray downloads the weights before the engine starts."
            )
        return self

    @property
    def accelerator(self) -> AcceleratorBackend:
        return self._accelerator

    @property
    def cache_id(self) -> str:
        """Identifier naming this model's local cache directory and download lock.

        A mirrored model is cached under `model_id` rather than under its source
        URI, because a URI leaks its scheme and slashes into the directory name
        (`models--s3:----bucket--...`).
        """
        if self.mirror_config is not None:
            return self.model_id
        return self.model_source or self.model_id

    def resolve_model_path(self) -> str:
        """The path to hand the engine as `model`, resolved against this node.

        This is a pure read. It never writes back to the config, so every caller
        on every node gets an answer that matches that node's disk and there is
        no ordering requirement between resolving and downloading.

        For a mirrored model that means the local snapshot once the weights are
        downloaded, and the source URI until then. Streaming load formats depend
        on the latter: they deliberately skip the download
        (`NodeModelDownloadable.NONE`) so the engine can read the weights
        straight out of object storage.
        """
        if self.mirror_config is None:
            # Not mirrored: a HuggingFace repo id or a local path, either of
            # which the engine consumes directly (and caches itself, for HF).
            return self.model_source or self.model_id

        local_path = get_model_location_on_disk(self.cache_id)
        if local_path != self.cache_id:
            return local_path

        return self.mirror_config.bucket_uri or self.model_source or self.model_id

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

        engine_kwargs["model"] = self.resolve_model_path()
        engine_kwargs["served_model_name"] = [self.model_id]

        # Handle distributed_executor_backend based on backend type
        if isinstance(self.accelerator, CPUAccelerator):
            executor_backend = EXECUTOR_BACKEND_MP
        else:
            executor_backend = EXECUTOR_BACKEND_RAY

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
        #
        # `model_source` records where the weights live and is never rewritten.
        # `mirror_config` records that they have to be copied down from cloud
        # storage first, which is also what makes `cache_id` fall back to
        # `model_id` so the cache directory is not named after a URI.
        source = llm_config.model_loading_config.model_source
        model_source, mirror_config = None, None
        if source is None:
            model_source = llm_config.model_id
        elif isinstance(source, str):
            model_source = source
            if is_remote_path(source):
                mirror_config = CloudMirrorConfig(bucket_uri=source)
        else:
            # If it's a CloudMirrorConfig (or subtype)
            mirror_config = source
            model_source = source.bucket_uri

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
            model_source=model_source,
            mirror_config=mirror_config,
            accelerator_type=llm_config.accelerator_type,
            accelerator_config=llm_config.accelerator_config,
            engine_kwargs=engine_kwargs,
            frontend_kwargs=frontend_kwargs,
            runtime_env=llm_config.runtime_env,
            placement_group_config=placement_group_config,
        )

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
            bundle_per_worker = self.placement_group_config.get("bundle_per_worker")

            if bundle_per_worker is not None:
                # Expand bundle_per_worker to num_devices bundles
                bundles = []
                for _ in range(self.num_devices):
                    bundle = bundle_per_worker.copy()
                    if self.accelerator_type:
                        res_key = format_ray_accelerator_resource(self.accelerator_type)
                        bundle.setdefault(res_key, 0.001)
                    bundles.append(bundle)
                return bundles

            # Otherwise use explicit bundles list
            bundles = []
            explicit_bundles = self.placement_group_config.get("bundles") or []
            for bundle_dict in explicit_bundles:
                bundle = bundle_dict.copy()
                if self.accelerator_type:
                    # Use setdefault to add accelerator hint WITHOUT overriding explicit user values
                    res_key = format_ray_accelerator_resource(self.accelerator_type)
                    bundle.setdefault(res_key, 0.001)
                bundles.append(bundle)
            return bundles

        # Default bundles based on the accelerator backend.
        return self.accelerator.default_bundles(
            num_devices=self.num_devices, accelerator_type_str=self.accelerator_type
        )

    def get_or_create_pg(self) -> PlacementGroup:
        """Gets or creates a placement group.

        If we are already in a placement group, return the existing placement group.
        Else, delegate PG creation to the accelerator backend.
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

            pg = self.accelerator.create_placement_group(
                bundles=self.placement_bundles,
                strategy=self.placement_strategy,
                name=name,
                accelerator_type_str=self.accelerator_type,
            )

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
