import asyncio
import hashlib
import json
import random
import time
from enum import Enum
from typing import TYPE_CHECKING, Callable, Dict, Optional, Sequence

import ray
from ray import serve
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray._common.usage.usage_lib import (
    get_hardware_usages_to_report,
    record_extra_usage_tag,
)
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.observability.telemetry_utils import DEFAULT_GPU_TYPE
from ray.llm._internal.common.utils.lora_utils import get_lora_model_ids
from ray.llm._internal.serve.observability.logging import get_logger
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
    from ray.llm._internal.serve.core.protocol import LLMServerProtocol

LLM_SERVE_TELEMETRY_NAMESPACE = "llm_serve_telemetry"
LLM_SERVE_TELEMETRY_ACTOR_NAME = "llm_serve_telemetry"

logger = get_logger(__name__)


class TelemetryTags(str, Enum):
    """Telemetry tags for LLM SERVE."""

    LLM_SERVE_SERVE_MULTIPLE_MODELS = "LLM_SERVE_SERVE_MULTIPLE_MODELS"
    LLM_SERVE_SERVE_MULTIPLE_APPS = "LLM_SERVE_SERVE_MULTIPLE_APPS"
    LLM_SERVE_LORA_BASE_MODELS = "LLM_SERVE_LORA_BASE_MODELS"
    LLM_SERVE_INITIAL_NUM_LORA_ADAPTERS = "LLM_SERVE_INITIAL_NUM_LORA_ADAPTERS"
    LLM_SERVE_AUTOSCALING_ENABLED_MODELS = "LLM_SERVE_AUTOSCALING_ENABLED_MODELS"
    LLM_SERVE_AUTOSCALING_MIN_REPLICAS = "LLM_SERVE_AUTOSCALING_MIN_REPLICAS"
    LLM_SERVE_AUTOSCALING_MAX_REPLICAS = "LLM_SERVE_AUTOSCALING_MAX_REPLICAS"
    LLM_SERVE_TENSOR_PARALLEL_DEGREE = "LLM_SERVE_TENSOR_PARALLEL_DEGREE"
    LLM_SERVE_NUM_REPLICAS = "LLM_SERVE_NUM_REPLICAS"
    LLM_SERVE_MODELS = "LLM_SERVE_MODELS"
    LLM_SERVE_GPU_TYPE = "LLM_SERVE_GPU_TYPE"
    LLM_SERVE_NUM_GPUS = "LLM_SERVE_NUM_GPUS"
    LLM_SERVE_DEPLOY_OUTCOME = "LLM_SERVE_DEPLOY_OUTCOME"
    LLM_SERVE_SERVING_PATTERN = "LLM_SERVE_SERVING_PATTERN"
    LLM_SERVE_ENGINE_CONFIG = "LLM_SERVE_ENGINE_CONFIG"


class TelemetryModel(BaseModelExtended):
    """Telemetry model for LLM Serve.

    ``model_id_hash`` is the dedup identity used by the telemetry agent and is
    never recorded as a tag value. It is a hash of the model id so the raw model
    name never reaches the head-node actor.
    """

    model_id_hash: str
    model_architecture: str
    num_replicas: int
    use_lora: bool
    initial_num_lora_adapters: int
    use_autoscaling: bool
    min_replicas: int
    max_replicas: int
    tensor_parallel_degree: int
    gpu_type: str
    num_gpus: int
    deploy_outcome: str = "success"
    serving_pattern: str = "default"
    quantization: str = "none"
    dtype: str = "auto"
    max_model_len: int = 0
    prefix_caching: str = "default"
    chunked_prefill: str = "default"
    kv_cache_dtype: str = "auto"
    speculative_decoding: str = "off"
    enforce_eager: str = "default"
    pipeline_parallel_degree: int = 1
    accelerator_kind: str = "unspecified"
    log_engine_metrics: str = "default"
    distributed_executor_backend: str = "default"
    load_format: str = "auto"
    multimodal: str = "0"


@ray.remote(
    name=LLM_SERVE_TELEMETRY_ACTOR_NAME,
    namespace=LLM_SERVE_TELEMETRY_NAMESPACE,
    num_cpus=0,
    lifetime="detached",
)
class TelemetryAgent:
    """Named Actor to keep the state of all deployed models and record telemetry."""

    def __init__(self):
        # Keyed by model_id_hash so repeated reports from replicas/restarts of
        # the same model overwrite rather than accumulate.
        self.models: Dict[str, TelemetryModel] = {}
        self.record_tag_func = record_extra_usage_tag

    def _update_record_tag_func(self, record_tag_func: Callable) -> None:
        """This method is only used in tests to record the telemetry tags to a different
        object than Ray's default `record_extra_usage_tag` function."""
        self.record_tag_func = record_tag_func

    def _reset_models(self):
        """This method is only used in tests to clean up the models list."""
        self.models = {}

    def _multiple_models(self) -> str:
        unique_models = {model.model_architecture for model in self.models.values()}
        return "1" if len(unique_models) > 1 else "0"

    @staticmethod
    def _multiple_apps() -> str:
        try:
            try:
                serve_status = serve.status()
            except ray.exceptions.ActorDiedError:
                # In a workspace with multiple Serve sessions, the long-running
                # telemetry agent may still be connected to a previous, now-dead
                # session. Shut down so we can reconnect to the live one.
                serve.shutdown()
                serve_status = serve.status()
            return "1" if len(serve_status.applications) > 1 else "0"
        except Exception:
            # Telemetry must never fail; fall back to "not multiple".
            logger.debug("Failed to query serve.status() for telemetry", exc_info=True)
            return "0"

    def _lora_base_nodes(self) -> str:
        return ",".join(
            [
                model.model_architecture
                for model in self.models.values()
                if model.use_lora
            ]
        )

    def _lora_initial_num_adaptors(self) -> str:
        return ",".join(
            [
                str(model.initial_num_lora_adapters)
                for model in self.models.values()
                if model.use_lora
            ]
        )

    def _autoscaling_enabled_models(self) -> str:
        return ",".join(
            [
                model.model_architecture
                for model in self.models.values()
                if model.use_autoscaling
            ]
        )

    def _autoscaling_min_replicas(self) -> str:
        return ",".join(
            [
                str(model.min_replicas)
                for model in self.models.values()
                if model.use_autoscaling
            ]
        )

    def _autoscaling_max_replicas(self) -> str:
        return ",".join(
            [
                str(model.max_replicas)
                for model in self.models.values()
                if model.use_autoscaling
            ]
        )

    def _join(self, attr: str) -> str:
        """Comma joined attribute across models, index aligned with LLM_SERVE_MODELS."""
        return ",".join(str(getattr(model, attr)) for model in self.models.values())

    def _engine_configs(self) -> str:
        """JSON array of per-model engine facts, index aligned with LLM_SERVE_MODELS."""
        return json.dumps(
            [
                {
                    "quantization": model.quantization,
                    "dtype": model.dtype,
                    "max_model_len": model.max_model_len,
                    "prefix_caching": model.prefix_caching,
                    "chunked_prefill": model.chunked_prefill,
                    "kv_cache_dtype": model.kv_cache_dtype,
                    "speculative_decoding": model.speculative_decoding,
                    "enforce_eager": model.enforce_eager,
                    "pipeline_parallel": model.pipeline_parallel_degree,
                    "accelerator_kind": model.accelerator_kind,
                    "log_engine_metrics": model.log_engine_metrics,
                    "distributed_executor_backend": model.distributed_executor_backend,
                    "load_format": model.load_format,
                    "multimodal": model.multimodal,
                }
                for model in self.models.values()
            ],
            separators=(",", ":"),
        )

    def generate_report(self) -> Dict[str, str]:
        return {
            TelemetryTags.LLM_SERVE_SERVE_MULTIPLE_MODELS: self._multiple_models(),
            TelemetryTags.LLM_SERVE_SERVE_MULTIPLE_APPS: self._multiple_apps(),
            TelemetryTags.LLM_SERVE_LORA_BASE_MODELS: self._lora_base_nodes(),
            TelemetryTags.LLM_SERVE_INITIAL_NUM_LORA_ADAPTERS: self._lora_initial_num_adaptors(),
            TelemetryTags.LLM_SERVE_AUTOSCALING_ENABLED_MODELS: self._autoscaling_enabled_models(),
            TelemetryTags.LLM_SERVE_AUTOSCALING_MIN_REPLICAS: self._autoscaling_min_replicas(),
            TelemetryTags.LLM_SERVE_AUTOSCALING_MAX_REPLICAS: self._autoscaling_max_replicas(),
            TelemetryTags.LLM_SERVE_MODELS: self._join("model_architecture"),
            TelemetryTags.LLM_SERVE_TENSOR_PARALLEL_DEGREE: self._join(
                "tensor_parallel_degree"
            ),
            TelemetryTags.LLM_SERVE_NUM_REPLICAS: self._join("num_replicas"),
            TelemetryTags.LLM_SERVE_GPU_TYPE: self._join("gpu_type"),
            TelemetryTags.LLM_SERVE_NUM_GPUS: self._join("num_gpus"),
            TelemetryTags.LLM_SERVE_DEPLOY_OUTCOME: self._join("deploy_outcome"),
            TelemetryTags.LLM_SERVE_SERVING_PATTERN: self._join("serving_pattern"),
            TelemetryTags.LLM_SERVE_ENGINE_CONFIG: self._engine_configs(),
        }

    def record(self, model: Optional[TelemetryModel] = None) -> None:
        """Record telemetry model."""
        from ray._common.usage.usage_lib import TagKey

        if model:
            self.models[model.model_id_hash] = model

        for key, value in self.generate_report().items():
            try:
                self.record_tag_func(TagKey.Value(key), value)
            except ValueError:
                # If the key doesn't exist in the TagKey enum, skip it.
                continue


def _get_or_create_telemetry_agent() -> TelemetryAgent:
    """Get or create the detached telemetry agent.

    ``get_if_exists`` makes creation atomic, so concurrent replicas converge on a
    single actor without racing on the name.
    """
    return TelemetryAgent.options(
        name=LLM_SERVE_TELEMETRY_ACTOR_NAME,
        namespace=LLM_SERVE_TELEMETRY_NAMESPACE,
        get_if_exists=True,
        # Ensure the actor is created on the head node.
        resources={HEAD_NODE_RESOURCE_NAME: 0.001},
        # Ensure the actor is not scheduled with the existing placement group.
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None),
    ).remote()


def _retry_get_telemetry_agent(
    max_retries: int = 5, base_delay: float = 0.1
) -> TelemetryAgent:
    """Get-or-create the telemetry agent, retrying transient failures."""
    for attempt in range(max_retries):
        try:
            return _get_or_create_telemetry_agent()
        except Exception as e:
            logger.info(
                "Attempt %s/%s to get telemetry agent failed", attempt + 1, max_retries
            )
            if attempt == max_retries - 1:
                raise e
            # Exponential backoff with jitter; ~3.5s total over 5 attempts.
            time.sleep(base_delay * (2**attempt) + random.uniform(0, 0.5))


def _push_telemetry_report(model: Optional[TelemetryModel] = None) -> None:
    """Push telemetry report for a model."""
    telemetry_agent = _retry_get_telemetry_agent()
    assert telemetry_agent is not None
    ray.get(telemetry_agent.record.remote(model))


class HardwareUsage:
    """Hardware usage class to report telemetry."""

    def __init__(self, get_hardware_fn: Callable = get_hardware_usages_to_report):
        self._get_hardware_fn = get_hardware_fn

    def infer_gpu_from_hardware(self) -> str:
        """Infer the GPU type from the hardware when the accelerator type on llm config is
        not specified.
        """
        from ray.llm._internal.serve.core.configs.accelerators import AcceleratorType

        all_accelerator_types = [t.value for t in AcceleratorType]
        gcs_client = ray.experimental.internal_kv.internal_kv_get_gcs_client()
        hardwares = self._get_hardware_fn(gcs_client)
        for hardware in hardwares:
            if hardware in all_accelerator_types:
                return hardware

        return DEFAULT_GPU_TYPE


def push_telemetry_report_for_all_models(
    all_models: Optional[Sequence["LLMConfig"]] = None,
    get_lora_model_func: Callable = get_lora_model_ids,
    get_hardware_fn: Callable = get_hardware_usages_to_report,
    deploy_outcome: str = "success",
    serving_pattern: str = "default",
):
    """Push a telemetry report for each model. Never raises."""
    if not all_models:
        return

    for model in all_models:
        # Telemetry must never break the caller (e.g. engine start).
        try:
            _push_model_telemetry(
                model,
                get_lora_model_func,
                get_hardware_fn,
                deploy_outcome=deploy_outcome,
                serving_pattern=serving_pattern,
            )
        except Exception:
            logger.exception(
                "Failed to push telemetry for model %s",
                getattr(model, "model_id", "<unknown>"),
            )


def _flag(value: Optional[bool]) -> str:
    """Tri-state for an optional engine flag: on/off, or default when unset."""
    if value is None:
        return "default"
    return "on" if value else "off"


def serving_pattern(server: "LLMServerProtocol") -> str:
    """Classify the serving pattern from the server class hierarchy.

    Uses MRO class names to avoid importing the DP/PD subclasses (which import
    LLMServer, so importing them here would create a cycle).
    """
    names = {cls.__name__ for cls in type(server).__mro__}
    is_dp = "DPServer" in names
    is_pd = bool(names & {"PDPrefillServer", "PDDecodeServer"})
    if is_dp and is_pd:
        return "dp_pd"
    if is_pd:
        return "pd"
    if is_dp:
        return "dp"
    return "default"


def classify_start_failure(exc: BaseException) -> str:
    """Map an engine-start failure to a coarse, non-identifying category.

    Classify by exception type, which is stable across engine versions. The
    message is never inspected or recorded, so a failure we can't type-identify
    becomes "other" rather than risk a wrong label from brittle string matching.
    """
    if isinstance(exc, asyncio.TimeoutError):
        return "engine_start_timeout"
    if isinstance(exc, ImportError):  # ModuleNotFoundError is a subclass.
        return "import_error"
    if isinstance(exc, MemoryError) or "outofmemory" in type(exc).__name__.lower():
        return "oom"  # torch.cuda.OutOfMemoryError, MemoryError, etc.
    if isinstance(exc, ValueError):
        return "invalid_config"
    return "other"


def _push_model_telemetry(
    model: "LLMConfig",
    get_lora_model_func: Callable,
    get_hardware_fn: Callable,
    deploy_outcome: str = "success",
    serving_pattern: str = "default",
) -> None:
    use_lora = (
        model.lora_config is not None
        and model.lora_config.dynamic_lora_loading_path is not None
    )
    initial_num_lora_adapters = 0
    if use_lora:
        lora_model_ids = get_lora_model_func(
            dynamic_lora_loading_path=model.lora_config.dynamic_lora_loading_path,
            base_model_id=model.model_id,
        )
        initial_num_lora_adapters = len(lora_model_ids)

    deployment_config = model.deployment_config
    autoscaling_config = deployment_config.get("autoscaling_config")
    if deployment_config.get("num_replicas") == "auto":
        # "auto" resolves to an autoscaling config; mirror LLMConfig validation
        # since the stored deployment_config keeps the literal "auto".
        from ray.serve._private.config import handle_num_replicas_auto

        _, autoscaling_config = handle_num_replicas_auto(
            deployment_config.get("max_ongoing_requests"), autoscaling_config
        )

    use_autoscaling = autoscaling_config is not None
    if use_autoscaling:
        from ray.serve.config import AutoscalingConfig

        if isinstance(autoscaling_config, dict):
            autoscaling_config = AutoscalingConfig(**autoscaling_config)
        num_replicas = (
            autoscaling_config.initial_replicas or autoscaling_config.min_replicas
        )
        min_replicas = autoscaling_config.min_replicas
        max_replicas = autoscaling_config.max_replicas
    else:
        # Fixed replica count; honor the configured value (including 0),
        # defaulting to 1 only when unset.
        num_replicas = deployment_config.get("num_replicas")
        if num_replicas is None:
            num_replicas = 1
        min_replicas = max_replicas = num_replicas

    engine_config = model.get_engine_config()
    hardware_usage = HardwareUsage(get_hardware_fn)

    telemetry_model = TelemetryModel(
        # Hash so the cleartext model name (possibly proprietary) never reaches
        # the head-node actor; deterministic across replicas/restarts so dedup holds.
        model_id_hash=hashlib.sha256(model.model_id.encode("utf-8")).hexdigest(),
        model_architecture=model.model_architecture,
        num_replicas=num_replicas,
        use_lora=use_lora,
        initial_num_lora_adapters=initial_num_lora_adapters,
        use_autoscaling=use_autoscaling,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        tensor_parallel_degree=engine_config.tensor_parallel_degree,
        gpu_type=model.accelerator_type or hardware_usage.infer_gpu_from_hardware(),
        num_gpus=engine_config.num_devices,
        deploy_outcome=deploy_outcome,
        serving_pattern=serving_pattern,
        # Non-identifying engine facts.
        quantization=model.engine_kwargs.get("quantization") or "none",
        dtype=str(model.engine_kwargs.get("dtype") or "auto"),
        max_model_len=int(model.engine_kwargs.get("max_model_len") or 0),
        prefix_caching=_flag(model.engine_kwargs.get("enable_prefix_caching")),
        chunked_prefill=_flag(model.engine_kwargs.get("enable_chunked_prefill")),
        kv_cache_dtype=str(model.engine_kwargs.get("kv_cache_dtype") or "auto"),
        speculative_decoding="on"
        if model.engine_kwargs.get("speculative_config")
        or model.engine_kwargs.get("speculative_model")
        else "off",
        enforce_eager=_flag(model.engine_kwargs.get("enforce_eager")),
        pipeline_parallel_degree=engine_config.pipeline_parallel_degree,
        accelerator_kind=getattr(model.accelerator_config, "kind", None)
        or "unspecified",
        log_engine_metrics=_flag(model.log_engine_metrics),
        distributed_executor_backend=str(
            model.engine_kwargs.get("distributed_executor_backend") or "default"
        ),
        load_format=str(model.engine_kwargs.get("load_format") or "auto"),
        multimodal="1" if model.supports_vision else "0",
    )
    _push_telemetry_report(telemetry_model)
