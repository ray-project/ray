import random
import time
from enum import Enum
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence

import ray
from ray import serve
from ray._common.usage.usage_lib import (
    get_hardware_usages_to_report,
    record_extra_usage_tag,
)
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.observability.telemetry_utils import DEFAULT_GPU_TYPE
from ray.llm._internal.common.utils.lora_utils import get_lora_model_ids
from ray.llm._internal.serve.observability.logging import get_logger

if TYPE_CHECKING:
    from ray.llm._internal.serve.configs.server_models import LLMConfig

LLM_SERVE_TELEMETRY_NAMESPACE = "llm_serve_telemetry"
LLM_SERVE_TELEMETRY_ACTOR_NAME = "llm_serve_telemetry"

logger = get_logger(__name__)


class TelemetryTags(str, Enum):
    """Telemetry tags for LLM SERVE."""

    LLM_SERVE_SERVE_MULTIPLE_MODELS = "LLM_SERVE_SERVE_MULTIPLE_MODELS"
    LLM_SERVE_SERVE_MULTIPLE_APPS = "LLM_SERVE_SERVE_MULTIPLE_APPS"
    LLM_SERVE_JSON_MODE_MODELS = "LLM_SERVE_JSON_MODE_MODELS"
    LLM_SERVE_JSON_MODE_NUM_REPLICAS = "LLM_SERVE_JSON_MODE_NUM_REPLICAS"
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


class TelemetryModel(BaseModelExtended):
    """Telemetry model for LLM Serve."""

    model_architecture: str
    num_replicas: int
    use_json_mode: bool
    use_lora: bool
    initial_num_lora_adapters: int
    use_autoscaling: bool
    min_replicas: int
    max_replicas: int
    tensor_parallel_degree: int
    gpu_type: str
    num_gpus: int


@ray.remote(
    name=LLM_SERVE_TELEMETRY_ACTOR_NAME,
    namespace=LLM_SERVE_TELEMETRY_NAMESPACE,
    num_cpus=0,
    lifetime="detached",
)
class TelemetryAgent:
    """Named Actor to keep the state of all deployed models and record telemetry."""

    def __init__(self):
        self.models: List[TelemetryModel] = []
        self.record_tag_func = record_extra_usage_tag

    def _update_record_tag_func(self, record_tag_func: Callable) -> None:
        """This method is only used in tests to record the telemetry tags to a different
        object than Ray's default `record_extra_usage_tag` function."""
        self.record_tag_func = record_tag_func

    def _reset_models(self):
        """This method is only used in tests to clean up the models list."""
        self.models = []

    def _multiple_models(self) -> str:
        unique_models = {model.model_architecture for model in self.models}
        return "1" if len(unique_models) > 1 else "0"

    @staticmethod
    def _multiple_apps() -> str:
        try:
            serve_status = serve.status()
        except ray.exceptions.ActorDiedError:
            # This only happens in a workspace and when multiple Serve sessions are
            # used. Since telemetry agent is long running, it might be connecting to the
            # previous Serve session, which is already dead. In this case, we need to
            # call `serve.shutdown()` before the telemetry agent can re-connect to the
            # existing Serve session.
            serve.shutdown()
            serve_status = serve.status()
        return "1" if len(serve_status.applications) > 1 else "0"

    def _json_mode_models(self) -> str:
        return ",".join(
            [model.model_architecture for model in self.models if model.use_json_mode]
        )

    def _json_mode_num_deployments(self) -> str:
        return ",".join(
            [str(model.num_replicas) for model in self.models if model.use_json_mode]
        )

    def _lora_base_nodes(self) -> str:
        return ",".join(
            [model.model_architecture for model in self.models if model.use_lora]
        )

    def _lora_initial_num_adaptors(self) -> str:
        return ",".join(
            [
                str(model.initial_num_lora_adapters)
                for model in self.models
                if model.use_lora
            ]
        )

    def _autoscaling_enabled_models(self) -> str:
        return ",".join(
            [model.model_architecture for model in self.models if model.use_autoscaling]
        )

    def _autoscaling_min_replicas(self) -> str:
        return ",".join(
            [str(model.min_replicas) for model in self.models if model.use_autoscaling]
        )

    def _autoscaling_max_replicas(self) -> str:
        return ",".join(
            [str(model.max_replicas) for model in self.models if model.use_autoscaling]
        )

    def _model_architectures(self) -> str:
        return ",".join([model.model_architecture for model in self.models])

    def _tensor_parallel_degree(self) -> str:
        return ",".join([str(model.tensor_parallel_degree) for model in self.models])

    def _num_replicas(self) -> str:
        return ",".join([str(model.num_replicas) for model in self.models])

    def _gpu_type(self) -> str:
        return ",".join([model.gpu_type for model in self.models])

    def _num_gpus(self) -> str:
        return ",".join([str(model.num_gpus) for model in self.models])

    def generate_report(self) -> Dict[str, str]:
        return {
            TelemetryTags.LLM_SERVE_SERVE_MULTIPLE_MODELS: self._multiple_models(),
            TelemetryTags.LLM_SERVE_SERVE_MULTIPLE_APPS: self._multiple_apps(),
            TelemetryTags.LLM_SERVE_JSON_MODE_MODELS: self._json_mode_models(),
            TelemetryTags.LLM_SERVE_JSON_MODE_NUM_REPLICAS: self._json_mode_num_deployments(),
            TelemetryTags.LLM_SERVE_LORA_BASE_MODELS: self._lora_base_nodes(),
            TelemetryTags.LLM_SERVE_INITIAL_NUM_LORA_ADAPTERS: self._lora_initial_num_adaptors(),
            TelemetryTags.LLM_SERVE_AUTOSCALING_ENABLED_MODELS: self._autoscaling_enabled_models(),
            TelemetryTags.LLM_SERVE_AUTOSCALING_MIN_REPLICAS: self._autoscaling_min_replicas(),
            TelemetryTags.LLM_SERVE_AUTOSCALING_MAX_REPLICAS: self._autoscaling_max_replicas(),
            TelemetryTags.LLM_SERVE_MODELS: self._model_architectures(),
            TelemetryTags.LLM_SERVE_TENSOR_PARALLEL_DEGREE: self._tensor_parallel_degree(),
            TelemetryTags.LLM_SERVE_NUM_REPLICAS: self._num_replicas(),
            TelemetryTags.LLM_SERVE_GPU_TYPE: self._gpu_type(),
            TelemetryTags.LLM_SERVE_NUM_GPUS: self._num_gpus(),
        }

    def record(self, model: Optional[TelemetryModel] = None) -> None:
        """Record telemetry model."""
        from ray._common.usage.usage_lib import TagKey

        if model:
            self.models.append(model)

        for key, value in self.generate_report().items():
            try:
                self.record_tag_func(TagKey.Value(key), value)
            except ValueError:
                # If the key doesn't exist in the TagKey enum, skip it.
                continue


def _get_or_create_telemetry_agent() -> TelemetryAgent:
    """Helper to get or create the telemetry agent."""
    try:
        telemetry_agent = ray.get_actor(
            LLM_SERVE_TELEMETRY_ACTOR_NAME, namespace=LLM_SERVE_TELEMETRY_NAMESPACE
        )
    except ValueError:
        from ray._common.constants import HEAD_NODE_RESOURCE_NAME
        from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

        telemetry_agent = TelemetryAgent.options(
            # Ensure the actor is created on the head node.
            resources={HEAD_NODE_RESOURCE_NAME: 0.001},
            # Ensure the actor is not scheduled with the existing placement group.
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None),
        ).remote()

    return telemetry_agent


def _retry_get_telemetry_agent(
    max_retries: int = 5, base_delay: float = 0.1
) -> TelemetryAgent:
    max_retries = 5
    base_delay = 0.1

    telemetry_agent = None
    for attempt in range(max_retries):
        try:
            telemetry_agent = _get_or_create_telemetry_agent()
            return telemetry_agent
        except ValueError as e:
            # Due to race conditions among multiple replicas, we may get:
            #   ValueError: Actor with name 'llm_serve_telemetry' already
            #   exists in the namespace llm_serve_telemetry
            logger.info(
                "Attempt %s/%s to get telemetry agent failed", attempt + 1, max_retries
            )
            if attempt == max_retries - 1:
                raise e

            # Exponential backoff with jitter
            exponential_delay = base_delay * (2**attempt)
            jitter = random.uniform(0, 0.5)
            delay = exponential_delay + jitter
            # Max total wait time is ~3.5 seconds for 5 attempts.
            time.sleep(delay)


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

        Iterate through all the hardware recorded on the cluster and return the first
        ray-compatible accelerator as the GPU type used for the deployment. If not, return
        `UNSPECIFIED` as the default GPU type.
        """
        from ray.llm._internal.serve.configs.server_models import GPUType

        all_accelerator_types = [t.value for t in GPUType]
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
):
    """Push telemetry report for all models."""
    if not all_models:
        return

    for model in all_models:
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

        use_autoscaling = model.deployment_config.get("autoscaling_config") is not None
        num_replicas, min_replicas, max_replicas = 1, 1, 1
        if use_autoscaling:
            from ray.serve.config import AutoscalingConfig

            autoscaling_config = AutoscalingConfig(
                **model.deployment_config["autoscaling_config"]
            )
            num_replicas = (
                autoscaling_config.initial_replicas or autoscaling_config.min_replicas
            )
            min_replicas = autoscaling_config.min_replicas
            max_replicas = autoscaling_config.max_replicas

        engine_config = model.get_engine_config()
        hardware_usage = HardwareUsage(get_hardware_fn)

        telemetry_model = TelemetryModel(
            model_architecture=model.model_architecture,
            num_replicas=num_replicas,
            use_json_mode=True,
            use_lora=use_lora,
            initial_num_lora_adapters=initial_num_lora_adapters,
            use_autoscaling=use_autoscaling,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            tensor_parallel_degree=engine_config.tensor_parallel_degree,
            gpu_type=model.accelerator_type or hardware_usage.infer_gpu_from_hardware(),
            num_gpus=engine_config.num_devices,
        )
        _push_telemetry_report(telemetry_model)
