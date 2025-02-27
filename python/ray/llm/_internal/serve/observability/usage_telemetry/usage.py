from enum import Enum
from typing import Callable, Dict, List, Optional, Sequence, TYPE_CHECKING

import ray

# TODO (genesu): remove dependency on botocore
from botocore.exceptions import ClientError
from ray import serve

from ray._private.usage.usage_lib import record_extra_usage_tag

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.deployments.llm.multiplex.utils import get_lora_model_ids
from ray.llm._internal.serve.configs.base import BaseModelExtended

if TYPE_CHECKING:
    from ray.llm._internal.serve.configs.server_models import LLMConfig

RAYLLM_TELEMETRY_NAMESPACE = "rayllm_telemetry"
RAYLLM_TELEMETRY_ACTOR_NAME = "rayllm_telemetry"

logger = get_logger(__name__)
DEFAULT_GPU_TYPE = "UNSPECIFIED"


class TelemetryTags(str, Enum):
    """Telemetry tags for RayLLM."""

    RAYLLM_VERSION = "RAYLLM_VERSION"
    RAYLLM_COMMIT = "RAYLLM_COMMIT"
    RAYLLM_SERVE_MULTIPLE_MODELS = "RAYLLM_SERVE_MULTIPLE_MODELS"
    RAYLLM_SERVE_MULTIPLE_APPS = "RAYLLM_SERVE_MULTIPLE_APPS"
    RAYLLM_JSON_MODE_MODELS = "RAYLLM_JSON_MODE_MODELS"
    RAYLLM_JSON_MODE_NUM_REPLICAS = "RAYLLM_JSON_MODE_NUM_REPLICAS"
    RAYLLM_LORA_BASE_MODELS = "RAYLLM_LORA_BASE_MODELS"
    RAYLLM_INITIAL_NUM_LORA_ADAPTERS = "RAYLLM_INITIAL_NUM_LORA_ADAPTERS"
    RAYLLM_AUTOSCALING_ENABLED_MODELS = "RAYLLM_AUTOSCALING_ENABLED_MODELS"
    RAYLLM_AUTOSCALING_MIN_REPLICAS = "RAYLLM_AUTOSCALING_MIN_REPLICAS"
    RAYLLM_AUTOSCALING_MAX_REPLICAS = "RAYLLM_AUTOSCALING_MAX_REPLICAS"
    RAYLLM_TENSOR_PARALLEL_DEGREE = "RAYLLM_TENSOR_PARALLEL_DEGREE"
    RAYLLM_NUM_REPLICAS = "RAYLLM_NUM_REPLICAS"
    RAYLLM_MODELS = "RAYLLM_MODELS"
    RAYLLM_GPU_TYPE = "RAYLLM_GPU_TYPE"
    RAYLLM_NUM_GPUS = "RAYLLM_NUM_GPUS"


class TelemetryModel(BaseModelExtended):
    """Telemetry model for RayLLM."""

    model_id: str
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
    name=RAYLLM_TELEMETRY_ACTOR_NAME,
    namespace=RAYLLM_TELEMETRY_NAMESPACE,
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
        unique_models = {model.model_id for model in self.models}
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
            [model.model_id for model in self.models if model.use_json_mode]
        )

    def _json_mode_num_deployments(self) -> str:
        return ",".join(
            [str(model.num_replicas) for model in self.models if model.use_json_mode]
        )

    def _lora_base_nodes(self) -> str:
        return ",".join([model.model_id for model in self.models if model.use_lora])

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
            [model.model_id for model in self.models if model.use_autoscaling]
        )

    def _autoscaling_min_replicas(self) -> str:
        return ",".join(
            [str(model.min_replicas) for model in self.models if model.use_autoscaling]
        )

    def _autoscaling_max_replicas(self) -> str:
        return ",".join(
            [str(model.max_replicas) for model in self.models if model.use_autoscaling]
        )

    def _model_ids(self) -> str:
        return ",".join([model.model_id for model in self.models])

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
            TelemetryTags.RAYLLM_VERSION: ray.__version__,
            TelemetryTags.RAYLLM_COMMIT: ray.__commit__,
            TelemetryTags.RAYLLM_SERVE_MULTIPLE_MODELS: self._multiple_models(),
            TelemetryTags.RAYLLM_SERVE_MULTIPLE_APPS: self._multiple_apps(),
            TelemetryTags.RAYLLM_JSON_MODE_MODELS: self._json_mode_models(),
            TelemetryTags.RAYLLM_JSON_MODE_NUM_REPLICAS: self._json_mode_num_deployments(),
            TelemetryTags.RAYLLM_LORA_BASE_MODELS: self._lora_base_nodes(),
            TelemetryTags.RAYLLM_INITIAL_NUM_LORA_ADAPTERS: self._lora_initial_num_adaptors(),
            TelemetryTags.RAYLLM_AUTOSCALING_ENABLED_MODELS: self._autoscaling_enabled_models(),
            TelemetryTags.RAYLLM_AUTOSCALING_MIN_REPLICAS: self._autoscaling_min_replicas(),
            TelemetryTags.RAYLLM_AUTOSCALING_MAX_REPLICAS: self._autoscaling_max_replicas(),
            TelemetryTags.RAYLLM_MODELS: self._model_ids(),
            TelemetryTags.RAYLLM_TENSOR_PARALLEL_DEGREE: self._tensor_parallel_degree(),
            TelemetryTags.RAYLLM_NUM_REPLICAS: self._num_replicas(),
            TelemetryTags.RAYLLM_GPU_TYPE: self._gpu_type(),
            TelemetryTags.RAYLLM_NUM_GPUS: self._num_gpus(),
        }

    def record(self, model: Optional[TelemetryModel] = None) -> None:
        """Record telemetry model."""
        from ray._private.usage.usage_lib import TagKey

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
            RAYLLM_TELEMETRY_ACTOR_NAME, namespace=RAYLLM_TELEMETRY_NAMESPACE
        )
    except ValueError:
        telemetry_agent = TelemetryAgent.remote()

    return telemetry_agent


def _push_telemetry_report(model: Optional[TelemetryModel] = None) -> None:
    """Push telemetry report for a model."""
    telemetry_agent = _get_or_create_telemetry_agent()
    ray.get(telemetry_agent.record.remote(model))


def push_telemetry_report_for_all_models(
    all_models: Optional[Sequence["LLMConfig"]] = None,
    get_lora_model_func: Callable = get_lora_model_ids,
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
            # This try-except block is used to handle the case where the Lora model IDs
            # cannot be fetched. In such cases, the telemetry report will be pushed with
            # 0 initial Lora adapters.
            try:
                lora_model_ids = get_lora_model_func(
                    dynamic_lora_loading_path=model.lora_config.dynamic_lora_loading_path,
                    base_model_id=model.model_id,
                )
                initial_num_lora_adapters = len(lora_model_ids)
            except ClientError as e:
                logger.error(
                    f"Failed to get Lora model IDs for model {model.model_id}: {e}"
                )
        use_autoscaling = model.deployment_config.get("autoscaling_config") is not None
        num_replicas, min_replicas, max_replicas = 1, 1, 1
        if use_autoscaling:
            from ray.serve.config import AutoscalingConfig

            autoscaling_config = AutoscalingConfig(
                **model.deployment_config["autoscaling_config"]
            )
            num_replicas = autoscaling_config.initial_replicas
            min_replicas = autoscaling_config.min_replicas
            max_replicas = autoscaling_config.max_replicas

        engine_config = model.get_engine_config()

        telemetry_model = TelemetryModel(
            model_id=model.model_id,
            num_replicas=num_replicas,
            use_json_mode=True,
            use_lora=use_lora,
            initial_num_lora_adapters=initial_num_lora_adapters,
            use_autoscaling=use_autoscaling,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            tensor_parallel_degree=engine_config.tensor_parallel_degree,
            gpu_type=model.accelerator_type or DEFAULT_GPU_TYPE,
            num_gpus=engine_config.num_gpu_workers,
        )
        _push_telemetry_report(telemetry_model)
