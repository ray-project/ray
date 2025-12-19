import logging
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

from pydantic import Field, field_validator, model_validator

import ray
from ray.data import Dataset
from ray.data.block import UserDefinedFunction
from ray.llm._internal.batch.stages import (
    StatefulStage,
    wrap_postprocess,
    wrap_preprocess,
)
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.util.annotations import DeveloperAPI, PublicAPI

logger = logging.getLogger(__name__)


# Higher values here are better for prefetching and locality. It's ok for this to be
# fairly high since streaming backpressure prevents us from overloading actors.
DEFAULT_MAX_TASKS_IN_FLIGHT = 4


class ProcessorConfig(BaseModelExtended):
    """The processor configuration."""

    batch_size: int = Field(
        default=32,
        description="Large batch sizes are likely to saturate the compute resources "
        "and could achieve higher throughput. On the other hand, small batch sizes "
        "are more fault-tolerant and could reduce bubbles in the data pipeline. "
        "You can tune the batch size to balance the throughput and fault-tolerance "
        "based on your use case. Defaults to 32.",
    )
    resources_per_bundle: Optional[Dict[str, float]] = Field(
        default=None,
        description="[DEPRECATED] This parameter is deprecated and will be removed in a future version. ",
        deprecated=True,
    )
    accelerator_type: Optional[str] = Field(
        default=None,
        description="The accelerator type used by the LLM stage in a processor. "
        "Default to None, meaning that only the CPU will be used.",
    )
    concurrency: Union[int, Tuple[int, int]] = Field(
        default=1,
        description="The number of workers for data parallelism. Default to 1. "
        "If ``concurrency`` is a ``tuple`` ``(m, n)``, Ray creates an autoscaling "
        "actor pool that scales between ``m`` and ``n`` workers (``1 <= m <= n``). "
        "If ``concurrency`` is an ``int`` ``n``, Ray uses either a fixed pool of ``n`` "
        "workers or an autoscaling pool from ``1`` to ``n`` workers, depending on "
        "the processor and stage.",
    )

    experimental: Dict[str, Any] = Field(
        default_factory=dict,
        description="[Experimental] Experimental configurations."
        "Supported keys:\n"
        "`max_tasks_in_flight_per_actor`: The maximum number of tasks in flight per actor. Default to 4.",
    )

    @field_validator("concurrency")
    def validate_concurrency(
        cls, concurrency: Union[int, Tuple[int, int]]
    ) -> Union[int, Tuple[int, int]]:
        """Validate that `concurrency` is either:
        - a positive int, or
        - a 2-tuple `(min, max)` of positive ints with `min <= max`.
        """

        def require(condition: bool, message: str) -> None:
            if not condition:
                raise ValueError(message)

        if isinstance(concurrency, int):
            require(
                concurrency > 0,
                f"A positive integer for `concurrency` is expected! Got: `{concurrency}`.",
            )
        elif isinstance(concurrency, tuple):
            require(
                all(c > 0 for c in concurrency),
                f"`concurrency` tuple items must be positive integers! Got: `{concurrency}`.",
            )

            min_concurrency, max_concurrency = concurrency
            require(
                min_concurrency <= max_concurrency,
                f"min > max in the concurrency tuple `{concurrency}`!",
            )
        return concurrency

    def get_concurrency(self, autoscaling_enabled: bool = True) -> Tuple[int, int]:
        """Return a normalized `(min, max)` worker range from `self.concurrency`.

        Behavior:
        - If `concurrency` is an int `n`:
          - `autoscaling_enabled` is True  -> return `(1, n)` (autoscaling).
          - `autoscaling_enabled` is False -> return `(n, n)` (fixed-size pool).
        - If `concurrency` is a 2-tuple `(m, n)`, return it unchanged
          (the `autoscaling_enabled` flag is ignored).

        Args:
            autoscaling_enabled: When False, treat an integer `concurrency` as fixed `(n, n)`;
                otherwise treat it as a range `(1, n)`. Defaults to True.

        Returns:
            tuple[int, int]: The allowed worker range `(min, max)`.

        Examples:
            >>> self.concurrency = (2, 4)
            >>> self.get_concurrency()
            (2, 4)
            >>> self.concurrency = 4
            >>> self.get_concurrency()
            (1, 4)
            >>> self.get_concurrency(autoscaling_enabled=False)
            (4, 4)
        """
        if isinstance(self.concurrency, int):
            if autoscaling_enabled:
                return 1, self.concurrency
            else:
                return self.concurrency, self.concurrency
        return self.concurrency

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


class OfflineProcessorConfig(ProcessorConfig):
    """The processor configuration for offline processing."""

    model_source: str = Field(
        description="The model source to use for the offline processing.",
    )
    runtime_env: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The runtime environment to use for the offline processing.",
    )
    max_pending_requests: Optional[int] = Field(
        default=None,
        description="The maximum number of pending requests. If not specified, "
        "will use the default value from the backend engine.",
    )
    max_concurrent_batches: int = Field(
        default=8,
        description="The maximum number of concurrent batches in the engine. "
        "This is to overlap the batch processing to avoid the tail latency of "
        "each batch. The default value may not be optimal when the batch size "
        "or the batch processing latency is too small, but it should be good "
        "enough for batch size >= 32.",
    )
    should_continue_on_error: bool = Field(
        default=False,
        description="If True, continue processing when inference fails for a row "
        "instead of raising an exception. Failed rows will have a non-null "
        "'__inference_error__' column containing the error message, and other "
        "output columns will be None. Error rows bypass postprocess. "
        "If False (default), any inference error will raise an exception.",
    )

    # Processor stage configurations (legacy booleans, will be deprecated).
    apply_chat_template: bool = Field(
        default=True,
        description="[DEPRECATED] Prefer `chat_template_stage`. Whether to apply chat template.",
    )
    chat_template: Optional[str] = Field(
        default=None,
        description="[DEPRECATED] Prefer `chat_template_stage.chat_template`. The chat template to use.",
    )
    tokenize: bool = Field(
        default=True,
        description="[DEPRECATED] Prefer `tokenize_stage`. Whether to tokenize input before engine.",
    )
    detokenize: bool = Field(
        default=True,
        description="[DEPRECATED] Prefer `detokenize_stage`. Whether to detokenize the output.",
    )
    has_image: bool = Field(
        default=False,
        description="[DEPRECATED] Prefer `prepare_multimodal_stage` for processing multimodal data. "
        "Whether the input messages have images.",
    )

    # New nested stage configuration (bool | dict | typed config).
    chat_template_stage: Any = Field(
        default=True,
        description="Chat templating stage config (bool | dict | ChatTemplateStageConfig).",
    )
    tokenize_stage: Any = Field(
        default=True,
        description="Tokenizer stage config (bool | dict | TokenizerStageConfig).",
    )
    detokenize_stage: Any = Field(
        default=True,
        description="Detokenizer stage config (bool | dict | DetokenizeStageConfig).",
    )
    prepare_image_stage: Any = Field(
        default=False,
        description="[DEPRECATED] Prefer `prepare_multimodal_stage` for processing multimodal data. Prepare image stage config (bool | dict | PrepareImageStageConfig).",
    )
    prepare_multimodal_stage: Any = Field(
        default=False,
        description="Prepare multimodal stage config (bool | dict | PrepareMultimodalStageConfig).",
    )

    @model_validator(mode="before")
    def _coerce_legacy_to_stage_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # Only set stage fields if not explicitly provided.
        # Emit deprecation warnings when legacy boolean flags are used.

        # Chat template stage: special case (handles both apply_chat_template and chat_template fields)
        if "chat_template_stage" not in values:
            if "apply_chat_template" in values or "chat_template" in values:
                logger.warning(
                    "The `apply_chat_template` and `chat_template` fields are deprecated. "
                    "Use `chat_template_stage` instead. For example: "
                    "`chat_template_stage=ChatTemplateStageConfig(enabled=True, chat_template='...')` "
                    "or `chat_template_stage={'enabled': True, 'chat_template': '...'}`. "
                    "This will raise an error in a future version."
                )
                enabled_value = values.get("apply_chat_template")
                enabled = enabled_value if enabled_value is not None else True
                stage: Dict[str, Any] = {"enabled": enabled}
                if values.get("chat_template") is not None:
                    stage["chat_template"] = values["chat_template"]
                values["chat_template_stage"] = stage

        # Other stages: simple boolean-to-stage mapping
        stage_mappings = [
            ("tokenize_stage", "tokenize", True, "TokenizerStageConfig"),
            ("detokenize_stage", "detokenize", True, "DetokenizeStageConfig"),
            ("prepare_image_stage", "has_image", False, "PrepareImageStageConfig"),
        ]
        for (
            stage_field,
            legacy_field,
            default_enabled,
            config_class_name,
        ) in stage_mappings:
            if stage_field not in values and legacy_field in values:
                logger.warning(
                    f"The `{legacy_field}` field is deprecated. "
                    f"Use `{stage_field}` instead. For example: "
                    f"`{stage_field}={config_class_name}(enabled=True)` "
                    f"or `{stage_field}={{'enabled': True}}`. "
                    "This will raise an error in a future version."
                )
                legacy_value = values.get(legacy_field)
                enabled = default_enabled if legacy_value is None else legacy_value
                values[stage_field] = {"enabled": enabled}

        return values

    @model_validator(mode="before")
    def _warn_prepare_image_stage_deprecation(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Warn if prepare_image_stage is enabled, recommend prepare_multimodal_stage instead."""
        if "prepare_image_stage" in values:
            prepare_image_stage_value = values.get("prepare_image_stage")
            if prepare_image_stage_value is None:
                is_enabled = False
            elif isinstance(prepare_image_stage_value, bool):
                is_enabled = prepare_image_stage_value
            elif isinstance(prepare_image_stage_value, dict):
                is_enabled = True
            else:
                is_enabled = prepare_image_stage_value.enabled

            if is_enabled:
                logger.warning(
                    "The stage `prepare_image_stage` is deprecated. "
                    "Prefer `prepare_multimodal_stage` instead, which unifies image, audio, "
                    "video, etc. processing with a single stage. For example: "
                    "`prepare_multimodal_stage=PrepareMultimodalStageConfig(enabled=True)` "
                    "or `prepare_multimodal_stage={'enabled': True}`. "
                    "This will raise an error in a future version."
                )

        return values


@PublicAPI(stability="alpha")
class Processor:
    """A processor is composed of a preprocess stage, followed by one or more
    processing stages, and finally a postprocess stage. We use processor as a
    paradigm for processing data using LLMs.

    Args:
        config: The processor config.
        stages: List of processing stages.
        preprocess: An optional lambda function that takes a row (dict) as input
            and returns a preprocessed row (dict). The output row must contain the
            required fields for the following processing stages.
        postprocess: An optional lambda function that takes a row (dict) as input
            and returns a postprocessed row (dict).
        preprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            preprocess stage (e.g., num_cpus, memory, concurrency).
        postprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            postprocess stage (e.g., num_cpus, memory, concurrency).
    """

    # The internal used data column name ("__data"). Your input
    # dataset should not contain this column. If you want to use this column
    # in your input dataset, you have to derive and customize Processor.
    DATA_COLUMN: str = "__data"

    def __init__(
        self,
        config: ProcessorConfig,
        stages: List[StatefulStage],
        preprocess: Optional[UserDefinedFunction] = None,
        postprocess: Optional[UserDefinedFunction] = None,
        preprocess_map_kwargs: Optional[Dict[str, Any]] = None,
        postprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    ):
        self.config = config
        self.preprocess = None
        self.postprocess = None
        self.preprocess_map_kwargs = preprocess_map_kwargs or {}
        self.postprocess_map_kwargs = postprocess_map_kwargs or {}
        self.stages: OrderedDict[str, StatefulStage] = OrderedDict()

        # FIXES: https://github.com/ray-project/ray/issues/53124
        # TODO (Kourosh): Remove this once the issue is fixed
        data_context = ray.data.DataContext.get_current()
        data_context.wait_for_min_actors_s = 600
        # TODO: Remove this when https://github.com/ray-project/ray/issues/53169
        # is fixed.
        data_context._enable_actor_pool_on_exit_hook = True

        # NOTE (Kourosh): If pre/postprocess is not provided, use the identity function.
        # Wrapping is required even if they are identity functions, b/c data_column
        # gets inserted/removed via wrap_preprocess/wrap_postprocess.
        preprocess = preprocess or (lambda row: row)
        postprocess = postprocess or (lambda row: row)

        self.preprocess = wrap_preprocess(
            preprocess,
            self.DATA_COLUMN,
        )

        # When should_continue_on_error is enabled, include __inference_error__ column
        # in all output rows for consistent schema (None for success, message for error).
        include_error_column = getattr(config, "should_continue_on_error", False)
        self.postprocess = wrap_postprocess(
            postprocess,
            self.DATA_COLUMN,
            include_error_column=include_error_column,
        )

        for stage in stages:
            self._append_stage(stage)

    def __call__(self, dataset: Dataset) -> Dataset:
        """Execute the processor:
        preprocess -> stages -> postprocess.
        Note that the dataset won't be materialized during the execution.

        Args:
            dataset: The input dataset.

        Returns:
            The output dataset.
        """
        if self.preprocess is not None:
            dataset = dataset.map(self.preprocess, **self.preprocess_map_kwargs)

        # Apply stages.
        for stage in self.stages.values():
            kwargs = stage.get_dataset_map_batches_kwargs(
                batch_size=self.config.batch_size,
                data_column=self.DATA_COLUMN,
            )
            dataset = dataset.map_batches(stage.fn, **kwargs)

        if self.postprocess is not None:
            dataset = dataset.map(self.postprocess, **self.postprocess_map_kwargs)
        return dataset

    def _append_stage(self, stage: StatefulStage) -> None:
        """Append a stage before postprocess. The stage class name will be used as
        the stage name. If there are multiple stages with the same type, a suffix
        will be added to the stage name to avoid conflicts.

        Args:
            stage: The stage to append.
        """
        stage_name = type(stage).__name__

        # When a processor has multiple stages with the same type,
        # append a index suffix to the stage name to avoid conflicts.
        if stage_name in self.stages:
            num_same_type_stage = len([s for s in self.stages.values() if s is stage])
            stage_name = f"{stage_name}_{num_same_type_stage + 1}"
        self.stages[stage_name] = stage

    def list_stage_names(self) -> List[str]:
        """List the stage names of this processor in order. Preprocess and postprocess
        are not included.

        Returns:
            A list of stage names.
        """
        return list(self.stages.keys())

    def get_stage_by_name(self, name: str) -> StatefulStage:
        """Get a particular stage by its name. If the stage is not found,
        a ValueError will be raised.

        Args:
            name: The stage name.

        Returns:
            The pipeline stage.
        """
        if name in self.stages:
            return self.stages[name]
        raise ValueError(f"Stage {name} not found")

    def log_input_column_names(self):
        """Log.info the input stage and column names of this processor.
        If the input dataset does not contain these columns, you have to
        provide a preprocess function to bridge the gap.
        """
        name, stage = list(self.stages.items())[0]
        expected_input_keys = stage.get_required_input_keys()
        optional_input_keys = stage.get_optional_input_keys()

        message = f"The first stage of the processor is {name}."
        if expected_input_keys:
            message += "\nRequired input columns:\n"
            message += "\n".join(f"\t{k}: {v}" for k, v in expected_input_keys.items())
        if optional_input_keys:
            message += "\nOptional input columns:\n"
            message += "\n".join(f"\t{k}: {v}" for k, v in optional_input_keys.items())

        logger.info(message)


@DeveloperAPI
class ProcessorBuilder:
    """Build a processor based on the configuration."""

    _registry: Dict[str, Callable] = {}

    @classmethod
    def register(cls, config_type: Type[ProcessorConfig], builder: Callable) -> None:
        """A decorator to associate a particular pipeline config
        with its build function.
        """
        type_name = config_type.__name__
        if type_name in cls._registry:
            raise ValueError(f"Processor config type {type_name} already registered.")
        cls._registry[type_name] = builder

    @classmethod
    def clear_registry(cls) -> None:
        """Clear the processor builder registry."""
        cls._registry.clear()

    @classmethod
    def validate_builder_kwargs(cls, builder_kwargs: Optional[Dict[str, Any]]) -> None:
        """Validate builder kwargs for conflicts with reserved keys.

        Args:
            builder_kwargs: Optional additional kwargs to pass to the processor builder
                function.

        Raises:
            ValueError: If builder_kwargs contains reserved keys that conflict with
                explicit arguments.
        """
        if builder_kwargs is not None:
            # Check for conflicts with explicitly passed arguments
            reserved_keys = {
                "preprocess",
                "postprocess",
                "preprocess_map_kwargs",
                "postprocess_map_kwargs",
            }
            conflicting_keys = reserved_keys & builder_kwargs.keys()
            if conflicting_keys:
                raise ValueError(
                    f"builder_kwargs cannot contain {conflicting_keys} as these are "
                    "passed as explicit arguments to build_processor. "
                    "Please pass these directly instead of in builder_kwargs."
                )

    @classmethod
    def build(
        cls,
        config: ProcessorConfig,
        override_stage_config_fn: Optional[Callable] = None,
        **kwargs,
    ) -> Processor:
        """Build a processor.

        Args:
            config: The processor config.
            override_stage_config_fn: Custom stages configurations.
            **kwargs: Additional keyword arguments to pass through to the
                registered builder function. The builder function must accept
                these kwargs in its signature, otherwise a TypeError will be raised.

        Returns:
            The built processor.
        """
        type_name = type(config).__name__
        if type_name not in cls._registry:
            raise ValueError(
                f"Processor config type {type_name} not registered. "
                f"Available types: {cls._registry.keys()}"
            )
        processor = cls._registry[type_name](config, **kwargs)
        if override_stage_config_fn is not None:
            for name, stage in processor.stages.items():
                override_stage_config_fn(name, stage)
        return processor
