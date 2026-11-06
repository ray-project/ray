"""The base class for SGLang batch tasks."""

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Tuple, Type

from pydantic import BaseModel

from ray.llm._internal.batch.processor.utils import build_cpu_stage_map_kwargs
from ray.llm._internal.batch.stages.base import StatefulStage
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    DetokenizeStageConfig,
    TokenizerStageConfig,
    resolve_stage_config,
)

if TYPE_CHECKING:
    from ray.llm._internal.batch.processor.sglang_engine_proc import (
        SGLangEngineProcessorConfig,
    )

logger = logging.getLogger(__name__)

_STAGE_CONFIG_CLS = {
    "chat_template_stage": ChatTemplateStageConfig,
    "tokenize_stage": TokenizerStageConfig,
    "detokenize_stage": DetokenizeStageConfig,
}


class _SGLangTask:
    """A task the SGLang processor can run, such as generate.

    A task declares its input columns and config rules as class attributes,
    and implements `prepare_request` and `run`. The class methods run on the
    driver when the processor is built; instances run in the engine stage.
    """

    # Input columns the task requires, and their descriptions.
    required_input_keys: ClassVar[Dict[str, str]] = {}
    # Input columns the task can use, and their descriptions.
    optional_input_keys: ClassVar[Dict[str, str]] = {}
    # Column with per-row parameters, passed to `prepare_request` as `params`
    # and added to the output as the `params` column.
    params_column: ClassVar[Optional[str]] = None
    # Stages the task can't use. They're turned off, and enabling one raises.
    unsupported_stages: ClassVar[Tuple[str, ...]] = ()
    # Engine kwargs the task requires. They override the user's values.
    forced_engine_kwargs: ClassVar[Dict[str, Any]] = {}
    # Validates `task_kwargs`. If None, the task takes no `task_kwargs`.
    options_model: ClassVar[Optional[Type[BaseModel]]] = None

    @classmethod
    def prepare_config(
        cls, config: "SGLangEngineProcessorConfig"
    ) -> "SGLangEngineProcessorConfig":
        """Apply the task's rules to the config and return the updated copy."""
        if cls.options_model is not None:
            task_kwargs = cls.options_model(**config.task_kwargs).model_dump()
        elif config.task_kwargs:
            raise ValueError(
                f"task_type={config.task_type!r} takes no task_kwargs, got "
                f"{sorted(config.task_kwargs)}."
            )
        else:
            task_kwargs = {}

        update: Dict[str, Any] = {"task_kwargs": task_kwargs}
        for stage in cls.unsupported_stages:
            if (
                stage in config.model_fields_set
                and resolve_stage_config(
                    getattr(config, stage), _STAGE_CONFIG_CLS[stage]
                ).enabled
            ):
                raise ValueError(
                    f"task_type={config.task_type!r} doesn't support {stage}."
                )
            update[stage] = False

        engine_kwargs = dict(config.engine_kwargs)
        for key, value in cls.forced_engine_kwargs.items():
            if key in engine_kwargs and engine_kwargs[key] != value:
                logger.warning(
                    "Overriding engine_kwargs[%r]=%r with %r, which "
                    "task_type=%r requires.",
                    key,
                    engine_kwargs[key],
                    value,
                    config.task_type,
                )
            engine_kwargs[key] = value
        update["engine_kwargs"] = engine_kwargs

        return config.model_copy(update=update)

    @classmethod
    def build_tokenize_stage(
        cls, config: "SGLangEngineProcessorConfig", stage_cfg: TokenizerStageConfig
    ) -> StatefulStage:
        """Build the tokenize stage. By default, it tokenizes the prompt."""
        from ray.llm._internal.batch.stages import TokenizeStage

        return TokenizeStage(
            fn_constructor_kwargs=dict(
                model=stage_cfg.model_source,
                trust_remote_code=config.engine_kwargs.get("trust_remote_code", False),
            ),
            map_batches_kwargs=build_cpu_stage_map_kwargs(stage_cfg),
        )

    def __init__(self, engine_kwargs: Dict[str, Any], **task_kwargs: Any):
        """Set up the task in the engine stage.

        Args:
            engine_kwargs: The kwargs the `sglang.Engine` is created with.
            **task_kwargs: The validated `task_kwargs` from the config.
        """
        self.engine_kwargs = engine_kwargs
        self.task_kwargs = task_kwargs

    def prepare_request(
        self, row: Dict[str, Any], params: Optional[Dict[str, Any]]
    ) -> Any:
        """Build the engine request for one row.

        Args:
            row: The input row.
            params: The row's `params_column` value, or None.

        Returns:
            The request that `run` takes.
        """
        raise NotImplementedError

    async def run(self, engine: Any, request: Any) -> Dict[str, Any]:
        """Run one request on the engine and return its output columns."""
        raise NotImplementedError
