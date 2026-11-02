"""The per-task interface for building the SGLang processor."""

from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.llm._internal.batch.stages.base import StatefulStage
from ray.llm._internal.batch.stages.configs import TokenizerStageConfig

if TYPE_CHECKING:
    from ray.llm._internal.batch.processor.sglang_engine_proc import (
        SGLangEngineProcessorConfig,
    )


class _SGLangTask:
    """The per-task part of building the SGLang processor.

    Each task type has one subclass instance, registered in
    `sglang_engine_proc._TASKS`. Subclasses must build the tokenize stage and
    can override the other steps, which do nothing by default. The chat
    template, engine, and detokenize stages are built the same way for every
    task; a task that can't use one of them disables it in `prepare_config`.
    """

    def prepare_config(
        self, config: "SGLangEngineProcessorConfig"
    ) -> "SGLangEngineProcessorConfig":
        """Validate the config and return it with the task's stage defaults.

        This is where a task disables the stages it can't use, for example
        detokenize for a task that generates no tokens.
        """
        return config

    def engine_task_options(
        self, config: "SGLangEngineProcessorConfig", tokenizer_source: str
    ) -> Optional[Dict[str, Any]]:
        """Return the engine stage's `task_options`, or None if the task has none."""
        return None

    def build_tokenize_stage(
        self,
        config: "SGLangEngineProcessorConfig",
        stage_cfg: TokenizerStageConfig,
        task_options: Optional[Dict[str, Any]],
    ) -> StatefulStage:
        """Build the tokenize stage."""
        raise NotImplementedError

    def validate_model(
        self, config: "SGLangEngineProcessorConfig", hf_config: Any
    ) -> None:
        """Check the config against the model's Hugging Face config, if loaded."""
