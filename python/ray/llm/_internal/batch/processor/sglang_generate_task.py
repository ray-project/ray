"""The generate task of the SGLang processor."""

from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.llm._internal.batch.processor.sglang_task import _SGLangTask
from ray.llm._internal.batch.processor.utils import build_cpu_stage_map_kwargs
from ray.llm._internal.batch.stages.base import StatefulStage
from ray.llm._internal.batch.stages.configs import TokenizerStageConfig

if TYPE_CHECKING:
    from ray.llm._internal.batch.processor.sglang_engine_proc import (
        SGLangEngineProcessorConfig,
    )


class _SGLangGenerateTask(_SGLangTask):
    """Builds the generate task: prompt tokenize stage, then `Engine.async_generate`."""

    def build_tokenize_stage(
        self,
        config: "SGLangEngineProcessorConfig",
        stage_cfg: TokenizerStageConfig,
        task_options: Optional[Dict[str, Any]],
    ) -> StatefulStage:
        from ray.llm._internal.batch.stages import TokenizeStage

        return TokenizeStage(
            fn_constructor_kwargs=dict(
                model=stage_cfg.model_source,
                trust_remote_code=config.engine_kwargs.get("trust_remote_code", False),
            ),
            map_batches_kwargs=build_cpu_stage_map_kwargs(stage_cfg),
        )
