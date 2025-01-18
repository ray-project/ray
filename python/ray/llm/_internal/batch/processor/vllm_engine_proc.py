"""The vLLM engine processor."""

from typing import Any, Dict, Optional

import ray
from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorConfig,
    ProcessorBuilder,
)
from ray.llm._internal.batch.stages import (
    vLLMEngineStage,
    ChatTemplateStage,
    TokenizeStage,
    DetokenizeStage,
)


class vLLMEngineProcessorConfig(ProcessorConfig):
    """The configuration for the vLLM engine processor."""

    # The model name.
    model: str
    # The engine kwargs.
    engine_kwargs: Dict[str, Any]
    # The runtime environment.
    runtime_env: Optional[Dict[str, Any]]
    # The task type.
    task_type: str = "generate"
    # The maximum number of pending requests.
    max_pending_requests: Optional[int] = None
    # Whether to apply chat template.
    apply_chat_template: bool = True
    # Whether to tokenize the input.
    tokenize: bool = True
    # Whether to detokenize the output.
    detokenize: bool = True


def build_vllm_engine_processor(
    config: vLLMEngineProcessorConfig, **kwargs
) -> Processor:
    """Construct a Processor and configure stages.

    Args:
        config: The configuration for the processor.
        **kwargs: The keyword arguments for the processor.

    Returns:
        The constructed processor.
    """
    ray.init(runtime_env=config.runtime_env, ignore_reinit_error=True)

    processor = Processor(config, **kwargs)

    if config.apply_chat_template:
        processor.append_stage(
            ChatTemplateStage(
                fn_constructor_kwargs=dict(
                    model=config.model,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=(1, processor.concurrency),
                    batch_size=config.batch_size,
                ),
            )
        )

    if config.tokenize:
        processor.append_stage(
            TokenizeStage(
                fn_constructor_kwargs=dict(
                    model=config.model,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=(1, processor.concurrency),
                    batch_size=config.batch_size,
                ),
            )
        )

    # Core stage -- the vLLM engine.

    processor.append_stage(
        vLLMEngineStage(
            fn_constructor_kwargs=dict(
                model=config.model,
                engine_kwargs=config.engine_kwargs,
                task_type=config.task_type,
                runtime_env=config.runtime_env,
                max_pending_requests=config.max_pending_requests,
            ),
            map_batches_kwargs=dict(
                zero_copy_batch=True,
                # The number of running replicas. Note that we use a single
                # integer to let Ray Data prepare all replicas before kicking
                # off the processing for now.
                concurrency=processor.concurrency,
                # The number of running batches "per actor" in Ray Core level.
                # This is used to make sure we overlap batches to avoid the tail
                # latency of each batch. This hardcoded value may not be optimal
                # (too small) when the batch size or the batch processing latency
                # is too small, but it should be good enough for batch size >= 64.
                max_concurrency=4,
                accelerator_type=processor.accelerator_type,
            ),
        )
    )

    if config.detokenize:
        processor.append_stage(
            DetokenizeStage(
                fn_constructor_kwargs=dict(
                    model=config.model,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=(1, processor.concurrency),
                    batch_size=config.batch_size,
                ),
            )
        )
    return processor


ProcessorBuilder.register(vLLMEngineProcessorConfig, build_vllm_engine_processor)
