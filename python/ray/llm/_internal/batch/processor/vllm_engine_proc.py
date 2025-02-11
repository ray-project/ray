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
    PrepareImageStage,
    TokenizeStage,
    DetokenizeStage,
)


class vLLMEngineProcessorConfig(ProcessorConfig):
    """The configuration for the vLLM engine processor."""

    # The model name.
    model: str
    # The engine kwargs.
    engine_kwargs: Dict[str, Any]
    # The task type.
    task_type: str = "generate"
    # The runtime environment.
    runtime_env: Optional[Dict[str, Any]] = None
    # The maximum number of pending requests.
    max_pending_requests: Optional[int] = None
    # Whether to apply chat template.
    apply_chat_template: bool = True
    # Whether to tokenize the input.
    tokenize: bool = True
    # Whether to detokenize the output.
    detokenize: bool = True
    # Whether the input messages have images.
    has_image: bool = False


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

    stages = []

    if config.has_image:
        stages.append(
            PrepareImageStage(
                fn_constructor_kwargs={},
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=(1, config.concurrency),
                    batch_size=config.batch_size,
                ),
            )
        )
    if config.apply_chat_template:
        stages.append(
            ChatTemplateStage(
                fn_constructor_kwargs=dict(
                    model=config.model,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=(1, config.concurrency),
                    batch_size=config.batch_size,
                ),
            )
        )

    if config.tokenize:
        stages.append(
            TokenizeStage(
                fn_constructor_kwargs=dict(
                    model=config.model,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=(1, config.concurrency),
                    batch_size=config.batch_size,
                ),
            )
        )

    # Core stage -- the vLLM engine.

    stages.append(
        vLLMEngineStage(
            fn_constructor_kwargs=dict(
                model=config.model,
                engine_kwargs=config.engine_kwargs,
                task_type=config.task_type,
                max_pending_requests=config.max_pending_requests,
            ),
            map_batches_kwargs=dict(
                zero_copy_batch=True,
                # The number of running replicas. Note that we use a single
                # integer to let Ray Data prepare all replicas before kicking
                # off the processing for now.
                concurrency=config.concurrency,
                # The number of running batches "per actor" in Ray Core level.
                # This is used to make sure we overlap batches to avoid the tail
                # latency of each batch. This hardcoded value may not be optimal
                # (too small) when the batch size or the batch processing latency
                # is too small, but it should be good enough for batch size >= 64.
                max_concurrency=4,
                accelerator_type=config.accelerator_type,
            ),
        )
    )

    if config.detokenize:
        stages.append(
            DetokenizeStage(
                fn_constructor_kwargs=dict(
                    model=config.model,
                ),
                map_batches_kwargs=dict(
                    zero_copy_batch=True,
                    concurrency=(1, config.concurrency),
                    batch_size=config.batch_size,
                ),
            )
        )

    processor = Processor(config, stages, **kwargs)
    return processor


ProcessorBuilder.register(vLLMEngineProcessorConfig, build_vllm_engine_processor)
