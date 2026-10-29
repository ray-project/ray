"""Shared utility functions for processor builders."""

from typing import Any, Dict, Optional, Tuple, Union

from ray.data import ActorPoolStrategy
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    TokenizerStageConfig,
    _StageConfigBase,
)


def get_value_or_fallback(value: Any, fallback: Any) -> Any:
    """Return value if not None, otherwise return fallback."""
    return value if value is not None else fallback


def resolve_tokenize_add_special_tokens(
    tokenize_stage_cfg: TokenizerStageConfig,
    chat_template_stage_cfg: ChatTemplateStageConfig,
) -> bool:
    """Resolve whether the tokenize stage adds special tokens such as BOS.

    Chat templates render special tokens such as BOS into the prompt text, so
    tokenizing that text again with special tokens would duplicate them. An
    explicit value on the tokenize stage config always wins.

    Args:
        tokenize_stage_cfg: The resolved tokenize stage config.
        chat_template_stage_cfg: The resolved chat template stage config.

    Returns:
        The explicit ``add_special_tokens`` value if set, otherwise False when
        the chat template stage is enabled and True when it is disabled.
    """
    return get_value_or_fallback(
        tokenize_stage_cfg.add_special_tokens,
        not chat_template_stage_cfg.enabled,
    )


def extract_resource_kwargs(
    runtime_env: Optional[Dict[str, Any]],
    num_cpus: Optional[float],
    memory: Optional[float],
) -> Dict[str, Any]:
    """Extract non-None resource kwargs for map_batches."""
    kwargs = {}
    if runtime_env is not None:
        kwargs["runtime_env"] = runtime_env
    if num_cpus is not None:
        kwargs["num_cpus"] = num_cpus
    if memory is not None:
        kwargs["memory"] = memory
    return kwargs


def normalize_cpu_stage_concurrency(
    concurrency: Optional[Union[int, Tuple[int, int]]]
) -> Dict[str, int]:
    """Normalize concurrency for CPU stages (int -> (1, int) for autoscaling)."""
    if concurrency is None:
        return {"size": 1}  # Default to minimal autoscaling pool
    if isinstance(concurrency, int):
        return {"min_size": 1, "max_size": concurrency}
    return {
        "min_size": concurrency[0],
        "max_size": concurrency[1],
    }


def build_cpu_stage_map_kwargs(
    stage_cfg: _StageConfigBase,
) -> Dict[str, Any]:
    """Build map_batches_kwargs for CPU stages."""
    concurrency = normalize_cpu_stage_concurrency(stage_cfg.concurrency)
    return dict(
        zero_copy_batch=True,
        compute=ActorPoolStrategy(**concurrency),
        batch_size=stage_cfg.batch_size,
        **extract_resource_kwargs(
            stage_cfg.runtime_env,
            stage_cfg.num_cpus,
            stage_cfg.memory,
        ),
    )
