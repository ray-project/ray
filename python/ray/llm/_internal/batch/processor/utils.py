"""Shared utility functions for processor builders."""

from typing import Any, Dict, Optional, Tuple, Union

from ray.llm._internal.batch.stages.configs import _StageConfigBase


def get_value_or_fallback(value: Any, fallback: Any) -> Any:
    """Return value if not None, otherwise return fallback."""
    return value if value is not None else fallback


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
) -> Tuple[int, int]:
    """Normalize concurrency for CPU stages (int -> (1, int) for autoscaling)."""
    if concurrency is None:
        return (1, 1)  # Default to minimal autoscaling pool
    if isinstance(concurrency, int):
        return (1, concurrency)
    return concurrency


def build_cpu_stage_map_kwargs(
    stage_cfg: _StageConfigBase,
) -> Dict[str, Any]:
    """Build map_batches_kwargs for CPU stages."""
    concurrency = normalize_cpu_stage_concurrency(stage_cfg.concurrency)
    return dict(
        zero_copy_batch=True,
        concurrency=concurrency,
        batch_size=stage_cfg.batch_size,
        **extract_resource_kwargs(
            stage_cfg.runtime_env,
            stage_cfg.num_cpus,
            stage_cfg.memory,
        ),
    )
