from typing import Any, Dict, Optional, Tuple, Type, TypeVar, Union

from pydantic import Field

from ray.llm._internal.common.base_pydantic import BaseModelExtended

T = TypeVar("T", bound="_StageConfigBase")


class _StageConfigBase(BaseModelExtended):
    enabled: bool = Field(default=True, description="Whether this stage is enabled.")
    # Optional overrides; processor-level defaults still apply
    batch_size: Optional[int] = Field(default=None, description="Rows per batch.")
    concurrency: Optional[Union[int, Tuple[int, int]]] = Field(
        default=None, description="Actor pool size or range for this stage."
    )
    runtime_env: Optional[Dict[str, Any]] = Field(
        default=None, description="Optional runtime env for this stage."
    )
    num_cpus: Optional[float] = Field(
        default=None,
        description="Number of CPUs to reserve for each map worker in this stage.",
    )
    memory: Optional[float] = Field(
        default=None,
        description="Heap memory in bytes to reserve for each map worker in this stage.",
    )


class ChatTemplateStageConfig(_StageConfigBase):
    model: Optional[str] = Field(default=None)
    chat_template: Optional[str] = Field(default=None)
    chat_template_kwargs: Optional[Dict[str, Any]] = Field(default=None)


class TokenizerStageConfig(_StageConfigBase):
    model: Optional[str] = Field(default=None)


class DetokenizeStageConfig(_StageConfigBase):
    model: Optional[str] = Field(default=None)


class PrepareImageStageConfig(_StageConfigBase):
    pass


def resolve_stage_config(
    stage_cfg_value: Union[bool, Dict[str, Any], _StageConfigBase],
    stage_config_cls: Type[T],
    processor_defaults: Optional[Dict[str, Any]] = None,
) -> T:
    """Resolve a stage config value (bool | dict | StageConfig) into a typed StageConfig.

    Args:
        stage_cfg_value: The stage config value (bool, dict, or typed StageConfig).
        stage_config_cls: The StageConfig class to instantiate.
        processor_defaults: Optional dict of processor-level defaults to merge in.
            Expected keys: 'batch_size', 'concurrency', 'runtime_env', 'model_source'.

    Returns:
        Resolved StageConfig instance with defaults merged.
    """
    processor_defaults = processor_defaults or {}

    # If already a typed config, create a copy to avoid mutating the input
    if isinstance(stage_cfg_value, stage_config_cls):
        resolved = stage_config_cls.model_validate(stage_cfg_value.model_dump())
    # If bool, create minimal config with enabled flag
    elif isinstance(stage_cfg_value, bool):
        resolved = stage_config_cls(enabled=stage_cfg_value)
    # If dict, parse it into the config class
    elif isinstance(stage_cfg_value, dict):
        resolved = stage_config_cls(**stage_cfg_value)
    else:
        # Fallback: create enabled=True config
        resolved = stage_config_cls(enabled=True)

    # Merge processor defaults for fields not explicitly set
    if resolved.batch_size is None and "batch_size" in processor_defaults:
        resolved.batch_size = processor_defaults["batch_size"]
    if resolved.concurrency is None and "concurrency" in processor_defaults:
        resolved.concurrency = processor_defaults["concurrency"]
    if resolved.runtime_env is None and "runtime_env" in processor_defaults:
        resolved.runtime_env = processor_defaults["runtime_env"]
    # For model fields, use processor model_source if not set
    if hasattr(resolved, "model") and resolved.model is None:
        if "model_source" in processor_defaults:
            resolved.model = processor_defaults["model_source"]

    return resolved
