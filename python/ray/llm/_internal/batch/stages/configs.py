from typing import Any, Dict, Literal, Optional, Tuple, Type, TypeVar, Union

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
    model_source: Optional[str] = Field(
        default=None, description="Model source/identifier for this stage."
    )
    chat_template: Optional[str] = Field(default=None)
    chat_template_kwargs: Optional[Dict[str, Any]] = Field(default=None)


class TokenizerStageConfig(_StageConfigBase):
    model_source: Optional[str] = Field(
        default=None, description="Model source/identifier for this stage."
    )


class DetokenizeStageConfig(_StageConfigBase):
    model_source: Optional[str] = Field(
        default=None, description="Model source/identifier for this stage."
    )


class PrepareImageStageConfig(_StageConfigBase):
    pass


class PrepareMultimodalStageConfig(_StageConfigBase):
    model_source: Optional[str] = Field(
        default=None,
        description="Name or path of the Hugging Face model to use for the multimodal processor. "
        "This is required to process multimodal data according to a specific model.",
    )
    chat_template_content_format: Optional[Literal["string", "openai"]] = Field(
        default="string",
        description="The content format to use for the chat template. "
        "This is used to format the chat template content according to a specific model.",
    )
    apply_sys_msg_formatting: Optional[bool] = Field(
        default=False,
        description="Whether to apply formatting system messages.",
    )


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
        raise TypeError(
            f"Unsupported type for stage config: {type(stage_cfg_value).__name__}. "
            f"Expected bool, dict, or {stage_config_cls.__name__} instance. "
            f"Got: {stage_cfg_value}"
        )

    # Merge processor defaults for fields not explicitly set
    default_fields = ["batch_size", "concurrency", "runtime_env", "model_source"]
    for field_name in default_fields:
        # Skip if field doesn't exist on this config class (e.g., model_source only on some stages)
        if not hasattr(resolved, field_name):
            continue
        if (
            getattr(resolved, field_name, None) is None
            and field_name in processor_defaults
        ):
            setattr(resolved, field_name, processor_defaults[field_name])

    return resolved
