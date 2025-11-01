from typing import Any, Dict, Optional, Tuple, Union

from pydantic import Field

from ray.llm._internal.common.base_pydantic import BaseModelExtended


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
