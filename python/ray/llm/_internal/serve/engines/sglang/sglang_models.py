"""SGLang engine configuration models for Ray Serve LLM."""

import dataclasses
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field, model_validator

from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)
sglang = try_import("sglang")


class SGLangEngineConfig(BaseModelExtended):
    """Configuration model for SGLang engine.

    Validates engine_kwargs against SGLang ServerArgs and provides
    WideEP-specific validation.
    """

    model_config = ConfigDict(
        use_enum_values=True,
    )

    model_id: str = Field(
        description="The identifier for the model.",
    )
    hf_model_id: Optional[str] = Field(
        None, description="The Hugging Face model identifier."
    )
    engine_kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description="Engine kwargs to pass to SGLang Engine.",
    )

    @model_validator(mode="after")
    def _validate_engine_kwargs(self):
        """Validate engine_kwargs against SGLang ServerArgs."""
        if sglang is None:
            logger.warning(
                "SGLang is not installed. Skipping engine_kwargs validation."
            )
            return self

        # Get valid ServerArgs field names
        try:
            from sglang.srt.server_args import ServerArgs

            server_args_fields = {f.name for f in dataclasses.fields(ServerArgs)}
        except ImportError:
            logger.warning("Could not import ServerArgs for validation.")
            return self

        # Validate that all engine_kwargs are valid ServerArgs fields
        for key in self.engine_kwargs:
            if key not in server_args_fields and key != "placement_group":
                logger.warning(
                    f"engine_kwargs contains unknown field '{key}' not in ServerArgs."
                )

        return self

    @property
    def tp_size(self) -> int:
        """Tensor parallel size."""
        return self.engine_kwargs.get("tp_size", 1)

    @property
    def pp_size(self) -> int:
        """Pipeline parallel size."""
        return self.engine_kwargs.get("pp_size", 1)

    @property
    def dp_size(self) -> int:
        """Data parallel size."""
        return self.engine_kwargs.get("dp_size", 1)

    @property
    def nnodes(self) -> int:
        """Number of nodes."""
        return self.engine_kwargs.get("nnodes", 1)

    @property
    def moe_a2a_backend(self) -> Optional[str]:
        """MoE all-to-all backend."""
        return self.engine_kwargs.get("moe_a2a_backend")

    @property
    def enable_dp_attention(self) -> bool:
        """Whether DP attention is enabled."""
        return self.engine_kwargs.get("enable_dp_attention", False)

    @property
    def is_wideep(self) -> bool:
        """Whether WideEP is enabled (moe_a2a_backend='deepep')."""
        return self.moe_a2a_backend == "deepep"

    @property
    def num_devices(self) -> int:
        """Total number of GPUs needed."""
        return self.tp_size * self.pp_size

    def validate_wideep_config(self) -> None:
        """Validate WideEP-specific configuration constraints.

        Raises:
            ValueError: If WideEP configuration is invalid.
        """
        if not self.is_wideep:
            return

        moe_dp_size = self.engine_kwargs.get("moe_dp_size", 1)

        # Pure WideEP (without DPA) requires moe_dp_size=1
        if not self.enable_dp_attention and moe_dp_size != 1:
            raise ValueError(
                f"WideEP without DPA requires moe_dp_size=1. "
                f"Got moe_dp_size={moe_dp_size}. "
                f"For DPA+WideEP, set enable_dp_attention=True."
            )

        # Multi-node validation
        if self.nnodes > 1:
            total_gpus = self.num_devices
            if total_gpus % self.nnodes != 0:
                raise ValueError(
                    f"total_gpus (tp_size * pp_size = {total_gpus}) must be "
                    f"divisible by nnodes ({self.nnodes})."
                )

        logger.info(
            f"WideEP config validated: tp_size={self.tp_size}, "
            f"nnodes={self.nnodes}, moe_a2a_backend={self.moe_a2a_backend}"
        )

    @classmethod
    def from_llm_config(cls, llm_config: LLMConfig) -> "SGLangEngineConfig":
        """Convert LLMConfig to SGLangEngineConfig."""
        hf_model_id = None
        if llm_config.model_loading_config.model_source is None:
            hf_model_id = llm_config.model_id
        elif isinstance(llm_config.model_loading_config.model_source, str):
            hf_model_id = llm_config.model_loading_config.model_source

        return cls(
            model_id=llm_config.model_id,
            hf_model_id=hf_model_id,
            engine_kwargs=llm_config.engine_kwargs.copy(),
        )
