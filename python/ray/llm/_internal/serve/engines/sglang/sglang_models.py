"""SGLang engine configuration for Ray Serve LLM.

Provides SGLangEngineConfig, parallel to VLLMEngineConfig, that validates
engine_kwargs against SGLang ServerArgs fields.
"""

import dataclasses
import os
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field

from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.constants import ENV_VARS_TO_PROPAGATE
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.observability.logging import get_logger

sglang = try_import("sglang")
logger = get_logger(__name__)


class SGLangEngineConfig(BaseModelExtended):
    """SGLang engine configuration, parallel to VLLMEngineConfig.

    Validates engine_kwargs against SGLang ServerArgs fields to catch
    invalid parameters early.
    """

    model_config = ConfigDict(use_enum_values=True)

    model_id: str = Field(
        description="The identifier for the model. This is the id that will be used to query the model.",
    )
    model_path: Optional[str] = Field(
        None,
        description="The model source path (HF ID or local path).",
    )
    runtime_env: Optional[Dict[str, Any]] = None
    engine_kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description="SGLang ServerArgs passed to sglang.Engine.",
    )

    @classmethod
    def from_llm_config(cls, llm_config: LLMConfig) -> "SGLangEngineConfig":
        """Convert LLMConfig to SGLangEngineConfig, validating against ServerArgs."""
        # Import ServerArgs to get valid field names
        if sglang is None:
            raise ImportError(
                "SGLang is not installed. Please install it with `pip install sglang[all]`."
            )
        from sglang.srt.server_args import ServerArgs

        # Get all valid ServerArgs field names
        server_args_fields = {f.name for f in dataclasses.fields(ServerArgs)}

        # Validate each engine_kwargs key
        engine_kwargs = llm_config.engine_kwargs.copy()
        unknown_keys = [k for k in engine_kwargs if k not in server_args_fields]
        if unknown_keys:
            raise ValueError(
                f"Unknown SGLang ServerArgs fields: {unknown_keys}. "
                f"Valid fields: {sorted(server_args_fields)}"
            )

        # Get model source
        model_source = llm_config.model_loading_config.model_source
        if model_source is None:
            model_path = llm_config.model_id
        elif isinstance(model_source, str):
            model_path = model_source
        else:
            # CloudMirrorConfig case - would need download handling
            raise ValueError(
                "CloudMirrorConfig model source not yet supported for SGLang. "
                "Please provide a string model_source (HF ID or local path)."
            )

        return cls(
            model_id=llm_config.model_id,
            model_path=model_path,
            engine_kwargs=engine_kwargs,
            runtime_env=llm_config.runtime_env,
        )

    def get_engine_kwargs(self) -> Dict[str, Any]:
        """Get kwargs for sglang.Engine initialization.

        Parallel to VLLMEngineConfig.get_initialization_kwargs().
        """
        kwargs = self.engine_kwargs.copy()

        if "model_path" in kwargs:
            raise ValueError(
                "model_path is not allowed in engine_kwargs when using Ray Serve LLM. "
                "Please use `model_loading_config` in LLMConfig instead."
            )

        kwargs["model_path"] = self.model_path or self.model_id

        return kwargs

    def get_runtime_env_with_local_env_vars(self) -> dict:
        """Get runtime_env with propagated environment variables."""
        runtime_env = self.runtime_env or {}
        runtime_env.setdefault("env_vars", {})
        env_vars = runtime_env["env_vars"]

        # Propagate env vars to the runtime env
        for env_var in ENV_VARS_TO_PROPAGATE:
            if env_var in os.environ:
                env_vars[env_var] = os.getenv(env_var)

        return runtime_env

    @property
    def tp_size(self) -> int:
        """Tensor parallel size."""
        return self.engine_kwargs.get("tp_size", 1)

    @property
    def pp_size(self) -> int:
        """Pipeline parallel size."""
        return self.engine_kwargs.get("pp_size", 1)

    @property
    def moe_dp_size(self) -> int:
        """Wide-EP MoE data parallel size (determines gang size)."""
        return self.engine_kwargs.get("moe_dp_size", 1)

    @property
    def num_devices(self) -> int:
        """Total GPUs per replica (for placement group)."""
        return self.tp_size * self.pp_size
