"""Per-engine adapters.

Each adapter declares how an engine names its KV connector and which
engine-config class builds its ``EngineConfig``. This replaces ``if vLLM /
elif SGLang`` branching in ``llm_config.py`` so adding an engine is additive:
add an adapter and register it here.
"""

import abc
from typing import TYPE_CHECKING, Any, Dict, Optional, Type

if TYPE_CHECKING:
    pass


class EngineAdapter(abc.ABC):
    @abc.abstractmethod
    def connector_name(self, engine_kwargs: Dict[str, Any]) -> Optional[str]:
        """The KV-connector registry name implied by engine_kwargs, or None."""
        ...

    @abc.abstractmethod
    def engine_config_cls(self) -> Type:
        """The engine-config class whose ``from_llm_config`` builds the config."""
        ...


class VLLMAdapter(EngineAdapter):
    def connector_name(self, engine_kwargs: Dict[str, Any]) -> Optional[str]:
        cfg = engine_kwargs.get("kv_transfer_config")
        if not cfg:
            return None
        kv_connector = cfg.get("kv_connector")
        if not kv_connector:
            # Fail fast: a kv_transfer_config with no kv_connector is a
            # misconfiguration, not a "no connector" case.
            raise ValueError("Connector type is not specified.")
        return kv_connector

    def engine_config_cls(self) -> Type:
        from ray.llm._internal.serve.engines.vllm.vllm_models import VLLMEngineConfig

        return VLLMEngineConfig


class SGLangAdapter(EngineAdapter):
    def connector_name(self, engine_kwargs: Dict[str, Any]) -> Optional[str]:
        return (
            "SGLang" if engine_kwargs.get("disaggregation_transfer_backend") else None
        )

    def engine_config_cls(self) -> Type:
        from ray.llm._internal.serve.engines.sglang.sglang_engine import (
            SGLangEngineConfig,
        )

        return SGLangEngineConfig


_ADAPTERS = {"vLLM": VLLMAdapter, "SGLang": SGLangAdapter}


def get_engine_adapter(llm_engine: str) -> EngineAdapter:
    try:
        return _ADAPTERS[llm_engine]()
    except KeyError:
        raise ValueError(f"Unsupported engine: {llm_engine}")
