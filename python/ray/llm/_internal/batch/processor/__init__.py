"""Lazy re-exports for batch processors.

Each ``*_proc.py`` module pulls in heavy ML dependencies (transformers, vllm,
sglang, ...). Eagerly importing all of them here causes a single
``from ray.llm._internal.batch.processor import HttpRequestProcessorConfig``
to load the entire ML stack and to fail when optional dependencies (e.g.
sglang) are not installed.

We use PEP 562 ``__getattr__`` to load each engine-specific config only when
it is first referenced. The ``base`` module is imported eagerly because it is
cheap and exports types used everywhere else.

Note on registration side effects: each ``*_proc.py`` calls
``ProcessorBuilder.register(...)`` at import time. With lazy loading the
registration happens the first time the corresponding config is accessed via
this package -- which is exactly when a user constructs the config and asks
``ProcessorBuilder.build`` to build a processor for it, so the registry is
populated in time for every realistic usage.
"""

from typing import TYPE_CHECKING

from ray.llm._internal.batch.processor.base import (
    Processor,
    ProcessorBuilder,
    ProcessorConfig,
)

# Mapping of public attribute name -> (submodule, attribute name in submodule).
# Each entry is an engine-specific processor config whose defining submodule
# transitively imports heavy optional dependencies.
_LAZY_ATTRS = {
    "HttpRequestProcessorConfig": (
        "http_request_proc",
        "HttpRequestProcessorConfig",
    ),
    "ServeDeploymentProcessorConfig": (
        "serve_deployment_proc",
        "ServeDeploymentProcessorConfig",
    ),
    "SGLangEngineProcessorConfig": (
        "sglang_engine_proc",
        "SGLangEngineProcessorConfig",
    ),
    "vLLMEngineProcessorConfig": (
        "vllm_engine_proc",
        "vLLMEngineProcessorConfig",
    ),
}


def __getattr__(name):
    """Lazily import engine-specific processor configs (PEP 562)."""
    try:
        submodule, attr = _LAZY_ATTRS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None

    import importlib

    module = importlib.import_module(f"{__name__}.{submodule}")
    value = getattr(module, attr)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()).union(_LAZY_ATTRS))


if TYPE_CHECKING:
    from ray.llm._internal.batch.processor.http_request_proc import (  # noqa: F401
        HttpRequestProcessorConfig,
    )
    from ray.llm._internal.batch.processor.serve_deployment_proc import (  # noqa: F401
        ServeDeploymentProcessorConfig,
    )
    from ray.llm._internal.batch.processor.sglang_engine_proc import (  # noqa: F401
        SGLangEngineProcessorConfig,
    )
    from ray.llm._internal.batch.processor.vllm_engine_proc import (  # noqa: F401
        vLLMEngineProcessorConfig,
    )


__all__ = [
    "ProcessorConfig",
    "ProcessorBuilder",
    "HttpRequestProcessorConfig",
    "vLLMEngineProcessorConfig",
    "SGLangEngineProcessorConfig",
    "ServeDeploymentProcessorConfig",
    "Processor",
]
