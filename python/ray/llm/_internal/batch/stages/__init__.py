"""Lazy re-exports for batch stages.

The stage modules transitively import heavy ML dependencies (transformers,
vllm, sglang, mistral_common, etc.). Eagerly importing them all here causes
even lightweight processors (e.g. ``HttpRequestProcessor``) to pay the cost of
loading the entire ML stack, which is both slow and pulls in optional
dependencies that may not be installed.

We use PEP 562 ``__getattr__`` to defer those imports until first access so
that ``from ray.llm._internal.batch.stages import X`` only loads the submodule
that defines ``X``.
"""

from typing import TYPE_CHECKING

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    wrap_postprocess,
    wrap_preprocess,
)
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    DetokenizeStageConfig,
    PrepareImageStageConfig,
    PrepareMultimodalStageConfig,
    TokenizerStageConfig,
)

# Mapping of public attribute name -> (submodule, attribute name in submodule).
# Each entry here is a stage class that is expensive to import (because the
# defining submodule pulls in transformers / vllm / sglang / mistral_common /
# pillow / etc.). They are loaded on first access via ``__getattr__`` below.
_LAZY_ATTRS = {
    "ChatTemplateStage": ("chat_template_stage", "ChatTemplateStage"),
    "HttpRequestStage": ("http_request_stage", "HttpRequestStage"),
    "PrepareImageStage": ("prepare_image_stage", "PrepareImageStage"),
    "PrepareMultimodalStage": ("prepare_multimodal_stage", "PrepareMultimodalStage"),
    "ServeDeploymentStage": ("serve_deployment_stage", "ServeDeploymentStage"),
    "SGLangEngineStage": ("sglang_engine_stage", "SGLangEngineStage"),
    "TokenizeStage": ("tokenize_stage", "TokenizeStage"),
    "DetokenizeStage": ("tokenize_stage", "DetokenizeStage"),
    "vLLMEngineStage": ("vllm_engine_stage", "vLLMEngineStage"),
}


def __getattr__(name):
    """Lazily import heavy stage classes on first access (PEP 562)."""
    try:
        submodule, attr = _LAZY_ATTRS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None

    import importlib

    module = importlib.import_module(f"{__name__}.{submodule}")
    value = getattr(module, attr)
    # Cache on the package so subsequent lookups skip __getattr__.
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()).union(_LAZY_ATTRS))


# Help static type checkers (and IDEs) see the lazily-exported names. These
# imports are never executed at runtime, so they do not reintroduce the heavy
# dependencies.
if TYPE_CHECKING:
    from ray.llm._internal.batch.stages.chat_template_stage import (  # noqa: F401
        ChatTemplateStage,
    )
    from ray.llm._internal.batch.stages.http_request_stage import (  # noqa: F401
        HttpRequestStage,
    )
    from ray.llm._internal.batch.stages.prepare_image_stage import (  # noqa: F401
        PrepareImageStage,
    )
    from ray.llm._internal.batch.stages.prepare_multimodal_stage import (  # noqa: F401
        PrepareMultimodalStage,
    )
    from ray.llm._internal.batch.stages.serve_deployment_stage import (  # noqa: F401
        ServeDeploymentStage,
    )
    from ray.llm._internal.batch.stages.sglang_engine_stage import (  # noqa: F401
        SGLangEngineStage,
    )
    from ray.llm._internal.batch.stages.tokenize_stage import (  # noqa: F401
        DetokenizeStage,
        TokenizeStage,
    )
    from ray.llm._internal.batch.stages.vllm_engine_stage import (  # noqa: F401
        vLLMEngineStage,
    )


__all__ = [
    "StatefulStage",
    "HttpRequestStage",
    "PrepareMultimodalStage",
    "ChatTemplateStage",
    "TokenizeStage",
    "DetokenizeStage",
    "vLLMEngineStage",
    "SGLangEngineStage",
    "ServeDeploymentStage",
    "wrap_preprocess",
    "wrap_postprocess",
    "PrepareImageStage",
    "ChatTemplateStageConfig",
    "DetokenizeStageConfig",
    "PrepareImageStageConfig",
    "PrepareMultimodalStageConfig",
    "TokenizerStageConfig",
]
