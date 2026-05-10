"""SGLang serving patterns for Ray Serve LLM."""

from ray.llm._internal.serve.serving_patterns.sglang.builder import (
    build_sglang_wideep_deployment,
)

__all__ = ["build_sglang_wideep_deployment"]
