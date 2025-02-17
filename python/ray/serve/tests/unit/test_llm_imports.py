import sys
import pytest


def test_serve_llm_import_does_not_error():
    import ray.serve.llm  # noqa: F401
    from ray.serve.llm import (
        LLMConfig,  # noqa: F401
        VLLMDeployment,  # noqa: F401
        LLMModelRouterDeployment,  # noqa: F401
        build_vllm_deployment,  # noqa: F401
        build_openai_app,  # noqa: F401
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
