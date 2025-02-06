import sys
import pytest


def test_serve_llm_import_does_not_error():
    import ray.serve.llm
    from ray.serve.llm import (
        LLMConfig,
        VLLMDeployment,
        LLMModelRouterDeployment,
        build_vllm_deployment,
        build_openai_app,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
