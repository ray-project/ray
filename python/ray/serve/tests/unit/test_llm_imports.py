import sys
import pytest
from packaging import version
import pydantic

# skip the test if vllm is installed
HAS_VLLM = False
try:
    import vllm  # noqa: F401

    HAS_VLLM = True
except ImportError:
    pass


PYDANTIC_VERSION = version.parse(pydantic.__version__)


@pytest.mark.skipif(
    PYDANTIC_VERSION < version.parse("2.0"),
    reason="ray.serve.llm requires pydantic>=2.0",
)
@pytest.mark.skipif(not HAS_VLLM, reason="vllm is not installed")
def test_serve_llm_import_does_not_error():
    # expected ImportError because of missing
    # dependencies without ray[llm] dependencies
    with pytest.raises(ImportError):
        import ray.serve.llm  # noqa: F401
    with pytest.raises(ImportError):
        from ray.serve.llm import (
            LLMConfig,  # noqa: F401
        )
    with pytest.raises(ImportError):
        from ray.serve.llm import (
            LLMServer,  # noqa: F401
            LLMRouter,  # noqa: F401
        )
    with pytest.raises(ImportError):
        from ray.serve.llm import (
            build_llm_deployment,  # noqa: F401
            build_openai_app,  # noqa: F401
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
