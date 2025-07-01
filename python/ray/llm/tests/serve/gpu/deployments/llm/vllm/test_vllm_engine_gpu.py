import sys

import pytest

from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import (
    VLLMEngine,
    _get_vllm_engine_config,
)


class TestVLLMEngine:
    """Test the VLLMEngine."""
    pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
