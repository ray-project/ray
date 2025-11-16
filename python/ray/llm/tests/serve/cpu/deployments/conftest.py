import pytest


@pytest.fixture
def llm_config_with_mock_engine(llm_config):
    # Make sure engine is mocked.
    if llm_config.runtime_env is None:
        llm_config.runtime_env = {}
    llm_config.runtime_env.setdefault("env_vars", {})[
        "RAYLLM_VLLM_ENGINE_CLS"
    ] = "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine"
    yield llm_config
