from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.engines.sglang.sglang_engine import SGLangServer


def test_sglang_server_applies_serve_llm_replica_loop_defaults():
    options = SGLangServer.get_deployment_options(
        LLMConfig(model_loading_config={"model_id": "test_model"})
    )

    env_vars = options["ray_actor_options"]["runtime_env"]["env_vars"]
    assert env_vars["RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD"] == "0"
    assert env_vars["RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP"] == "0"


def test_sglang_server_preserves_explicit_replica_loop_overrides():
    options = SGLangServer.get_deployment_options(
        LLMConfig(
            model_loading_config={"model_id": "test_model"},
            runtime_env={
                "env_vars": {
                    "RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD": "1",
                    "RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP": "1",
                }
            },
        )
    )

    env_vars = options["ray_actor_options"]["runtime_env"]["env_vars"]
    assert env_vars["RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD"] == "1"
    assert env_vars["RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP"] == "1"
