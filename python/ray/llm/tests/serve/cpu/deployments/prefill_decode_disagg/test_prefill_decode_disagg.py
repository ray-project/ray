import sys

import pytest

from ray.llm._internal.serve.deployments.prefill_decode_disagg.prefill_decode_disagg import (
    build_pd_openai_app,
)
from ray.serve.llm import LLMConfig


class TestServingArgsParsing:
    @pytest.mark.parametrize("kv_connector", ["NixlConnector", "LMCacheConnectorV1"])
    def test_parse_dict(self, kv_connector: str):
        prefill_config = LLMConfig(
            model_loading_config=dict(
                model_id="qwen-0.5b",
                model_source="Qwen/Qwen2.5-0.5B-Instruct",
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=2,
                    max_replicas=2,
                )
            ),
            engine_kwargs=dict(
                tensor_parallel_size=1,
            ),
        )

        decode_config = LLMConfig(
            model_loading_config=dict(
                model_id="qwen-0.5b",
                model_source="Qwen/Qwen2.5-0.5B-Instruct",
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=1,
                    max_replicas=1,
                )
            ),
            engine_kwargs=dict(
                tensor_parallel_size=1,
            ),
        )

        pd_config = {"prefill_config": prefill_config, "decode_config": decode_config}

        app = build_pd_openai_app(pd_config)
        assert app is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
