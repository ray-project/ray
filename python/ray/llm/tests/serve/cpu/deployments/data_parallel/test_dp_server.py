import sys
from copy import deepcopy

import pytest

from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.deployments.data_parallel.dp_server import DPServer


class TestGetDeploymentOptions:
    @pytest.mark.parametrize(
        "data_parallel_size,num_replica,allowed",
        [
            (None, 1, True),
            (None, 2, True),
            (None, 3, True),
            (1, 1, True),
            (1, 2, True),
            (1, 3, True),
            (2, 2, False),
            (2, 3, False),
            (4, 2, False),
            (2, None, True),
            (None, None, True),
        ],
    )
    def test_multi_replica_dp_validation(
        self, data_parallel_size, num_replica, allowed
    ):
        """Test that multi-replica and DP size are mutually exclusive.

        Ray.llm's implementation does not yet support multi-replica
        deployment along with DP.
        """
        engine_kwargs = (
            {}
            if data_parallel_size is None
            else {"data_parallel_size": data_parallel_size}
        )
        deployment_config = {} if num_replica is None else {"num_replicas": num_replica}

        def get_serve_options_with_num_replica():
            llm_config = LLMConfig(
                model_loading_config=dict(model_id="test_model"),
                engine_kwargs=deepcopy(engine_kwargs),
                deployment_config=deepcopy(deployment_config),
            )
            deployment_options = DPServer.get_deployment_options(llm_config)

            return deployment_options

        if allowed:
            serve_options = get_serve_options_with_num_replica()
            actual_num_replicas = serve_options.get("num_replicas", 1)
            expected_num_replicas = (data_parallel_size or 1) * (num_replica or 1)
            assert actual_num_replicas == expected_num_replicas
        else:
            with pytest.raises(
                ValueError,
                match="use engine_kwargs.data_parallel_size",
            ):
                get_serve_options_with_num_replica()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
