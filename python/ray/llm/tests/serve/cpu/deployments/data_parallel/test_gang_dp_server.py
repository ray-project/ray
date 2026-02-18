import sys
from copy import deepcopy

import pytest
from ray import serve

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.serving_patterns.data_parallel.gang_dp_server import (
    GangDPServer,
    _GangDPMasterInfoBroker,
)
from ray.serve.config import GangSchedulingConfig, GangPlacementStrategy, GangRuntimeFailurePolicy


class TestGetDeploymentOptions:
    """Mirrors test_dp_server.py but verifies gang scheduling config."""

    @pytest.mark.parametrize(
        "data_parallel_size,num_replicas,allowed",
        [
            (None, 1, True),
            (1, 1, True),
            (2, None, True),
            (4, None, True),
            (2, 2, False),
            (4, 2, False),
        ],
    )
    def test_num_replicas_dp_validation(
        self, data_parallel_size, num_replicas, allowed
    ):
        engine_kwargs = (
            {}
            if data_parallel_size is None
            else {"data_parallel_size": data_parallel_size}
        )
        deployment_config = {} if num_replicas is None else {"num_replicas": num_replicas}
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs=deepcopy(engine_kwargs),
            deployment_config=deepcopy(deployment_config),
        )

        if allowed:
            opts = GangDPServer.get_deployment_options(llm_config)
            dp_size = data_parallel_size or 1
            if dp_size > 1:
                assert opts["num_replicas"] == dp_size
                assert isinstance(opts["gang_scheduling_config"], GangSchedulingConfig)
                assert opts["gang_scheduling_config"].gang_size == dp_size
                assert opts["gang_scheduling_config"].gang_placement_strategy == GangPlacementStrategy.PACK
                assert opts["gang_scheduling_config"].runtime_failure_policy == GangRuntimeFailurePolicy.RESTART_GANG
            else:
                assert "gang_scheduling_config" not in opts
        else:
            with pytest.raises(
                ValueError, match="num_replicas should not be specified"
            ):
                GangDPServer.get_deployment_options(llm_config)

    def test_autoscaling_unsupported(self):
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs={"data_parallel_size": 4},
            deployment_config={
                "autoscaling_config": {"target_ongoing_requests": 10}
            },
        )
        with pytest.raises(ValueError, match="autoscaling_config is not supported"):
            GangDPServer.get_deployment_options(llm_config)


class TestGangDPMasterInfoBroker:
    def test_set_then_get(self, shutdown_ray_and_serve):
        handle = serve.run(_GangDPMasterInfoBroker.bind())
        handle.set_master_info.remote("10.0.0.1", 12345).result()
        address, port = handle.get_master_info.remote().result()
        assert address == "10.0.0.1"
        assert port == 12345


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
