from unittest.mock import Mock

import pytest

import ray
import ray.train
from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    ResourceRequestStrategy,
)
from ray.train._internal.autoscaling_coordinator_client import (
    build_train_resource_request,
)
from ray.train.v2._internal.execution.scaling_policy.autoscaling_coordinator_client import (  # noqa: E501
    TrainAutoscalingCoordinatorClient,
)


def test_build_train_resource_request_excludes_trainer_bundle_v2():
    scaling_config = ray.train.ScalingConfig(
        num_workers=2,
        use_gpu=True,
        resources_per_worker={"CPU": 4, "GPU": 1},
    )

    resources, label_selectors = build_train_resource_request(scaling_config, 2)

    assert resources == [
        {"CPU": 4, "GPU": 1},
        {"CPU": 4, "GPU": 1},
    ]
    assert label_selectors is None


@pytest.mark.parametrize(
    "placement_strategy", [strategy.value for strategy in ResourceRequestStrategy]
)
def test_send_resource_request_forwards_placement_strategy(
    placement_strategy, monkeypatch
):
    """The coordinator reserves the bundles with the strategy the placement
    group places them with, so its guess matches where workers land."""
    coordinator = Mock()
    client = TrainAutoscalingCoordinatorClient(requester_id="train-run")
    client.__dict__["_autoscaling_coordinator"] = coordinator
    monkeypatch.setattr(ray, "get", lambda ref, timeout: None)

    client.send_resource_request(
        scaling_config=ray.train.ScalingConfig(
            num_workers=2, placement_strategy=placement_strategy
        ),
        num_workers=2,
    )

    coordinator.request_resources.remote.assert_called_once()
    _, kwargs = coordinator.request_resources.remote.call_args
    assert kwargs["strategy"] is ResourceRequestStrategy(placement_strategy)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
