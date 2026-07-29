import pytest

import ray.air
from ray.train._internal.autoscaling_coordinator_client import (
    build_train_resource_request,
)


def test_build_train_resource_request_includes_default_trainer_bundle_v1():
    scaling_config = ray.air.ScalingConfig(
        num_workers=2,
        use_gpu=True,
        resources_per_worker={"CPU": 4, "GPU": 1},
    )

    resources, label_selectors = build_train_resource_request(scaling_config, 2)

    assert resources == [
        {"CPU": 1},  # Trainer bundle for v1
        {"CPU": 4, "GPU": 1},
        {"CPU": 4, "GPU": 1},
    ]
    assert label_selectors is None


def test_build_train_resource_request_respects_zero_trainer_resources_v1():
    scaling_config = ray.air.ScalingConfig(
        num_workers=2,
        trainer_resources={"CPU": 0},
        resources_per_worker={"CPU": 1},
    )

    resources, label_selectors = build_train_resource_request(scaling_config, 2)

    assert resources == [{"CPU": 1}, {"CPU": 1}]
    assert label_selectors is None


def test_build_train_resource_request_prepends_trainer_label_selector_v1():
    scaling_config = ray.air.ScalingConfig(
        num_workers=2,
        resources_per_worker={"CPU": 1},
    )

    resources, label_selectors = build_train_resource_request(scaling_config, 2)

    assert resources == [{"CPU": 1}, {"CPU": 1}, {"CPU": 1}]
    assert label_selectors is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
