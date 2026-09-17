import pytest

import ray.train
from ray.train._internal.autoscaling_coordinator_client import (
    build_train_resource_request,
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
