import pyarrow.fs
import pytest

from ray.train import RunConfig, ScalingConfig


def test_scaling_config_validation():
    assert ScalingConfig(
        num_workers=2, use_gpu=True, resources_per_worker={"CPU": 1}
    ).total_resources == {"CPU": 2, "GPU": 2}

    with pytest.raises(ValueError, match="`use_gpu` is False but `GPU` was found in"):
        ScalingConfig(num_workers=2, use_gpu=False, resources_per_worker={"GPU": 1})

    with pytest.raises(ValueError, match="Cannot specify both"):
        ScalingConfig(num_workers=2, use_gpu=True, use_tpu=True)

    with pytest.raises(
        ValueError,
        match="If `bundle_label_selector` is a list, it must be the same length as `num_workers`",
    ):
        ScalingConfig(
            num_workers=2, bundle_label_selector=[{"subcluster": "my_subcluster"}]
        )


def test_scaling_config_accelerator_type():
    scaling_config = ScalingConfig(num_workers=2, use_gpu=True, accelerator_type="A100")
    assert scaling_config.accelerator_type == "A100"
    assert scaling_config._resources_per_worker_not_none == {
        "GPU": 1,
        "accelerator_type:A100": 0.001,
    }
    assert scaling_config.total_resources == {
        "GPU": 2,
        "accelerator_type:A100": 0.002,
    }
    assert scaling_config.additional_resources_per_worker == {
        "accelerator_type:A100": 0.001
    }


def test_storage_filesystem_repr():
    """Test for https://github.com/ray-project/ray/pull/40851"""
    config = RunConfig(storage_filesystem=pyarrow.fs.S3FileSystem())
    repr(config)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
