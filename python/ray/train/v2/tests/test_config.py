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
        match=(
            "If `label_selector` is a list, it must be the same length as "
            "`max_workers`"
        ),
    ):
        ScalingConfig(num_workers=2, label_selector=[{"subcluster": "my_subcluster"}])
    with pytest.raises(
        ValueError,
        match=(
            "If `label_selector` is a list, it must be the same length as "
            "`max_workers`"
        ),
    ):
        ScalingConfig(
            num_workers=(2, 3),
            label_selector=[{"subcluster": "a"}, {"subcluster": "b"}],
        )


def test_label_selector_per_worker():
    # None -> None (no constraint; downstream consumers handle this directly).
    assert ScalingConfig(num_workers=3)._label_selector_per_worker(3) is None

    # Dict -> replicated per worker, decoupled from the original.
    cfg = ScalingConfig(num_workers=2, label_selector={"zone": "a"})
    result = cfg._label_selector_per_worker(2)
    assert result == [{"zone": "a"}, {"zone": "a"}]
    result[0]["zone"] = "b"
    assert cfg.label_selector == {"zone": "a"}

    # List -> sliced to num_workers, decoupled from the original.
    cfg = ScalingConfig(
        num_workers=(1, 3),
        label_selector=[{"a": "1"}, {"a": "2"}, {"a": "3"}],
    )
    assert cfg._label_selector_per_worker(2) == [{"a": "1"}, {"a": "2"}]


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


def test_scaling_config_tpu_labels_and_resources():
    scaling_config = ScalingConfig(
        num_workers=4,
        use_tpu=True,
        topology="4x4",
        accelerator_type="TPU-V6E",
    )
    assert scaling_config._resources_per_worker_not_none == {"CPU": 1, "TPU": 4}
    assert (
        scaling_config._label_selector_per_worker(4)
        == [{"ray.io/accelerator-type": "TPU-V6E", "ray.io/tpu-topology": "4x4"}] * 4
    )
    assert (
        scaling_config._label_selector_per_worker(4, include_tpu_labels=False) is None
    )

    shorthand_config = ScalingConfig(
        num_workers=2,
        use_tpu=True,
        topology=" 2X2X2 ",
        accelerator_type="tpu7x",
    )
    assert (
        shorthand_config._label_selector_per_worker(2)
        == [{"ray.io/accelerator-type": "TPU-V7X", "ray.io/tpu-topology": "2x2x2"}] * 2
    )

    single_worker_custom = ScalingConfig(
        num_workers=1,
        use_tpu=True,
        accelerator_type="TPU-V6E",
        resources_per_worker={"CPU": 2},
        label_selector={"market-type": "spot"},
    )
    assert single_worker_custom._resources_per_worker_not_none == {"CPU": 2, "TPU": 1}
    assert single_worker_custom._label_selector_per_worker(1) == [
        {"ray.io/accelerator-type": "TPU-V6E", "market-type": "spot"}
    ]
    assert single_worker_custom._label_selector_per_worker(
        1, include_tpu_labels=False
    ) == [{"market-type": "spot"}]

    topology_custom_cpu = ScalingConfig(
        num_workers=2,
        use_tpu=True,
        topology="2x2x2",
        accelerator_type="TPU-V4",
        resources_per_worker={"CPU": 2},
    )
    assert topology_custom_cpu._resources_per_worker_not_none == {"CPU": 2, "TPU": 4}


def test_scaling_config_tpu_min_workers_multiple():
    with pytest.raises(ValueError, match="min_workers"):
        ScalingConfig(
            num_workers=(1, 2),
            use_tpu=True,
            topology="2x2x2",
            accelerator_type="TPU-V4",
            resources_per_worker={"TPU": 4},
        )

    with pytest.raises(
        ValueError, match="evenly divide the logical TPU devices per host"
    ):
        ScalingConfig(
            num_workers=2,
            use_tpu=True,
            topology="4x4",
            accelerator_type="TPU-V6E",
            resources_per_worker={"TPU": 8},
        )

    with pytest.raises(ValueError, match="TPU"):
        ScalingConfig(num_workers=1, use_tpu=True, resources_per_worker={"TPU": 0})


def test_scaling_config_tpu_multi_host_and_multi_slice_label_selector():
    """Verify multi-host, multi-slice, elastic, and validation paths for TPU label_selector."""
    # 1. Multi-host single-slice dict selector (v6e 4x4 -> 4 workers).
    single_slice_cfg = ScalingConfig(
        num_workers=4,
        use_tpu=True,
        topology="4x4",
        accelerator_type="TPU-V6E",
        label_selector={"market-type": "spot"},
    )
    assert (
        single_slice_cfg._label_selector_per_worker(4)
        == [
            {
                "ray.io/accelerator-type": "TPU-V6E",
                "ray.io/tpu-topology": "4x4",
                "market-type": "spot",
            }
        ]
        * 4
    )
    assert (
        single_slice_cfg._label_selector_per_worker(4, include_tpu_labels=False)
        == [{"market-type": "spot"}] * 4
    )

    # 2. Multi-slice dict selector (v6e 4x4, 8 workers = 2 slices) with decoupled copies.
    multi_slice_dict_cfg = ScalingConfig(
        num_workers=8,
        use_tpu=True,
        topology="4x4",
        accelerator_type="TPU-V6E",
        label_selector={"pool": "a"},
    )
    with_tpu = multi_slice_dict_cfg._label_selector_per_worker(8)
    without_tpu = multi_slice_dict_cfg._label_selector_per_worker(
        8, include_tpu_labels=False
    )
    assert len(with_tpu) == 8
    assert len(without_tpu) == 8
    assert len({id(d) for d in with_tpu}) == 8
    assert len({id(d) for d in without_tpu}) == 8
    without_tpu[0]["pool"] = "mutated"
    assert multi_slice_dict_cfg.label_selector == {"pool": "a"}
    assert without_tpu[1] == {"pool": "a"}

    # 3. Multi-slice per-worker list selector (4 pool=a + 4 pool=b).
    multi_slice_list_cfg = ScalingConfig(
        num_workers=8,
        use_tpu=True,
        topology="4x4",
        accelerator_type="TPU-V6E",
        label_selector=[{"pool": "a"}] * 4 + [{"pool": "b"}] * 4,
    )
    assert (
        multi_slice_list_cfg._label_selector_per_worker(8, include_tpu_labels=False)
        == [{"pool": "a"}] * 4 + [{"pool": "b"}] * 4
    )
    assert multi_slice_list_cfg._label_selector_per_worker(8) == (
        [
            {
                "ray.io/accelerator-type": "TPU-V6E",
                "ray.io/tpu-topology": "4x4",
                "pool": "a",
            }
        ]
        * 4
        + [
            {
                "ray.io/accelerator-type": "TPU-V6E",
                "ray.io/tpu-topology": "4x4",
                "pool": "b",
            }
        ]
        * 4
    )

    # 4. Elastic multi-slice list selector slices cleanly on downscale.
    elastic_cfg = ScalingConfig(
        num_workers=(4, 8),
        use_tpu=True,
        topology="4x4",
        accelerator_type="TPU-V6E",
        label_selector=[{"pool": "a"}] * 4 + [{"pool": "b"}] * 4,
    )
    assert (
        elastic_cfg._label_selector_per_worker(4, include_tpu_labels=False)
        == [{"pool": "a"}] * 4
    )
    assert elastic_cfg._label_selector_per_worker(8, include_tpu_labels=False) == (
        [{"pool": "a"}] * 4 + [{"pool": "b"}] * 4
    )

    # 5. Negative: mismatched list length raises ValueError.
    with pytest.raises(ValueError, match="must be the same length as `max_workers`"):
        ScalingConfig(
            num_workers=8,
            use_tpu=True,
            topology="4x4",
            accelerator_type="TPU-V6E",
            label_selector=[{"pool": "a"}] * 4,
        )

    # 6. Negative: invalid accelerator_type without topology fails eagerly in __post_init__.
    with pytest.raises(ValueError, match="Invalid accelerator_type"):
        ScalingConfig(num_workers=1, use_tpu=True, accelerator_type="INVALID-TPU")

    # 7. _tpu_label_selector helper across non-TPU, accelerator-only, and full TPU configs.
    assert ScalingConfig(num_workers=2, use_tpu=False)._tpu_label_selector() == {}
    assert ScalingConfig(
        num_workers=1, use_tpu=True, accelerator_type="TPU-V6E"
    )._tpu_label_selector() == {"ray.io/accelerator-type": "TPU-V6E"}
    assert single_slice_cfg._tpu_label_selector() == {
        "ray.io/accelerator-type": "TPU-V6E",
        "ray.io/tpu-topology": "4x4",
    }


def test_storage_filesystem_repr():
    """Test for https://github.com/ray-project/ray/pull/40851"""
    config = RunConfig(storage_filesystem=pyarrow.fs.S3FileSystem())
    repr(config)


def test_scaling_config_default_workers():
    """Test that num_workers defaults to 1 for non-TPU workloads."""
    config = ScalingConfig()
    assert config.num_workers == 1
    assert config.total_resources == {"CPU": 1}

    config_gpu = ScalingConfig(use_gpu=True)
    assert config_gpu.num_workers == 1
    assert config_gpu.total_resources == {"GPU": 1}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
