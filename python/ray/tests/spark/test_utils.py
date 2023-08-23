from unittest.mock import patch
import os
import re
import sys

import pytest
from ray.util.spark.utils import (
    get_spark_task_assigned_physical_gpus,
    _calc_mem_per_ray_worker_node,
    _convert_dbfs_path_to_local_path,
    _get_avail_mem_per_ray_worker_node,
)
from ray.util.spark.cluster_init import (
    _convert_ray_node_options,
    _verify_node_options,
    _append_default_spilling_dir_config,
)

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Ray on spark only supports running on Linux.",
)


def test_get_spark_task_assigned_physical_gpus():
    with patch.dict(os.environ, {}, clear=True):
        assert get_spark_task_assigned_physical_gpus([2, 5]) == [2, 5]

    with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "2,3,6"}, clear=True):
        assert get_spark_task_assigned_physical_gpus([0, 1]) == [2, 3]
        assert get_spark_task_assigned_physical_gpus([0, 2]) == [2, 6]


@patch("ray._private.ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES", 1)
def test_calc_mem_per_ray_worker_node():
    assert _calc_mem_per_ray_worker_node(4, 1000000, 400000, 100000) == (
        120000,
        80000,
        None,
    )
    assert _calc_mem_per_ray_worker_node(4, 1000000, 400000, 70000) == (
        130000,
        70000,
        None,
    )
    assert _calc_mem_per_ray_worker_node(4, 1000000, 400000, None) == (
        140000,
        60000,
        None,
    )
    assert _calc_mem_per_ray_worker_node(4, 1000000, 200000, None) == (
        160000,
        40000,
        None,
    )


@patch("ray._private.ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES", 1)
def test_get_avail_mem_per_ray_worker_node(monkeypatch):
    monkeypatch.setenv("RAY_ON_SPARK_WORKER_CPU_CORES", "4")
    monkeypatch.setenv("RAY_ON_SPARK_WORKER_GPU_NUM", "8")
    monkeypatch.setenv("RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES", "1000000")
    monkeypatch.setenv("RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES", "500000")

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=1,
        num_gpus_per_node=2,
        object_store_memory_per_node=None,
    ) == (140000, 60000, None, None)

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=1,
        num_gpus_per_node=2,
        object_store_memory_per_node=80000,
    ) == (120000, 80000, None, None)

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=1,
        num_gpus_per_node=2,
        object_store_memory_per_node=120000,
    ) == (100000, 100000, None, None)

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=2,
        num_gpus_per_node=2,
        object_store_memory_per_node=None,
    ) == (280000, 120000, None, None)

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=1,
        num_gpus_per_node=4,
        object_store_memory_per_node=None,
    ) == (280000, 120000, None, None)


def test_convert_ray_node_options():
    assert _convert_ray_node_options(
        {
            "cluster_name": "aBc",
            "disable_usage_stats": None,
            "include_dashboard": False,
        }
    ) == ["--cluster-name=aBc", "--disable-usage-stats", "--include-dashboard=False"]


def test_verify_node_options():
    _verify_node_options(
        node_options={"permitted": "127.0.0.1"},
        block_keys={"not_permitted": None},
        node_type="head",
    )

    with pytest.raises(
        ValueError,
        match=re.compile(
            "Setting the option 'node_ip_address' for head nodes is not allowed.*"
            "This option is controlled by Ray on Spark"
        ),
    ):
        _verify_node_options(
            node_options={"node_ip_address": "127.0.0.1"},
            block_keys={"node_ip_address": None},
            node_type="head",
        )

    with pytest.raises(
        ValueError,
        match=re.compile(
            "Setting the option 'not_permitted' for worker nodes is not allowed.*"
            "You should set the 'permitted' option instead",
        ),
    ):
        _verify_node_options(
            node_options={"not_permitted": "bad"},
            block_keys={"not_permitted": "permitted"},
            node_type="worker",
        )


def test_convert_dbfs_path_to_local_path():
    assert _convert_dbfs_path_to_local_path("dbfs:/xx/yy/zz") == "/dbfs/xx/yy/zz"
    assert _convert_dbfs_path_to_local_path("dbfs:///xx/yy/zz") == "/dbfs/xx/yy/zz"


def test_append_default_spilling_dir_config():
    assert _append_default_spilling_dir_config({}, "/xx/yy") == {
        "system_config": {
            "object_spilling_config": '{"type": "filesystem", "params": {"directory_path": "/xx/yy"}}'  # noqa: E501
        }
    }
    assert _append_default_spilling_dir_config(
        {"system_config": {"a": 3}}, "/xx/yy"
    ) == {
        "system_config": {
            "a": 3,
            "object_spilling_config": '{"type": "filesystem", "params": {"directory_path": "/xx/yy"}}',  # noqa: E501
        }
    }
    assert _append_default_spilling_dir_config(
        {
            "system_config": {
                "a": 4,
                "object_spilling_config": '{"type": "filesystem", "params": {"directory_path": "/aa/bb"}}',  # noqa: E501
            },
        },
        "/xx/yy",
    ) == {
        "system_config": {
            "a": 4,
            "object_spilling_config": '{"type": "filesystem", "params": {"directory_path": "/aa/bb"}}',  # noqa: E501
        }
    }


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
