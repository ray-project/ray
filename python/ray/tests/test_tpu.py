import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray._private.accelerators import TPUAcceleratorManager, tpu
from ray.tests.conftest import _ray_start_cluster
from ray.util.tpu import SlicePlacementGroup


def test_get_current_pod_name_smoke():
    with patch(
        "ray._private.accelerators.tpu.TPUAcceleratorManager.get_current_node_tpu_name",
        return_value="my-tpu",
    ):
        name = ray.util.tpu.get_current_pod_name()
    assert name == "my-tpu"


def test_empty_get_current_pod_name_returns_none():
    with patch(
        "ray._private.accelerators.tpu.TPUAcceleratorManager.get_current_node_tpu_name",
        return_value="",
    ):
        name = ray.util.tpu.get_current_pod_name()
    assert name is None


@pytest.mark.parametrize(
    "test_case",
    [
        # (number_chips_per_host, accl_type, expected_worker_count)
        (4, "v2-4", 1),
        (4, "v3-32", 4),
        (4, "v4-8", 1),
        (4, "v4-16", 2),
        (8, "v5litepod-4", 1),
        (8, "v5litepod-8", 1),
        (8, "v5litepod-16", 2),
        (8, "v5litepod-32", 4),
        (4, "v5p-4", 1),
        (4, "v5p-8", 1),
        (4, "v5p-16", 2),
        (8, "v6e-4", 1),
        (8, "v6e-8", 1),
        (8, "v6e-16", 2),
    ],
)
@patch("glob.glob")
def test_worker_count(mock_glob, test_case):
    num_devices, accelerator_type, expected_worker_count = test_case
    mock_glob.return_value = ["/dev/accel" + str(x) for x in range(num_devices)]
    TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()

    with patch(
        "ray._private.accelerators.tpu.TPUAcceleratorManager."
        "get_current_node_tpu_pod_type",
        return_value=accelerator_type,
    ):
        worker_count = ray.util.tpu.get_current_pod_worker_count()

    assert worker_count == expected_worker_count


@patch("glob.glob")
def test_num_tpu_chips(mock_glob):
    mock_glob.return_value = [
        "/dev/accel0",
        "/dev/accel1",
        "/dev/accel2",
        "/dev/accel3",
    ]
    TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
    num_tpu_chips = ray.util.tpu.get_num_tpu_chips_on_node()
    assert num_tpu_chips == 4


@pytest.mark.parametrize(
    "test_case",
    [
        # (accelerator_type, accelerator_topology, expected_result)
        ("v2-16", "4x4", True),
        ("v2-256", "16x16", True),
        ("v2-4", "2x2", False),
        ("v3-16", "4x4", True),
        ("v3-1024", "32x32", True),
        ("v3-4", "4x16", False),
        ("v4-4", "2x2x1", True),
        ("v4-32", "2x4x4", True),
        ("v4-2048", "8x8x16", True),
        ("v4-4", "16x16x16", False),
        ("v5p-128", "4x4x4", True),
        ("v5p-4096", "16x16x16", True),
        ("v5p-12288", "16x16x24", True),
        ("v5p-4", "24x24x24", False),
        ("v5litepod-16", "2x8", True),
        ("v5litepod-256", "16x16", True),
        ("v5litepod-4", "2x2", True),
        ("v6e-16", "4x4", True),
        ("v6e-64", "8x8", True),
        ("v6e-4", "4x16", False),
    ],
)
@patch("glob.glob")
def test_is_valid_tpu_accelerator_topology(_mock_glob, test_case):
    """Test valid TPU accelerator topologies."""
    accelerator_type, accelerator_topology, expected_result = test_case
    actual_result = TPUAcceleratorManager.is_valid_tpu_accelerator_topology(
        accelerator_type, accelerator_topology
    )

    assert actual_result == expected_result


def test_get_current_node_labels_env_only(monkeypatch):
    # Simulate GKE TPU environment variables
    monkeypatch.setenv("TPU_NAME", "tpu-worker-group-2")
    monkeypatch.setenv("TPU_WORKER_ID", "0")
    monkeypatch.setenv("TPU_ACCELERATOR_TYPE", "v6e-16")
    monkeypatch.setenv("TPU_TOPOLOGY", "4x4")

    tpu_labels = TPUAcceleratorManager.get_current_node_accelerator_labels()

    assert tpu_labels["ray.io/tpu-slice-name"] == "tpu-worker-group-2"
    assert tpu_labels["ray.io/tpu-worker-id"] == "0"
    assert tpu_labels["ray.io/tpu-topology"] == "4x4"
    assert tpu_labels["ray.io/tpu-pod-type"] == "v6e-16"


def test_get_current_node_tpu_topology_from_metadata():
    tpu_env_string = "TPU_ACCELERATOR:v6e.\nTOPOLOGY: '2x2x4'\nTPU_HOST_BOUNDS:0,1,1,2"

    with patch(
        "ray._private.accelerators.tpu._get_tpu_metadata", return_value=tpu_env_string
    ):
        topology = TPUAcceleratorManager.get_current_node_tpu_topology()
        assert topology == "2x2x4"


@pytest.mark.parametrize(
    "topology, accelerator_type, expected_pod_type, should_raise",
    [
        ("2x4", "TPU-V6E", "v6e-8", False),
        ("2x2x2", "TPU-V4", "v4-16", False),
        ("4x8", "TPU-V3", "v3-64", False),
        ("2x2x1", "TPU-V5P", "v5p-8", False),
        ("4x4", "TPU-V5P", "v5p-32", False),
        ("8x16", "TPU-V6E", "v6e-128", False),
        ("", "TPU-V3", None, False),
        ("4x", "TPU-V3", None, True),
    ],
)
def test_infer_tpu_pod_type_from_topology(
    topology, accelerator_type, expected_pod_type, should_raise
):
    if should_raise:
        with pytest.raises(ValueError):
            tpu.infer_tpu_pod_type_from_topology(topology, accelerator_type)
    else:
        actual_result = tpu.infer_tpu_pod_type_from_topology(topology, accelerator_type)
        assert actual_result == expected_pod_type


@pytest.fixture
def ray_start_cpu():
    address_info = ray.init(num_cpus=1)
    yield address_info
    ray.shutdown()


@pytest.fixture
def ray_tpu_cluster():
    """
    Simulates a Ray cluster with two multi-host TPU v4-16 slices.
    """
    pod_type = "v4-16"
    topology = "2x2x2"

    with _ray_start_cluster() as cluster:
        slice_0_env_common = {
            "TPU_NAME": "test-slice-0",
            "TPU_ACCELERATOR_TYPE": pod_type,
            "TPU_TOPOLOGY": topology,
        }
        slice_0_head_labels = {
            "ray.io/tpu-slice-name": "test-slice-0",
            "ray.io/tpu-worker-id": "0",
            "ray.io/tpu-pod-type": pod_type,
            "ray.io/tpu-topology": topology,
        }
        slice_0_worker_labels = {
            "ray.io/tpu-slice-name": "test-slice-0",
            "ray.io/tpu-worker-id": "1",
            "ray.io/tpu-pod-type": pod_type,
            "ray.io/tpu-topology": topology,
        }
        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4, f"TPU-{pod_type}-head": 1},
            env_vars={**slice_0_env_common, "TPU_WORKER_ID": "0"},
            labels=slice_0_head_labels,
        )
        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4},
            env_vars={**slice_0_env_common, "TPU_WORKER_ID": "1"},
            labels=slice_0_worker_labels,
        )

        slice_1_env_common = {
            "TPU_NAME": "test-slice-1",
            "TPU_ACCELERATOR_TYPE": pod_type,
            "TPU_TOPOLOGY": topology,
        }
        slice_1_head_labels = {
            "ray.io/tpu-slice-name": "test-slice-1",
            "ray.io/tpu-worker-id": "0",
            "ray.io/tpu-pod-type": pod_type,
            "ray.io/tpu-topology": topology,
        }
        slice_1_worker_labels = {
            "ray.io/tpu-slice-name": "test-slice-1",
            "ray.io/tpu-worker-id": "1",
            "ray.io/tpu-pod-type": pod_type,
            "ray.io/tpu-topology": topology,
        }
        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4, f"TPU-{pod_type}-head": 1},
            env_vars={**slice_1_env_common, "TPU_WORKER_ID": "0"},
            labels=slice_1_head_labels,
        )
        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4},
            env_vars={**slice_1_env_common, "TPU_WORKER_ID": "1"},
            labels=slice_1_worker_labels,
        )

        ray.init(address=cluster.address)
        yield cluster
        ray.shutdown()


def test_fetch_tpu_slice_name_from_pg(ray_tpu_cluster):
    """Tests that the slice name can be fetched from a PG."""
    tpu_head_pg = ray.util.placement_group(bundles=[{"TPU-v4-16-head": 1}])
    ray.get(tpu_head_pg.ready())

    expected_unique_slice_names = {"test-slice-0", "test-slice-1"}
    slice_name = tpu.fetch_tpu_slice_name_from_pg(tpu_head_pg)
    assert slice_name in expected_unique_slice_names

    ray.util.remove_placement_group(tpu_head_pg)


def test_reserve_tpu_slice(ray_tpu_cluster):
    """Tests that a TPU slice can be successfully reserved."""
    reserved_name_0, hg_pg_0 = tpu.reserve_tpu_slice(
        topology="2x2x2", accelerator_type="TPU-V4"
    )
    reserved_name_1, hg_pg_1 = tpu.reserve_tpu_slice(
        topology="2x2x2", accelerator_type="TPU-V4"
    )

    # Ensure the placement groups reserving the TPU slice using the head worker are valid.
    assert hg_pg_0 is not None, "Expected placement group for slice 0, got None"
    assert hg_pg_1 is not None, "Expected placement group for slice 1, got None"

    assert (
        reserved_name_0 != reserved_name_1
    ), f"Expected to reserve two different slices, but got the same name: {reserved_name_0}"
    expected_unique_slice_names = {"test-slice-0", "test-slice-1"}
    actual_reserved_names = {reserved_name_0, reserved_name_1}
    assert actual_reserved_names == expected_unique_slice_names, (
        f"Got unexpected slice names. Expected {expected_unique_slice_names}, "
        f"but got {actual_reserved_names}"
    )


def test_slice_placement_group(ray_tpu_cluster):
    """Test that single TPU slice can be successfully reserved."""
    slice_placement_group = ray.util.tpu.slice_placement_group(
        topology="2x2x2",
        accelerator_version="v4",
    )
    assert slice_placement_group.chips_per_host == 4
    assert slice_placement_group.num_hosts == 2
    assert slice_placement_group.placement_group.bundle_count == 2
    assert slice_placement_group.placement_group.bundle_specs == [
        {"TPU": 4, "CPU": 1.0},
        {"TPU": 4, "CPU": 1.0},
    ]


def test_multi_slice_placement_group(ray_tpu_cluster):
    """Test that multiple whole TPU slices can be successfully reserved"""
    multi_slice_placement_group = ray.util.tpu.slice_placement_group(
        topology="2x2x2",
        accelerator_version="v4",
        num_slices=2,
    )
    assert multi_slice_placement_group.placement_group.bundle_count == 4
    assert multi_slice_placement_group.num_hosts == 4
    assert multi_slice_placement_group.placement_group.bundle_specs == [
        {"TPU": 4, "CPU": 1.0},  # slice 1, host 1
        {"TPU": 4, "CPU": 1.0},  # slice 1, host 2
        {"TPU": 4, "CPU": 1.0},  # slice 2, host 1
        {"TPU": 4, "CPU": 1.0},  # slice 2, host 2
    ]


@patch("ray.util.tpu.placement_group")
@patch("ray.util.tpu.remove_placement_group")
@patch("ray.util.tpu.reserve_tpu_slice")
def test_slice_placement_group_partial_failure_cleanup(
    mock_reserve, mock_remove_pg, mock_create_pg
):
    """
    Verifies that if a multi-slice request fails halfway through,
    the TPU head placement groups are cleaned up to prevent leaks.
    """
    fake_head_pg_1 = MagicMock(name="head_pg_1")
    mock_reserve.side_effect = [("slice_1", fake_head_pg_1), None]

    with pytest.raises(RuntimeError, match="Failed to reserve TPU slice"):
        SlicePlacementGroup(topology="2x2x2", accelerator_version="v4", num_slices=2)

    # Validate that 2 TPU util attempted to reserve two slices, failed, and
    # correctly cleaned up the hanging TPU head placement groups.
    assert mock_reserve.call_count == 2
    mock_remove_pg.assert_called_once_with(fake_head_pg_1)
    mock_create_pg.assert_not_called()


@pytest.mark.parametrize(
    "accelerator_type, expected_version",
    [
        # type with "TPU-" prefix
        ("TPU-V4", "v4"),
        ("TPU-v4", "v4"),
        ("TPU-V6E", "v6e"),
        ("TPU-v5p", "v5p"),
        # Only the TPU version - no parsing necessary.
        ("v4", "v4"),
        ("v3", "v3"),
        ("v6e", "v6e"),
        ("v5litepod", "v5litepod"),
    ],
)
def test_get_tpu_version_valid(accelerator_type, expected_version):
    assert ray.util.tpu.get_tpu_version_from_type(accelerator_type) == expected_version


@pytest.mark.parametrize(
    "invalid_type",
    [
        "A100",  # GPU type
        "random-invalid-type",  # Random string
        "TPU-invalid",  # TPU prefix
        "",  # Empty string
    ],
)
def test_get_tpu_version_invalid(invalid_type):
    with pytest.raises(ValueError, match="Invalid accelerator_type"):
        ray.util.tpu.get_tpu_version_from_type(invalid_type)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
