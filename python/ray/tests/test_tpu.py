import json
import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray._private.accelerators import TPUAcceleratorManager, tpu
from ray.util.tpu import (
    SlicePlacementGroup,
    SubslicePlacementGroup,
    _find_valid_parent_topologies,
)


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
        # (number_chips_per_host, parsed accl_type, expected_worker_count)
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
        (4, "v6e-4", 1),
        (8, "v6e-8", 1),
        (4, "v6e-8", 2),
        (8, "v6e-16", 2),
        (4, "v7x-8", 1),
        (4, "v7x-16", 2),
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
        ("v6e-8", "2x4", True),
        ("v6e-16", "4x4", True),
        ("v6e-64", "8x8", True),
        ("v6e-4", "4x16", False),
        ("tpu7x-16", "2x2x2", True),
        ("tpu7x-64", "2x4x4", True),
        ("v7x-8", "4x4", False),
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
        ("2x2x2", "TPU-V7X", "v7x-16", False),
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
def ray_tpu_cluster(ray_start_cluster):
    """
    Simulates a Ray cluster with two multi-host TPU v4-16 slices.
    """
    pod_type = "v4-16"
    topology = "2x2x2"

    cluster = ray_start_cluster
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


@pytest.fixture
def ray_v6e_tpu_cluster(ray_start_cluster):
    """
    Simulates a Ray cluster with two v6e-8 slices (2x4 topology).

    """
    pod_type = "v6e-8"
    topology = "2x4"
    cluster = ray_start_cluster

    for i in range(2):
        env_common = {
            "TPU_NAME": f"test-v6e-slice-{i}",
            "TPU_ACCELERATOR_TYPE": pod_type,
            "TPU_TOPOLOGY": topology,
        }
        head_labels = {
            "ray.io/tpu-slice-name": f"test-v6e-slice-{i}",
            "ray.io/tpu-worker-id": "0",
            "ray.io/tpu-pod-type": pod_type,
            "ray.io/tpu-topology": topology,
        }
        # A single-host v6e-8 has 8 chips on one node
        cluster.add_node(
            num_cpus=4,
            resources={"TPU": 8, f"TPU-{pod_type}-head": 1},
            env_vars={**env_common, "TPU_WORKER_ID": "0"},
            labels=head_labels,
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
        ("TPU-V7X", "v7x"),
        # Only the TPU version - no parsing necessary.
        ("v4", "v4"),
        ("v3", "v3"),
        ("v6e", "v6e"),
        ("v5litepod", "v5litepod"),
        ("v7x", "v7x"),
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


@pytest.mark.parametrize(
    "topology, accelerator_type, num_workers, resources_per_worker, expected_slices",
    [
        # "2x2x1" has 4 chips, for 4 workers with TPU: 1 each we expect num_slices=1.
        ("2x2x1", "TPU-V4", 4, {"TPU": 1}, 1),
        # "2x2x1" has 4 chips, for 8 workers with TPU: 1 each we expect num_slices=2.
        ("2x2x1", "v4", 8, {"TPU": 1}, 2),
        # "2x2x2" has 8 chips and 2 hosts, defaulting to 1 TPU worker per host
        # and requesting 4 workers, we expect num_slices=2.
        ("2x2x2", "TPU-V4", 4, None, 2),
        # "2x2x4" has 16 chips and 4 hosts, defaulting to 1 TPU worker per host
        # and requesting 4 workers, we expect num_slices=1.
        ("2x2x4", "TPU-V4", 4, None, 1),
        # 0 workers requested -> fallback to 1 slice.
        ("2x2x1", "v4", 0, None, 1),
        # Invalid topology -> fallback to 1 slice.
        ("", "v4", 4, {"TPU": 1}, 1),
        ("2x2x1", "", 4, {"TPU": 1}, 1),
    ],
)
def test_get_tpu_num_slices_for_workers(
    topology, accelerator_type, num_workers, resources_per_worker, expected_slices
):
    num_slices = ray.util.tpu.get_tpu_num_slices_for_workers(
        topology=topology,
        accelerator_type=accelerator_type,
        num_workers=num_workers,
        resources_per_worker=resources_per_worker,
    )
    assert num_slices == expected_slices


def _make_mock_tpu_node(
    alive, pod_type, slice_name, worker_id, tpu_chips=4, node_id=None
):
    """Helper to mock a Ray Node dictionary returned by ray.nodes()."""
    if node_id is None:
        node_id = f"node_{slice_name}_{worker_id}"
    return {
        "NodeID": node_id,
        "Alive": alive,
        "Labels": {
            "ray.io/tpu-pod-type": pod_type,
            "ray.io/tpu-slice-name": slice_name,
            "ray.io/tpu-worker-id": str(worker_id),
        },
        "Resources": {"TPU": tpu_chips},
    }


@pytest.mark.parametrize(
    "topology, accelerator_type, mock_nodes, mock_avail_resources, expected_ready",
    [
        # 1 fully intact and available v4 slice (2 physical hosts).
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-1", 0, node_id="A"),
                _make_mock_tpu_node(True, "v4-16", "slice-1", 1, node_id="B"),
            ],
            {
                "A": {"TPU": 4},
                "B": {"TPU": 4},
            },
            1,
        ),
        # 1 fully intact slice, but one node is using 2 TPUs (unavailable) -> 0 ready slices.
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-1", 0, node_id="A"),
                _make_mock_tpu_node(True, "v4-16", "slice-1", 1, node_id="B"),
            ],
            {
                "A": {"TPU": 2},
                "B": {"TPU": 4},
            },
            0,
        ),
        # Fractured slice (missing a physical host) -> 0 ready slices.
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-1", 0, node_id="A"),
                # Worker 1 is missing
            ],
            {
                "A": {"TPU": 4},
            },
            0,
        ),
        # Correct number of hosts, but missing the head node (rank 0) -> 0 ready slices.
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-1", 1, node_id="A"),
                _make_mock_tpu_node(True, "v4-16", "slice-1", 2, node_id="B"),
            ],
            {
                "A": {"TPU": 4},
                "B": {"TPU": 4},
            },
            0,
        ),
        # Fractured slice (one physical host is dead) -> 0 ready slices.
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-1", 0, node_id="A"),
                _make_mock_tpu_node(False, "v4-16", "slice-1", 1, node_id="B"),
            ],
            {
                "A": {"TPU": 4},
            },
            0,
        ),
        # 2 slices total: one intact & available, one fractured -> 1 ready slice.
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-A", 0, node_id="A0"),
                _make_mock_tpu_node(True, "v4-16", "slice-A", 1, node_id="A1"),
                _make_mock_tpu_node(True, "v4-16", "slice-B", 0, node_id="B0"),
                # slice-B worker 1 is missing
            ],
            {
                "A0": {"TPU": 4},
                "A1": {"TPU": 4},
                "B0": {"TPU": 4},
            },
            1,
        ),
        # 1 fully intact and available v6e 2x4 slice (single-host).
        (
            "2x4",
            "v6e",
            [
                _make_mock_tpu_node(
                    True, "v6e-8", "slice-1", 0, tpu_chips=8, node_id="A"
                ),
            ],
            {
                "A": {"TPU": 8},
            },
            1,
        ),
        # 1 fully intact and available v6e 2x4 slice (2 physical hosts).
        (
            "2x4",
            "v6e",
            [
                _make_mock_tpu_node(
                    True, "v6e-8", "slice-1", 0, tpu_chips=4, node_id="A"
                ),
                _make_mock_tpu_node(
                    True, "v6e-8", "slice-1", 1, tpu_chips=4, node_id="B"
                ),
            ],
            {
                "A": {"TPU": 4},
                "B": {"TPU": 4},
            },
            1,
        ),
        # 2 fully intact v6e slices.
        (
            "4x4",
            "v6e",
            [
                _make_mock_tpu_node(True, "v6e-16", "slice-1", 0, node_id="S1_0"),
                _make_mock_tpu_node(True, "v6e-16", "slice-1", 1, node_id="S1_1"),
                _make_mock_tpu_node(True, "v6e-16", "slice-1", 2, node_id="S1_2"),
                _make_mock_tpu_node(True, "v6e-16", "slice-1", 3, node_id="S1_3"),
                _make_mock_tpu_node(True, "v6e-16", "slice-2", 0, node_id="S2_0"),
                _make_mock_tpu_node(True, "v6e-16", "slice-2", 1, node_id="S2_1"),
                _make_mock_tpu_node(True, "v6e-16", "slice-2", 2, node_id="S2_2"),
                _make_mock_tpu_node(True, "v6e-16", "slice-2", 3, node_id="S2_3"),
            ],
            {
                "S1_0": {"TPU": 4},
                "S1_1": {"TPU": 4},
                "S1_2": {"TPU": 4},
                "S1_3": {"TPU": 4},
                "S2_0": {"TPU": 4},
                "S2_1": {"TPU": 4},
                "S2_2": {"TPU": 4},
                "S2_3": {"TPU": 4},
            },
            2,
        ),
    ],
)
@patch("ray.is_initialized", return_value=True)
@patch("ray._private.state.available_resources_per_node")
@patch("ray.nodes")
def test_get_num_ready_tpu_slices_calculation(
    mock_nodes_call,
    mock_avail_resources_call,
    mock_is_initialized,
    topology,
    accelerator_type,
    mock_nodes,
    mock_avail_resources,
    expected_ready,
):
    """Test that the TPU slice readiness utility correctly calculates the number of ready
    slices in different mocked scenarios, including idle resource verification."""
    mock_nodes_call.return_value = mock_nodes
    mock_avail_resources_call.return_value = mock_avail_resources

    actual_ready = ray.util.tpu.get_num_ready_tpu_slices(
        topology=topology,
        accelerator_type=accelerator_type,
    )
    assert actual_ready == expected_ready


@pytest.mark.parametrize(
    "topology, accelerator_type, mock_nodes, expected_intact",
    [
        # 1 fully intact v4 slice (2 physical hosts).
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-1", 0, node_id="A"),
                _make_mock_tpu_node(True, "v4-16", "slice-1", 1, node_id="B"),
            ],
            1,
        ),
        # Fractured slice (missing a physical host) -> 0 intact slices.
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-1", 0, node_id="A"),
            ],
            0,
        ),
        # Missing head node (rank 0) -> 0 intact slices.
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-1", 1, node_id="A"),
                _make_mock_tpu_node(True, "v4-16", "slice-1", 2, node_id="B"),
            ],
            0,
        ),
        # One physical host is dead -> 0 intact slices.
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-1", 0, node_id="A"),
                _make_mock_tpu_node(False, "v4-16", "slice-1", 1, node_id="B"),
            ],
            0,
        ),
        # 2 slices: one intact, one fractured -> 1 intact slice.
        (
            "2x2x2",
            "v4",
            [
                _make_mock_tpu_node(True, "v4-16", "slice-A", 0, node_id="A0"),
                _make_mock_tpu_node(True, "v4-16", "slice-A", 1, node_id="A1"),
                _make_mock_tpu_node(True, "v4-16", "slice-B", 0, node_id="B0"),
            ],
            1,
        ),
        # 2 fully intact v6e slices.
        (
            "4x4",
            "v6e",
            [
                _make_mock_tpu_node(True, "v6e-16", "slice-1", 0, node_id="S1_0"),
                _make_mock_tpu_node(True, "v6e-16", "slice-1", 1, node_id="S1_1"),
                _make_mock_tpu_node(True, "v6e-16", "slice-1", 2, node_id="S1_2"),
                _make_mock_tpu_node(True, "v6e-16", "slice-1", 3, node_id="S1_3"),
                _make_mock_tpu_node(True, "v6e-16", "slice-2", 0, node_id="S2_0"),
                _make_mock_tpu_node(True, "v6e-16", "slice-2", 1, node_id="S2_1"),
                _make_mock_tpu_node(True, "v6e-16", "slice-2", 2, node_id="S2_2"),
                _make_mock_tpu_node(True, "v6e-16", "slice-2", 3, node_id="S2_3"),
            ],
            2,
        ),
    ],
)
@patch("ray.is_initialized", return_value=True)
@patch("ray.nodes")
def test_get_num_tpu_slices_calculation(
    mock_nodes_call,
    mock_is_initialized,
    topology,
    accelerator_type,
    mock_nodes,
    expected_intact,
):
    """Test that the intact TPU slice utility counts slices based purely on
    physical integrity (all hosts alive, correct chip count) without checking
    whether they are idle."""
    mock_nodes_call.return_value = mock_nodes

    actual_intact = ray.util.tpu.get_num_tpu_slices(
        topology=topology,
        accelerator_type=accelerator_type,
    )
    assert actual_intact == expected_intact


@patch("ray.is_initialized", return_value=False)
def test_get_num_tpu_slices_uninitialized(mock_is_initialized):
    """Test that the utility gracefully handles an uninitialized Ray context."""
    assert ray.util.tpu.get_num_tpu_slices("2x2x2", "v4") == 0


def test_get_num_ready_tpu_slices(ray_tpu_cluster):
    """
    Tests the get_num_ready_tpu_slices utility against a real Ray cluster.
    The ray_tpu_cluster fixture provisions two v4-16 slices (2x2x2 topology).
    """
    ready_slices = ray.util.tpu.get_num_ready_tpu_slices(
        topology="2x2x2", accelerator_type="v4"
    )
    assert ready_slices == 2


@patch("ray.is_initialized", return_value=False)
def test_get_num_ready_tpu_slices_uninitialized(mock_is_initialized):
    """Test that the utility gracefully handles an uninitialized Ray context."""
    assert ray.util.tpu.get_num_ready_tpu_slices("2x2x2", "v4") == 0


@pytest.mark.parametrize(
    "node_dict, expected_slice_name",
    [
        (_make_mock_tpu_node(True, "v4-16", "slice-1", 0), "slice-1"),
        (_make_mock_tpu_node(True, "v6e-8", "slice-A", 1), "slice-A"),
        (_make_mock_tpu_node(True, "v4-16", "", 0), ""),
        ({"Alive": True, "Labels": {}}, None),  # Missing TPU slice name
        ({"Alive": True}, None),  # Missing Node labels dict
    ],
)
def test_get_tpu_slice_name_from_node(node_dict, expected_slice_name):
    """Tests that the utility correctly extracts the TPU slice name from a node dictionary."""
    assert ray.util.tpu.get_tpu_slice_name_from_node(node_dict) == expected_slice_name


@patch("ray.is_initialized", return_value=True)
@patch("ray.nodes")
def test_get_tpu_nodes_for_slice(mock_nodes_call, mock_is_initialized):
    """Tests that the utility correctly filters alive nodes for a specific slice."""
    mock_nodes = [
        _make_mock_tpu_node(True, "v4-16", "slice-A", 0),
        _make_mock_tpu_node(True, "v4-16", "slice-A", 1),
        _make_mock_tpu_node(False, "v4-16", "slice-A", 2),  # Dead node
        _make_mock_tpu_node(True, "v4-16", "slice-B", 0),  # Wrong slice
    ]
    mock_nodes_call.return_value = mock_nodes

    # Call ray.nodes() to fetch from GCS
    nodes_a = ray.util.tpu.get_tpu_nodes_for_slice("slice-A")
    assert len(nodes_a) == 2
    assert nodes_a[0]["Labels"][ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY] == "0"
    assert nodes_a[1]["Labels"][ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY] == "1"
    assert mock_nodes_call.call_count == 1

    # Pass cached nodes directly
    nodes_b = ray.util.tpu.get_tpu_nodes_for_slice("slice-B", nodes=mock_nodes)
    assert len(nodes_b) == 1
    assert nodes_b[0]["Labels"][ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY] == "slice-B"
    assert mock_nodes_call.call_count == 1  # Call count remains 1

    # Use non-existent slice
    nodes_c = ray.util.tpu.get_tpu_nodes_for_slice("slice-C", nodes=mock_nodes)
    assert len(nodes_c) == 0


@patch("ray.is_initialized", return_value=False)
def test_get_tpu_nodes_for_slice_uninitialized(mock_is_initialized):
    """Test that the utility gracefully handles an uninitialized Ray context."""
    assert ray.util.tpu.get_tpu_nodes_for_slice("slice-A") == []


def test_get_tpu_worker_resources_chips_per_vm_override():
    """Test that chips_per_vm correctly overrides the default resource calculations."""

    # Default behavior: v6e 2x4 defaults to a single 8-chip host
    num_workers, resources = ray.util.tpu.get_tpu_worker_resources(
        topology="2x4", accelerator_type="v6e"
    )
    assert num_workers == 1
    assert resources["TPU"] == 8

    # Override behavior: v6e 2x4 forced to 4 chips per VM (2 hosts)
    num_workers_override, resources_override = ray.util.tpu.get_tpu_worker_resources(
        topology="2x4", accelerator_type="v6e", chips_per_vm=4
    )
    assert num_workers_override == 2
    assert resources_override["TPU"] == 4


def test_slice_placement_group_chips_per_vm_override(ray_v6e_tpu_cluster):
    """Test that SlicePlacementGroup respects chips_per_vm for host calculation."""

    # Default behavior (1 VM with 8 chips)
    default_pg = SlicePlacementGroup(topology="2x4", accelerator_version="v6e")
    assert default_pg.chips_per_host == 8
    assert default_pg.num_hosts == 1
    assert default_pg.num_bundles == 1
    assert default_pg.bundle_resources["TPU"] == 8

    # User-specified override behavior (2 VMs with 4 chips each)
    override_pg = SlicePlacementGroup(
        topology="2x4", accelerator_version="v6e", chips_per_vm=4
    )
    assert override_pg.chips_per_host == 4
    assert override_pg.num_hosts == 2
    assert override_pg.num_bundles == 2
    assert override_pg.bundle_resources["TPU"] == 4


def test_user_bundle_label_selector_merged(ray_tpu_cluster):
    """Verifies that user-passed bundle_label_selector is merged with dynamic TPU labels."""
    user_selectors = [{"env": "prod"}, {"env": "test"}]

    # 2x2x2 v4 = 2 hosts = 2 bundles
    slice_pg = SlicePlacementGroup(
        topology="2x2x2", accelerator_version="v4", bundle_label_selector=user_selectors
    )

    assert len(slice_pg._bundle_label_selector) == 2

    # Verify slice 0
    assert slice_pg._bundle_label_selector[0]["env"] == "prod"
    assert ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY in slice_pg._bundle_label_selector[0]

    # Verify slice 1
    assert slice_pg._bundle_label_selector[1]["env"] == "test"
    assert ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY in slice_pg._bundle_label_selector[1]


def test_user_bundle_label_selector_collision_dynamic_wins(ray_v6e_tpu_cluster):
    """Verifies that dynamic TPU labels take precedence on collision."""
    user_selectors = [{ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: "user-requested-slice"}]

    # v6e-8 is single host (1 bundle)
    slice_pg = SlicePlacementGroup(
        topology="2x4", accelerator_version="v6e", bundle_label_selector=user_selectors
    )

    assert len(slice_pg._bundle_label_selector) == 1
    # The dynamic value should win (it generates test-v6e-slice-N)
    actual_val = slice_pg._bundle_label_selector[0][
        ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY
    ]
    assert actual_val != "user-requested-slice"
    assert "test-v6e-slice-" in actual_val


def test_user_bundle_label_selector_length_mismatch_raises():
    """Verifies that providing wrong length of selector list raises ValueError."""
    user_selectors = [{"env": "prod"}]  # Only 1 provided but 2x2x2 v4 has 2 hosts

    with pytest.raises(ValueError, match="bundle_label_selector length"):
        SlicePlacementGroup(
            topology="2x2x2",
            accelerator_version="v4",
            bundle_label_selector=user_selectors,
        )


def test_release_head_pgs_idempotent(ray_tpu_cluster):
    """Verifies that release_head_pgs() is idempotent."""
    slice_pg = SlicePlacementGroup(topology="2x2x2", accelerator_version="v4")

    assert len(slice_pg.head_placement_groups) == 1

    slice_pg.release_head_pgs()
    assert len(slice_pg.head_placement_groups) == 0

    # Call again, should not raise
    slice_pg.release_head_pgs()
    assert len(slice_pg.head_placement_groups) == 0


def test_shutdown_idempotent(ray_tpu_cluster):
    """Verifies that shutdown() is idempotent."""
    slice_pg = SlicePlacementGroup(topology="2x2x2", accelerator_version="v4")

    slice_pg.shutdown()
    assert slice_pg.placement_group is None
    assert len(slice_pg.head_placement_groups) == 0

    # Call again, should not raise
    slice_pg.shutdown()


def test_shutdown_safe_after_construction_failure():
    """Verifies that shutdown() is safe to call on a partially-constructed instance."""
    with patch(
        "ray.util.tpu.SlicePlacementGroup._reserve_slice",
        side_effect=RuntimeError("Test failure"),
    ):
        with pytest.raises(RuntimeError, match="Test failure"):
            SlicePlacementGroup(topology="2x2x2", accelerator_version="v4")

    # If the above didn't crash or leak resources, we are good.
    # We can also manually construct a partial instance and call shutdown.
    partial_pg = SlicePlacementGroup.__new__(SlicePlacementGroup)
    partial_pg._head_pgs = []
    partial_pg._placement_group = None

    # Should not raise even though it's missing attributes
    partial_pg.shutdown()


def test_release_head_pgs_after_ready_then_shutdown(ray_tpu_cluster):
    """Validates Slice PG lifecycle: wait until ready, release head PGs, then shutdown."""
    slice_pg = SlicePlacementGroup(topology="2x2x2", accelerator_version="v4")

    # Wait for ready
    ray.get(slice_pg.placement_group.ready())

    slice_pg.release_head_pgs()
    assert len(slice_pg.head_placement_groups) == 0

    slice_pg.shutdown()
    assert slice_pg.placement_group is None


def test_chips_per_vm_zero_raises_value_error():
    """Verifies that passing chips_per_vm=0 explicitly raises a ValueError instead of silently using the topology default."""
    with pytest.raises(ValueError):
        SlicePlacementGroup(
            topology="2x2x2",
            accelerator_version="v4",
            chips_per_vm=0,
        )

    # Also verify when custom resources already include a "TPU" key
    with pytest.raises(ValueError):
        SlicePlacementGroup(
            topology="2x2x2",
            accelerator_version="v4",
            resources_per_bundle={"TPU": 4},
            chips_per_vm=0,
        )


def _make_mock_slice_handle(num_bundles=2, chips_per_host=4, tpu_per_bundle=4):
    """Return a MagicMock that looks like a SlicePlacementGroup."""
    mock_handle = MagicMock(spec=SlicePlacementGroup)
    mock_handle.num_bundles = num_bundles
    mock_handle.chips_per_host = chips_per_host
    mock_handle.bundle_resources = {"TPU": tpu_per_bundle, "CPU": 1.0}
    mock_handle.placement_group = MagicMock()
    return mock_handle


def _make_mock_fn():
    """Return a MagicMock that behaves like a @ray.remote function."""
    fn = MagicMock()
    fn.options.return_value = fn  # .options() returns itself for chaining
    fn.remote.return_value = MagicMock()  # each .remote() returns a fake ObjectRef
    return fn


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_creates_internal_slice(mock_spg_cls):
    """When tpu_slice=None a SlicePlacementGroup is constructed with the
    correct forwarded arguments."""
    mock_handle = _make_mock_slice_handle()
    mock_spg_cls.return_value = mock_handle

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            ray.util.tpu.dispatch(
                _make_mock_fn(),
                topology="2x2x2",
                accelerator_version="v4",
                num_slices=2,
                chips_per_vm=4,
                head_reservation_timeout_s=30.0,
            )

    mock_spg_cls.assert_called_once_with(
        topology="2x2x2",
        accelerator_version="v4",
        num_slices=2,
        chips_per_vm=4,
        head_reservation_timeout_s=30.0,
    )


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_uses_provided_slice(mock_spg_cls):
    """When tpu_slice= is provided, SlicePlacementGroup is never constructed
    and topology/accelerator_version are not required."""
    existing_handle = _make_mock_slice_handle()

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            ray.util.tpu.dispatch(
                _make_mock_fn(),
                tpu_slice=existing_handle,
            )

    mock_spg_cls.assert_not_called()


def test_dispatch_missing_topology_raises():
    """ValueError is raised when tpu_slice=None and topology or
    accelerator_version are omitted."""
    with pytest.raises(
        ValueError, match="topology and accelerator_version are required"
    ):
        ray.util.tpu.dispatch(_make_mock_fn(), accelerator_version="v4")

    with pytest.raises(
        ValueError, match="topology and accelerator_version are required"
    ):
        ray.util.tpu.dispatch(_make_mock_fn(), topology="2x2x2")


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_dispatches_one_task_per_bundle(mock_spg_cls):
    """dispatch returns exactly num_bundles ObjectRefs."""
    num_bundles = 3
    mock_handle = _make_mock_slice_handle(num_bundles=num_bundles)
    mock_spg_cls.return_value = mock_handle
    fn = _make_mock_fn()

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            refs = ray.util.tpu.dispatch(fn, topology="2x2x2", accelerator_version="v4")

    assert len(refs) == num_bundles
    assert fn.options.call_count == num_bundles


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_applies_unique_bundle_index_per_task(mock_spg_cls):
    """Each dispatched task uses a distinct, sequential placement_group_bundle_index."""
    num_bundles = 4
    mock_handle = _make_mock_slice_handle(num_bundles=num_bundles)
    mock_spg_cls.return_value = mock_handle
    fn = _make_mock_fn()

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            ray.util.tpu.dispatch(fn, topology="2x2x2", accelerator_version="v4")

    bundle_indices = [
        call.kwargs["scheduling_strategy"].placement_group_bundle_index
        for call in fn.options.call_args_list
    ]
    assert bundle_indices == list(range(num_bundles))


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_scheduling_strategy_references_correct_pg(mock_spg_cls):
    """The scheduling_strategy in every .options() call references the
    slice's placement group object."""
    mock_handle = _make_mock_slice_handle(num_bundles=2)
    mock_spg_cls.return_value = mock_handle
    fn = _make_mock_fn()

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            ray.util.tpu.dispatch(fn, topology="2x2x2", accelerator_version="v4")

    for call in fn.options.call_args_list:
        assert (
            call.kwargs["scheduling_strategy"].placement_group
            is mock_handle.placement_group
        )


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_sets_num_cpus_zero_and_tpu_resources(mock_spg_cls):
    """Every task is dispatched with num_cpus=0 and resources={"TPU": N}."""
    tpu_per_bundle = 8
    mock_handle = _make_mock_slice_handle(tpu_per_bundle=tpu_per_bundle)
    mock_spg_cls.return_value = mock_handle
    fn = _make_mock_fn()

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            ray.util.tpu.dispatch(fn, topology="2x2x2", accelerator_version="v4")

    for call in fn.options.call_args_list:
        assert call.kwargs["num_cpus"] == 0
        assert call.kwargs["resources"] == {"TPU": tpu_per_bundle}


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_tpu_count_falls_back_to_chips_per_host(mock_spg_cls):
    """When bundle_resources has no 'TPU' key, the TPU resource count
    falls back to chips_per_host."""
    chips_per_host = 4
    mock_handle = _make_mock_slice_handle(chips_per_host=chips_per_host)
    # Remove the 'TPU' key so the fallback path is exercised.
    mock_handle.bundle_resources = {"CPU": 1.0}
    mock_spg_cls.return_value = mock_handle
    fn = _make_mock_fn()

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            ray.util.tpu.dispatch(fn, topology="2x2x2", accelerator_version="v4")

    for call in fn.options.call_args_list:
        assert call.kwargs["resources"] == {"TPU": chips_per_host}


def test_dispatch_non_remote_fn_raises_type_error():
    """A plain (non-remote) function raises TypeError with a clear message."""

    def plain_fn():
        pass

    with pytest.raises(TypeError, match="@ray.remote"):
        ray.util.tpu.dispatch(plain_fn, topology="2x2x2", accelerator_version="v4")


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_forwards_args_and_kwargs(mock_spg_cls):
    """Positional and keyword arguments are forwarded unchanged to every task."""
    mock_handle = _make_mock_slice_handle(num_bundles=2)
    mock_spg_cls.return_value = mock_handle
    fn = _make_mock_fn()

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            ray.util.tpu.dispatch(
                fn,
                "pos_arg",
                topology="2x2x2",
                accelerator_version="v4",
                my_kwarg="hello",
            )

    # fn.options() returns fn itself (chained mock), so .remote() calls land on fn
    assert fn.remote.call_count == 2
    for call in fn.remote.call_args_list:
        assert call.args == ("pos_arg",)
        assert call.kwargs == {"my_kwarg": "hello"}


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_releases_head_pgs_when_owns_slice(mock_spg_cls):
    """When dispatch creates the slice internally it releases head PGs
    after the placement group becomes ready."""
    mock_handle = _make_mock_slice_handle()
    mock_spg_cls.return_value = mock_handle

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            ray.util.tpu.dispatch(
                _make_mock_fn(), topology="2x2x2", accelerator_version="v4"
            )

    mock_handle.release_head_pgs.assert_called_once()


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_does_not_release_head_pgs_when_provided(mock_spg_cls):
    """When the caller owns the SlicePlacementGroup, dispatch must not
    release its head PGs."""
    existing_handle = _make_mock_slice_handle()

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", return_value=None):
            ray.util.tpu.dispatch(
                _make_mock_fn(),
                topology="2x2x2",
                accelerator_version="v4",
                tpu_slice=existing_handle,
            )

    existing_handle.release_head_pgs.assert_not_called()


def test_dispatch_raises_if_provided_slice_is_shut_down():
    """A clear ValueError is raised when tpu_slice has already been shut down
    (placement_group is None), rather than a confusing AttributeError."""
    shut_down_handle = _make_mock_slice_handle()
    shut_down_handle.placement_group = None

    with pytest.raises(ValueError, match="already been shut down"):
        ray.util.tpu.dispatch(
            _make_mock_fn(),
            tpu_slice=shut_down_handle,
        )


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_pg_ready_exception_shuts_down_owned_slice(mock_spg_cls):
    """If pg.ready() resolves with an exception (e.g. PG was removed),
    ray.wait still returns it as ready. ray.get then surfaces the error;
    the internally-created slice must be shut down before re-raising."""
    mock_handle = _make_mock_slice_handle()
    mock_spg_cls.return_value = mock_handle

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", side_effect=RuntimeError("PG failed")):
            with pytest.raises(RuntimeError, match="PG failed"):
                ray.util.tpu.dispatch(
                    _make_mock_fn(),
                    topology="2x2x2",
                    accelerator_version="v4",
                )

    mock_handle.shutdown.assert_called_once()


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_pg_ready_exception_does_not_shutdown_provided_slice(mock_spg_cls):
    """If pg.ready() resolves with an exception and the slice was provided
    by the caller, shutdown() must not be called."""
    existing_handle = _make_mock_slice_handle()

    with patch.object(ray, "wait", return_value=([MagicMock()], [])):
        with patch.object(ray, "get", side_effect=RuntimeError("PG failed")):
            with pytest.raises(RuntimeError):
                ray.util.tpu.dispatch(
                    _make_mock_fn(),
                    tpu_slice=existing_handle,
                )

    existing_handle.shutdown.assert_not_called()


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_timeout_shuts_down_owned_slice(mock_spg_cls):
    """On a pg_ready timeout, the internally-created slice is shut down
    before the TimeoutError is raised."""
    mock_handle = _make_mock_slice_handle()
    mock_spg_cls.return_value = mock_handle

    with patch.object(ray, "wait", return_value=([], [MagicMock()])):
        with pytest.raises(TimeoutError, match="was not ready within"):
            ray.util.tpu.dispatch(
                _make_mock_fn(),
                topology="2x2x2",
                accelerator_version="v4",
                pg_ready_timeout_s=5.0,
            )

    mock_handle.shutdown.assert_called_once()


@patch("ray.util.tpu.SlicePlacementGroup")
def test_dispatch_timeout_does_not_shutdown_provided_slice(mock_spg_cls):
    """On a pg_ready timeout, a caller-provided slice is never shut down."""
    existing_handle = _make_mock_slice_handle()

    with patch.object(ray, "wait", return_value=([], [MagicMock()])):
        with pytest.raises(TimeoutError):
            ray.util.tpu.dispatch(
                _make_mock_fn(),
                topology="2x2x2",
                accelerator_version="v4",
                tpu_slice=existing_handle,
                pg_ready_timeout_s=5.0,
            )

    existing_handle.shutdown.assert_not_called()


def test_dispatch_integration_basic(ray_tpu_cluster):
    """End-to-end: dispatch dispatches one task per host and all tasks
    complete successfully. Uses the two-host v4-16 (2x2x2) fixture."""

    @ray.remote
    def tpu_work():
        return ray.get_runtime_context().get_node_id()

    refs = ray.util.tpu.dispatch(
        tpu_work,
        topology="2x2x2",
        accelerator_version="v4",
    )

    # 2x2x2 v4 has 2 hosts => 2 refs
    assert len(refs) == 2
    node_ids = ray.get(refs)
    # Each task ran on a distinct node
    assert len(set(node_ids)) == 2


def test_dispatch_integration_with_provided_slice(ray_tpu_cluster):
    """When a SlicePlacementGroup is supplied, dispatch uses it without
    creating or tearing down any extra placement groups."""

    @ray.remote
    def tpu_work():
        return ray.get_runtime_context().get_node_id()

    slice_handle = ray.util.tpu.slice_placement_group(
        topology="2x2x2", accelerator_version="v4"
    )
    ray.get(slice_handle.placement_group.ready())

    refs = ray.util.tpu.dispatch(tpu_work, tpu_slice=slice_handle)
    assert len(refs) == 2
    ray.get(refs)

    # The slice handle is intact: caller can still use and shut it down.
    assert slice_handle.placement_group is not None
    slice_handle.shutdown()


def test_dispatch_integration_multi_slice(ray_tpu_cluster):
    """With num_slices=2 the function reserves both slices and dispatches
    one task per host across both (2 hosts * 2 slices = 4 tasks)."""

    @ray.remote
    def tpu_work():
        return ray.get_runtime_context().get_node_id()

    refs = ray.util.tpu.dispatch(
        tpu_work,
        topology="2x2x2",
        accelerator_version="v4",
        num_slices=2,
    )

    assert len(refs) == 4
    node_ids = ray.get(refs)
    assert len(set(node_ids)) == 4


def test_dispatch_integration_v6e_single_host(ray_v6e_tpu_cluster):
    """A single-host v6e-8 slice produces exactly one ref."""

    @ray.remote
    def tpu_work():
        return ray.get_runtime_context().get_node_id()

    refs = ray.util.tpu.dispatch(
        tpu_work,
        topology="2x4",
        accelerator_version="v6e",
    )

    assert len(refs) == 1
    ray.get(refs)


# Mock data for SubslicePlacementGroup tests.
# Chip coordinates for 4 workers of a 4x4 v6e slice (4 chips/VM each).
# Format per worker: list of (hostname, chip_index, [x, y]) tuples.
_4X4_MOCK_COORDS = [
    [
        ("tpu0", 0, [0, 0]),
        ("tpu0", 1, [0, 1]),
        ("tpu0", 2, [1, 0]),
        ("tpu0", 3, [1, 1]),
    ],
    [
        ("tpu1", 0, [2, 0]),
        ("tpu1", 1, [2, 1]),
        ("tpu1", 2, [3, 0]),
        ("tpu1", 3, [3, 1]),
    ],
    [
        ("tpu2", 0, [0, 2]),
        ("tpu2", 1, [0, 3]),
        ("tpu2", 2, [1, 2]),
        ("tpu2", 3, [1, 3]),
    ],
    [
        ("tpu3", 0, [2, 2]),
        ("tpu3", 1, [2, 3]),
        ("tpu3", 2, [3, 2]),
        ("tpu3", 3, [3, 3]),
    ],
]

_4X4_DISCOVERY_RESULTS = [
    {"node_id": f"node_{i}", "coords": _4X4_MOCK_COORDS[i]} for i in range(4)
]


def _make_dummy_nodes(slice_name: str, topology: str, n_workers: int):
    """Build a list of dummy Ray node dicts for use in mocked tests."""
    return [
        {
            "NodeID": f"node_{i}",
            "Alive": True,
            "Resources": {"TPU": 4},
            "Labels": {
                ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name,
                ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY: str(i),
                ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY: topology,
            },
        }
        for i in range(n_workers)
    ]


@pytest.fixture
def mock_4x4_pgs():
    """Mock PlacementGroup objects for subslice integration tests.

    Clears the subslice runtime cache before each test to prevent
    cross-test pollution.
    """
    ray.util.tpu._tpu_subslice_cache.clear()

    from ray.util.placement_group import PlacementGroup

    mock_head_pg = MagicMock(spec=PlacementGroup)
    mock_worker_pg = MagicMock(spec=PlacementGroup)
    mock_id = MagicMock()
    mock_id.is_nil.return_value = False
    mock_worker_pg.id = mock_id
    mock_worker_pg.bundle_count = 4
    mock_worker_pg.ready.return_value = "ready_ref"

    return mock_head_pg, mock_worker_pg


# ---------------------------------------------------------------------------
# Cluster-aware parent topology resolution
# ---------------------------------------------------------------------------


def _alive_node(topology: str) -> dict:
    """Minimal alive node dict with the given topology label."""
    return {"Alive": True, "Labels": {ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY: topology}}


def _slice_nodes(slice_name: str, topology: str, n_workers: int = 4):
    """Node dicts for one slice with slice-name-prefixed NodeIDs, so multiple
    slices can coexist in a single test without NodeID collisions.
    """
    return [
        {
            "NodeID": f"{slice_name}-w{i}",
            "Alive": True,
            "Resources": {"TPU": 4},
            "Labels": {
                ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name,
                ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY: str(i),
                ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY: topology,
            },
        }
        for i in range(n_workers)
    ]


def _mock_worker_pg():
    """A MagicMock PlacementGroup whose id reports non-nil (i.e. created OK)."""
    from ray.util.placement_group import PlacementGroup

    mock_pg = MagicMock(spec=PlacementGroup)
    mock_id = MagicMock()
    mock_id.is_nil.return_value = False
    mock_pg.id = mock_id
    return mock_pg


# Standard 4x4 → 2x4 subslice cache: workers 0,1 form subslice 0; 2,3 form 1.
_SUBSLICE_2X4_LABELS = {
    "0": {"ray.io/tpu-subslice-2x4": "0"},
    "1": {"ray.io/tpu-subslice-2x4": "0"},
    "2": {"ray.io/tpu-subslice-2x4": "1"},
    "3": {"ray.io/tpu-subslice-2x4": "1"},
}


@pytest.mark.parametrize(
    "subslice, cluster_topos, expected",
    [
        # All valid parents returned, sorted smallest-first.
        ("2x4", ["4x4", "16x16"], ["4x4", "16x16"]),
        # Uses the cluster's actual topology, not the theoretical minimum
        # (regression: 2x2 on a 16x16-only cluster must resolve to 16x16).
        ("2x2", ["16x16"], ["16x16"]),
        # Multiple valid topologies all returned, sorted.
        ("2x2", ["4x4", "8x8", "16x16"], ["4x4", "8x8", "16x16"]),
        # Subslice itself is excluded (no strictly larger parent present).
        ("4x4", ["4x4"], []),
        # No cluster topology can contain the subslice.
        ("16x16", ["4x4"], []),
        # 3D topologies resolve correctly.
        ("2x2x2", ["4x4x4", "8x8x8"], ["4x4x4", "8x8x8"]),
    ],
)
def test_find_valid_parent_topologies(subslice, cluster_topos, expected):
    """Valid parents are all cluster topologies strictly larger than the
    subslice in every axis, sorted smallest-first; the subslice itself is
    excluded.
    """
    nodes = [_alive_node(t) for t in cluster_topos]
    assert _find_valid_parent_topologies(subslice, nodes) == expected


def test_find_valid_parent_topologies_ignores_dead_nodes():
    """Dead nodes' topology labels are not considered."""
    nodes = [
        {"Alive": False, "Labels": {ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY: "4x4"}},
        _alive_node("16x16"),
    ]
    assert _find_valid_parent_topologies("2x2", nodes) == ["16x16"]


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


def test_subslice_placement_group_validation():
    """Test validation and error handling for subslice_placement_group."""
    # Invalid subslice topology for accelerator (non-numeric)
    with pytest.raises(
        ValueError,
        match="is not valid for accelerator version",
    ):
        ray.util.tpu.subslice_placement_group(
            subslice_topology="invalid_topology",
            accelerator_version="v6e",
        )

    # Subslice topology is not supported for this accelerator
    with pytest.raises(
        ValueError,
        match="is not valid for accelerator version",
    ):
        ray.util.tpu.subslice_placement_group(
            subslice_topology="2x2x2",  # 3D not valid for v6e
            accelerator_version="v6e",
        )

    # chips_per_vm must be positive
    with pytest.raises(ValueError, match="chips_per_vm must be positive"):
        ray.util.tpu.subslice_placement_group(
            subslice_topology="2x4",
            accelerator_version="v6e",
            chips_per_vm=0,
        )


# SubslicePlacementGroup mocked integration tests


def test_subslice_placement_group_basic_mocked(mock_4x4_pgs):
    """Test full SubslicePlacementGroup lifecycle with mocked discovery."""
    mock_head_pg, mock_worker_pg = mock_4x4_pgs
    slice_name = "test-slice-basic"
    dummy_nodes = _make_dummy_nodes(slice_name, "4x4", 4)

    all_free = {f"node_{i}": {"TPU": 4} for i in range(4)}

    with (
        patch(
            "ray.util.tpu.reserve_tpu_slice",
            return_value=(slice_name, mock_head_pg),
        ),
        patch("ray.util.tpu.placement_group", return_value=mock_worker_pg),
        patch("ray.nodes", return_value=dummy_nodes),
        patch("ray.get") as mock_ray_get,
        patch(
            "ray._private.state.available_resources_per_node",
            return_value=all_free,
        ),
    ):
        mock_ray_get.side_effect = [None, _4X4_DISCOVERY_RESULTS]

        sg = ray.util.tpu.subslice_placement_group(
            subslice_topology="2x4",
            accelerator_version="v6e",
            chips_per_vm=4,
        )

        assert sg.parent_topology == "4x4"
        assert sg.subslice_topology == "2x4"
        assert sg.subslice_index == 0
        assert sg.slice_name == slice_name
        assert sg.num_hosts == 2  # 2 workers in a 2x4 subslice of 4x4
        assert sg.chips_per_host == 4
        assert sg.bundle_resources == {"CPU": 1, "TPU": 4}
        assert len(sg.bundle_label_selector) == 2

        # Verify cache was populated correctly.
        assert slice_name in ray.util.tpu._tpu_subslice_cache
        cache = ray.util.tpu._tpu_subslice_cache[slice_name]
        assert cache["0"]["ray.io/tpu-subslice-2x4"] == "0"
        assert cache["1"]["ray.io/tpu-subslice-2x4"] == "0"
        assert cache["2"]["ray.io/tpu-subslice-2x4"] == "1"
        assert cache["3"]["ray.io/tpu-subslice-2x4"] == "1"

        sg.shutdown()


def test_subslice_auto_select_skips_busy_first_subslice(mock_4x4_pgs):
    """Auto-select skips a fully-occupied subslice and picks the next idle one.

    Workers 0 and 1 form subslice 0; workers 2 and 3 form subslice 1.
    With subslice 0 fully busy, auto-select should pick subslice 1.
    """
    mock_head_pg, mock_worker_pg = mock_4x4_pgs
    slice_name = "test-slice-skip-busy"
    dummy_nodes = _make_dummy_nodes(slice_name, "4x4", 4)

    # Pre-populate cache so no discovery is needed.
    ray.util.tpu._tpu_subslice_cache[slice_name] = _SUBSLICE_2X4_LABELS

    # Workers 0 and 1 (subslice 0) fully occupied; workers 2 and 3 idle.
    avail = {
        "node_0": {"TPU": 0},
        "node_1": {"TPU": 0},
        "node_2": {"TPU": 4},
        "node_3": {"TPU": 4},
    }

    with (
        patch("ray.util.tpu.placement_group", return_value=mock_worker_pg),
        patch("ray.nodes", return_value=dummy_nodes),
        patch(
            "ray._private.state.available_resources_per_node",
            return_value=avail,
        ),
    ):
        sg = ray.util.tpu.subslice_placement_group(
            subslice_topology="2x4",
            accelerator_version="v6e",
            chips_per_vm=4,
        )

    assert sg.subslice_index == 1
    assert sg.num_hosts == 2
    sg.shutdown()


def test_subslice_release_head_pgs_and_shutdown():
    """Test that release_head_pgs and shutdown are idempotent."""
    from ray.util.placement_group import PlacementGroup

    mock_pg = MagicMock(spec=PlacementGroup)
    mock_head = MagicMock(spec=PlacementGroup)

    sg = SubslicePlacementGroup(
        placement_group=mock_pg,
        parent_topology="4x4",
        subslice_topology="2x4",
        subslice_index=0,
        slice_name="test-slice",
        num_hosts=2,
        chips_per_host=4,
        bundle_resources={"TPU": 4, "CPU": 1},
        head_placement_groups=[mock_head],
    )

    assert len(sg.head_placement_groups) == 1
    sg.release_head_pgs()
    assert len(sg.head_placement_groups) == 0
    # Idempotent
    sg.release_head_pgs()
    assert len(sg.head_placement_groups) == 0

    sg.shutdown()
    assert sg.placement_group is None
    # Idempotent
    sg.shutdown()
    assert sg.placement_group is None


def test_subslice_cache_hit_after_discovery(mock_4x4_pgs):
    """Test that a second subslice request uses the runtime cache, not discovery."""
    mock_head_pg, mock_worker_pg = mock_4x4_pgs
    slice_name = "test-slice-cache"
    dummy_nodes = _make_dummy_nodes(slice_name, "4x4", 4)

    all_free = {f"node_{i}": {"TPU": 4} for i in range(4)}

    with (
        patch(
            "ray.util.tpu.reserve_tpu_slice",
            return_value=(slice_name, mock_head_pg),
        ) as mock_reserve,
        patch("ray.util.tpu.placement_group", return_value=mock_worker_pg),
        patch("ray.nodes", return_value=dummy_nodes),
        patch("ray.get") as mock_ray_get,
        patch(
            "ray._private.state.available_resources_per_node",
            return_value=all_free,
        ),
    ):
        mock_ray_get.side_effect = [None, _4X4_DISCOVERY_RESULTS]

        # First call: triggers discovery and populates the runtime cache.
        sg1 = ray.util.tpu.subslice_placement_group(
            subslice_topology="2x4",
            accelerator_version="v6e",
            chips_per_vm=4,
        )
        assert mock_reserve.call_count == 1
        sg1.shutdown()

        # Second call with the same topology: must hit the runtime cache and
        # not trigger another slice reservation or libtpu discovery.
        mock_reserve.reset_mock()
        sg2 = ray.util.tpu.subslice_placement_group(
            subslice_topology="2x4",
            accelerator_version="v6e",
            chips_per_vm=4,
        )
        assert mock_reserve.call_count == 0  # Cache hit — no discovery.
        sg2.shutdown()


def test_discover_skips_fan_out_when_kv_already_populated(mock_4x4_pgs):
    """When the KV store already has subslice labels for the reserved slice,
    _discover_and_persist_subslices returns the cached data without running the
    libtpu fan-out.

    This covers the concurrent-caller scenario: the first caller discovers the
    slice, persists to KV, then releases the head PG. The second caller was
    blocked waiting for that head PG; when it finally acquires the slice it
    checks the KV (written by the first caller before shutdown) and short-circuits
    rather than repeating the expensive coordinate fan-out.
    """
    mock_head_pg, mock_worker_pg = mock_4x4_pgs
    slice_name = "test-slice-concurrent"

    # Pre-populate the KV store as a concurrent caller would have done.
    preloaded_labels = {
        "0": {"ray.io/tpu-subslice-2x4": "0"},
        "1": {"ray.io/tpu-subslice-2x4": "0"},
        "2": {"ray.io/tpu-subslice-2x4": "1"},
        "3": {"ray.io/tpu-subslice-2x4": "1"},
    }
    ray.experimental.internal_kv._internal_kv_put(
        ray.util.tpu._get_subslice_kv_key(slice_name),
        json.dumps(preloaded_labels).encode(),
        namespace=ray.util.tpu._TPU_SUBSLICE_KV_NAMESPACE,
    )

    with (
        patch(
            "ray.util.tpu.reserve_tpu_slice",
            return_value=(slice_name, mock_head_pg),
        ),
        patch("ray.util.tpu.placement_group", return_value=mock_worker_pg),
        patch("ray.get") as mock_ray_get,
    ):
        # Exactly one ray.get call is expected: for full_slice.placement_group.ready().
        # A second call would indicate the libtpu fan-out ran despite the KV hit.
        mock_ray_get.return_value = None

        result_name, result_labels = ray.util.tpu._discover_and_persist_subslices(
            "4x4", "v6e", 4, None
        )

    assert result_name == slice_name
    assert result_labels == preloaded_labels
    assert mock_ray_get.call_count == 1, (
        f"Expected ray.get called once (for .ready()), got {mock_ray_get.call_count}. "
        "The libtpu fan-out ran despite KV data being present."
    )


def test_discover_single_host_topology_completeness_check(mock_4x4_pgs):
    """Regression: single-host v6e 2x4 (8 chips/VM, 1 bundle) discovery must
    not raise a spurious 'incomplete' error.

    The static _VALID_TOPOLOGY_WORKER_DIMS_2D table returns (1, 2) for "2x4",
    implying 2 expected workers. But with chips_per_vm=8 there is only 1
    bundle (8 chips / 8 chips-per-VM), so the fan-out produces 1 result.
    The completeness check must use full_slice.num_bundles (the runtime value)
    not the static table, otherwise a healthy single-host slice always raises.
    """
    mock_head_pg, mock_worker_pg = mock_4x4_pgs
    slice_name = "test-slice-singlehost"

    # 8 chips covering the full 2x4 grid (x=0..3, y=0..1).
    single_host_coords = [("tpu0", i, [i % 4, i // 4]) for i in range(8)]
    single_host_discovery = [{"node_id": "node_0", "coords": single_host_coords}]

    dummy_nodes = [
        {
            "NodeID": "node_0",
            "Alive": True,
            "Resources": {"TPU": 8},
            "Labels": {
                ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name,
                ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY: "0",
                ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY: "2x4",
            },
        }
    ]

    with (
        patch(
            "ray.util.tpu.reserve_tpu_slice",
            return_value=(slice_name, mock_head_pg),
        ),
        patch("ray.util.tpu.placement_group", return_value=mock_worker_pg),
        patch("ray.nodes", return_value=dummy_nodes),
        patch("ray.get") as mock_ray_get,
    ):
        # ray.get called twice: once for .ready(), once for the 1-task fan-out.
        mock_ray_get.side_effect = [None, single_host_discovery]

        # Must not raise RuntimeError("incomplete: labeled 1 of 2 expected").
        result_name, result_labels = ray.util.tpu._discover_and_persist_subslices(
            "2x4", "v6e", 8, None
        )

    assert result_name == slice_name
    assert "0" in result_labels  # the single worker was labeled
    assert len(result_labels) == 1


def test_discover_raises_when_workers_incomplete(mock_4x4_pgs):
    """If discovery labels fewer workers than the slice has bundles (e.g. a
    worker returns no chip coordinates), _discover_and_persist_subslices raises
    RuntimeError rather than persisting a partial mapping that would later yield
    a placement group with the wrong number of hosts.
    """
    mock_head_pg, mock_worker_pg = mock_4x4_pgs
    slice_name = "test-slice-incomplete"
    dummy_nodes = _make_dummy_nodes(slice_name, "4x4", 4)

    # 4x4 has 4 bundles, but only 3 workers return coordinates; the 4th returns
    # empty coords and is skipped, leaving the mapping incomplete (3 of 4).
    incomplete_results = [
        {"node_id": f"node_{i}", "coords": _4X4_MOCK_COORDS[i]} for i in range(3)
    ] + [{"node_id": "node_3", "coords": []}]

    with (
        patch(
            "ray.util.tpu.reserve_tpu_slice",
            return_value=(slice_name, mock_head_pg),
        ),
        patch("ray.util.tpu.placement_group", return_value=mock_worker_pg),
        patch("ray.nodes", return_value=dummy_nodes),
        patch("ray.get") as mock_ray_get,
    ):
        mock_ray_get.side_effect = [None, incomplete_results]
        with pytest.raises(RuntimeError, match="incomplete"):
            ray.util.tpu._discover_and_persist_subslices("4x4", "v6e", 4, None)

    # Nothing should have been persisted for the incomplete slice.
    assert slice_name not in ray.util.tpu._tpu_subslice_cache


def test_subslice_continues_scheduling_when_kv_lookup_fails():
    """A GCS internal-KV lookup failure during cache refresh is swallowed so
    scheduling proceeds to discovery instead of aborting.
    """
    ray.util.tpu._tpu_subslice_cache.clear()

    nodes = _slice_nodes("slice-x", "4x4")
    avail = {f"slice-x-w{i}": {"TPU": 4} for i in range(4)}  # fully idle

    reached = {}

    def _fake_discover(
        parent_topology, version, chips_per_vm, timeout, target_slice_name=None
    ):
        reached["parent"] = parent_topology
        reached["target"] = target_slice_name
        raise RuntimeError("stop-loop")  # break out of the retry loop

    with (
        patch("ray.nodes", return_value=nodes),
        patch(
            "ray._private.state.available_resources_per_node",
            return_value=avail,
        ),
        patch(
            "ray.experimental.internal_kv._internal_kv_get",
            side_effect=RuntimeError("gcs unavailable"),
        ),
        patch(
            "ray.util.tpu._discover_and_persist_subslices",
            side_effect=_fake_discover,
        ),
        pytest.raises(RuntimeError, match="stop-loop"),
    ):
        ray.util.tpu.subslice_placement_group(
            subslice_topology="2x2",
            accelerator_version="v6e",
            chips_per_vm=4,
        )

    # KV failure did not abort scheduling: we reached discovery, pinned to the
    # specific idle slice.
    assert reached.get("parent") == "4x4"
    assert reached.get("target") == "slice-x"


def test_find_available_subslice_skips_incomplete_subslices():
    """Subslices with fewer workers than the topology requires are skipped.

    Corrupted or partial cache data could leave a subslice entry with too few
    workers. Selecting such a subslice would produce a PG that never becomes
    fully ready. The incomplete subslice must be skipped in favour of a valid
    complete one.
    """
    slice_name = "test-slice-partial"
    subslice_topology = "2x4"  # 2 workers per subslice

    # Subslice 0 has only 1 worker (missing worker-id "1"); subslice 1 is complete.
    worker_labels = {
        "0": {"ray.io/tpu-subslice-2x4": "0"},
        "2": {"ray.io/tpu-subslice-2x4": "1"},
        "3": {"ray.io/tpu-subslice-2x4": "1"},
    }

    dummy_nodes = [
        {
            "NodeID": f"node_{wid}",
            "Alive": True,
            "Resources": {"TPU": 4},
            "Labels": {
                ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name,
                ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY: wid,
            },
        }
        for wid in ["0", "2", "3"]
    ]
    avail_resources = {f"node_{wid}": {"TPU": 4} for wid in ["0", "2", "3"]}
    slice_worker_to_node = {
        (
            node["Labels"][ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY],
            node["Labels"][ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY],
        ): node
        for node in dummy_nodes
    }

    result_ids, result_idx = ray.util.tpu._find_available_subslice(
        slice_name,
        subslice_topology,
        worker_labels,
        avail_resources,
        slice_worker_to_node,
    )

    # Subslice 0 has only 1 worker and must be skipped.
    # Subslice 1 has 2 workers and all are idle, so it is selected.
    assert result_idx == 1
    assert set(result_ids) == {"2", "3"}


def test_subslice_iterates_to_second_slice_when_first_is_occupied():
    """When a cluster has two slices of the same parent topology and the first
    is fully occupied, the second slice's subslice is selected instead of
    raising RuntimeError.
    """
    ray.util.tpu._tpu_subslice_cache.clear()

    # Pre-populate the runtime cache with two 4x4 slices.
    ray.util.tpu._tpu_subslice_cache["slice-A"] = _SUBSLICE_2X4_LABELS
    ray.util.tpu._tpu_subslice_cache["slice-B"] = _SUBSLICE_2X4_LABELS

    dummy_nodes = _slice_nodes("slice-A", "4x4") + _slice_nodes("slice-B", "4x4")

    # slice-A: all workers fully occupied; slice-B: all workers idle.
    avail_resources = {
        **{f"slice-A-w{i}": {"TPU": 0} for i in range(4)},
        **{f"slice-B-w{i}": {"TPU": 4} for i in range(4)},
    }

    with (
        patch("ray.nodes", return_value=dummy_nodes),
        patch("ray.util.tpu.placement_group", return_value=_mock_worker_pg()),
        patch(
            "ray._private.state.available_resources_per_node",
            return_value=avail_resources,
        ),
    ):
        sg = ray.util.tpu.subslice_placement_group(
            subslice_topology="2x4",
            accelerator_version="v6e",
            chips_per_vm=4,
        )

    assert sg.slice_name == "slice-B"
    assert sg.subslice_index in (0, 1)
    assert sg.num_hosts == 2
    sg.shutdown()


def test_subslice_uses_any_valid_parent():
    """When all subslices of the smallest valid parent are occupied, the
    scheduler tries the next larger valid parent topology instead of failing.

    Cluster: one "4x4" slice (smallest parent, both subslices of "2x4" fully
    occupied) and one "16x16" slice (larger parent, an idle "2x4" subslice).
    """
    ray.util.tpu._tpu_subslice_cache.clear()

    slice_small = "slice-4x4"
    slice_large = "slice-16x16"

    # Two "2x4" subslices per slice (2 workers each).
    ray.util.tpu._tpu_subslice_cache[slice_small] = _SUBSLICE_2X4_LABELS
    ray.util.tpu._tpu_subslice_cache[slice_large] = _SUBSLICE_2X4_LABELS

    all_nodes = _slice_nodes(slice_small, "4x4") + _slice_nodes(slice_large, "16x16")

    # "4x4" slice: all workers busy; "16x16" slice: all workers free.
    avail = {
        **{f"{slice_small}-w{i}": {"TPU": 0} for i in range(4)},
        **{f"{slice_large}-w{i}": {"TPU": 4} for i in range(4)},
    }

    with (
        patch("ray.nodes", return_value=all_nodes),
        patch("ray.util.tpu.placement_group", return_value=_mock_worker_pg()),
        patch(
            "ray._private.state.available_resources_per_node",
            return_value=avail,
        ),
    ):
        sg = ray.util.tpu.subslice_placement_group(
            subslice_topology="2x4",
            accelerator_version="v6e",
            chips_per_vm=4,
        )

    # Must have selected from the "16x16" parent, not the occupied "4x4".
    assert sg.parent_topology == "16x16"
    assert sg.slice_name == slice_large
    assert sg.num_hosts == 2
    sg.shutdown()


def test_subslice_same_as_parent_raises_value_error():
    """When the requested subslice topology equals the only topology in the
    cluster, _validate_and_resolve raises ValueError directing the user to
    slice_placement_group().
    """
    ray.util.tpu._tpu_subslice_cache.clear()
    with (
        pytest.raises(ValueError, match="slice_placement_group"),
        patch("ray.nodes", return_value=[_alive_node("4x4")]),
    ):
        ray.util.tpu.subslice_placement_group(
            subslice_topology="4x4",
            accelerator_version="v6e",
            chips_per_vm=4,
        )


def test_find_undiscovered_idle_slice():
    """_find_undiscovered_idle_slice returns (parent_topology, slice_name) for
    the first undiscovered, fully-idle slice, or None.
    """
    ray.util.tpu._tpu_subslice_cache.clear()
    parent_topo = "4x4"

    def _make_slice_nodes(slice_name, node_ids):
        return [
            {
                "NodeID": nid,
                "Alive": True,
                "Resources": {"TPU": 4},
                "Labels": {
                    ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name,
                    ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY: str(i),
                    ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY: parent_topo,
                },
            }
            for i, nid in enumerate(node_ids)
        ]

    def check(parents, nodes, avail):
        return ray.util.tpu._find_undiscovered_idle_slice(parents, nodes, avail, "v6e")

    # No nodes at all → None.
    assert check([parent_topo], [], {}) is None

    nodes = _make_slice_nodes("slice-a", ["n0", "n1"])
    all_free = {"n0": {"TPU": 4}, "n1": {"TPU": 4}}
    one_busy = {"n0": {"TPU": 0}, "n1": {"TPU": 4}}

    # Undiscovered and fully idle → returns (parent_topology, slice_name).
    assert check([parent_topo], nodes, all_free) == (parent_topo, "slice-a")

    # Undiscovered but one worker busy → None.
    assert check([parent_topo], nodes, one_busy) is None

    # Slice present in cache → treated as already discovered → None.
    ray.util.tpu._tpu_subslice_cache["slice-a"] = {}
    assert check([parent_topo], nodes, all_free) is None


def test_resolve_chips_per_vm():
    """chips_per_vm is a per-parent property: the user override always wins,
    otherwise it is derived from the specific parent topology (single-host v6e
    topologies report their full chip count, multi-host report 4).
    """
    resolve = ray.util.tpu._resolve_chips_per_vm
    # Explicit override wins regardless of topology.
    assert resolve(16, "4x4", "v6e") == 16
    # Derived per parent when omitted.
    assert resolve(None, "4x4", "v6e") == 4  # multi-host
    assert resolve(None, "2x4", "v6e") == 8  # single-host (8 chips on one VM)
    assert resolve(None, "2x2", "v6e") == 4  # single-host, 4 chips


def test_subslice_omitted_chips_per_vm_matches_discovered_parent():
    """Regression: on a mixed v6e cluster (single-host 2x4 = 8 chips/VM and
    multi-host 4x4 = 4 chips/VM), omitting chips_per_vm must derive it from the
    parent actually discovered, not the smallest valid parent.

    Here the 2x4 slice (smallest valid parent) is occupied, so discovery falls
    to the 4x4 slice; chips_per_vm passed to discovery must be 4 (for 4x4), not
    8 (2x4's value). With the old code it was baked from parent_topologies[0]
    and discovery would fan out too few workers.
    """
    ray.util.tpu._tpu_subslice_cache.clear()

    # Single-host 2x4 slice (1 node, 8 chips, occupied) + multi-host 4x4 slice
    # (4 nodes, 4 chips each, idle). Nothing cached, forcing discovery.
    nodes = [
        {
            "NodeID": "s24-w0",
            "Alive": True,
            "Resources": {"TPU": 8},
            "Labels": {
                ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: "slice-2x4",
                ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY: "0",
                ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY: "2x4",
            },
        }
    ] + _slice_nodes("slice-4x4", "4x4")

    avail = {
        "s24-w0": {"TPU": 0},  # 2x4 occupied
        **{f"slice-4x4-w{i}": {"TPU": 4} for i in range(4)},  # 4x4 idle
    }

    captured = {}

    def _fake_discover(
        parent_topology, version, chips_per_vm, timeout, target_slice_name=None
    ):
        captured["parent"] = parent_topology
        captured["chips_per_vm"] = chips_per_vm
        raise RuntimeError("stop-loop")  # break out of the retry loop

    with (
        patch("ray.nodes", return_value=nodes),
        patch(
            "ray._private.state.available_resources_per_node",
            return_value=avail,
        ),
        patch(
            "ray.util.tpu._discover_and_persist_subslices",
            side_effect=_fake_discover,
        ),
        pytest.raises(RuntimeError, match="stop-loop"),
    ):
        ray.util.tpu.subslice_placement_group(
            subslice_topology="2x2",
            accelerator_version="v6e",
            # chips_per_vm intentionally omitted
        )

    assert captured["parent"] == "4x4"
    assert captured["chips_per_vm"] == 4  # derived from 4x4, not 2x4's 8


def test_refresh_cache_from_kv_tolerates_corrupt_json():
    """Corrupt persisted KV data is best-effort: the decode error is swallowed
    and the slice is left for fresh discovery rather than aborting the call.
    """
    ray.util.tpu._tpu_subslice_cache.clear()
    nodes = _slice_nodes("slice-c", "4x4")

    with patch(
        "ray.experimental.internal_kv._internal_kv_get",
        return_value=b"{not valid json",
    ):
        ray.util.tpu._refresh_cache_from_kv(["4x4"], nodes)  # must not raise

    assert "slice-c" not in ray.util.tpu._tpu_subslice_cache


def test_find_undiscovered_idle_slice_skips_held_head():
    """A slice with free chips but a held head resource on worker 0 is not
    selected, since a full-slice reservation could not actually complete.
    """
    ray.util.tpu._tpu_subslice_cache.clear()
    pod_type = tpu.infer_tpu_pod_type_from_topology("4x4", "TPU-V6E")
    head_resource = f"TPU-{pod_type}-head"

    nodes = _slice_nodes("slice-h", "4x4")  # workers 0..3, all chips idle
    avail = {f"slice-h-w{i}": {"TPU": 4} for i in range(4)}

    def check(avail):
        return ray.util.tpu._find_undiscovered_idle_slice(["4x4"], nodes, avail, "v6e")

    # Head free on worker 0 → slice is selectable.
    avail["slice-h-w0"] = {"TPU": 4, head_resource: 1}
    assert check(avail) == ("4x4", "slice-h")

    # Head held on worker 0 (reported as 0) → slice skipped despite idle chips.
    avail["slice-h-w0"] = {"TPU": 4, head_resource: 0}
    assert check(avail) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
