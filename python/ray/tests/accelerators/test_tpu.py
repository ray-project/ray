import os
import sys
from unittest import mock
from unittest.mock import patch

import pytest
import requests

from ray._private.accelerators import TPUAcceleratorManager, tpu


@patch("glob.glob")
def test_autodetect_num_tpus_accel(mock_glob):
    mock_glob.return_value = [
        "/dev/accel0",
        "/dev/accel1",
        "/dev/accel2",
        "/dev/accel3",
    ]
    TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
    assert TPUAcceleratorManager.get_current_node_num_accelerators() == 4


@patch("os.path.isdir")
@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_accel_ignores_blackwell_directory(
    mock_list, mock_glob, mock_isdir
):
    # NVIDIA drivers 570.x (Blackwell-class GPUs, e.g. RTX 5090) create
    # /dev/accel as a directory containing /dev/accel/accel0. The non-recursive
    # glob matches the directory entry; filtering directories out keeps real
    # TPU chips (character devices at /dev/accel0..N) while rejecting the
    # NVIDIA false positive.
    mock_glob.return_value = ["/dev/accel"]
    mock_isdir.side_effect = lambda p: p == "/dev/accel"
    mock_list.side_effect = FileNotFoundError
    TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
    assert TPUAcceleratorManager.get_current_node_num_accelerators() == 0


@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_vfio(mock_list, mock_glob):
    mock_glob.return_value = []
    # Four VFIO groups. Each group is backed by a single Google TPU PCI device
    # (vendor 0x1ae0) at /sys/kernel/iommu_groups/<n>/devices/<bdf>/vendor.
    mock_list.side_effect = [
        [f"{i}" for i in range(4)],
        [f"{i:04x}:00:00.0" for i in range(4)],
        [f"{i:04x}:00:00.0" for i in range(4)],
        [f"{i:04x}:00:00.0" for i in range(4)],
        [f"{i:04x}:00:00.0" for i in range(4)],
    ]
    with patch(
        "builtins.open",
        mock.mock_open(read_data="0x1ae0\n"),
    ):
        TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
        assert TPUAcceleratorManager.get_current_node_num_accelerators() == 4


@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_vfio_ignores_non_tpu_vendor(mock_list, mock_glob):
    # Regression test for https://github.com/ray-project/ray/issues/65034.
    # NVIDIA BlueField-3's SoC Management Interface is bound to vfio-pci by
    # RShim and surfaces as /dev/vfio/96. The underlying device is vendor
    # 0x15b3 (Mellanox), not Google (0x1ae0). Ray must NOT count it as a TPU,
    # otherwise NVIDIA GPUs on the same node would fail to register.
    mock_glob.return_value = []
    mock_list.side_effect = [
        ["96"],
        ["0000:c2:00.0"],
    ]
    with patch(
        "builtins.open",
        mock.mock_open(read_data="0x15b3\n"),
    ):
        TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
        assert TPUAcceleratorManager.get_current_node_num_accelerators() == 0


@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_vfio_mixed_groups(mock_list, mock_glob):
    # Two VFIO groups: one is a Google TPU (vendor 0x1ae0), the other is the
    # BlueField-3 SoC (vendor 0x15b3). Only the TPU-backed group is counted.
    mock_glob.return_value = []
    mock_list.side_effect = [
        ["10", "96"],
        ["0000:01:00.0"],
        ["0000:c2:00.0"],
    ]
    open_mock = mock.mock_open()
    open_mock.side_effect = [
        mock.mock_open(read_data="0x1ae0\n").return_value,
        mock.mock_open(read_data="0x15b3\n").return_value,
    ]
    with patch("builtins.open", open_mock):
        TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
        assert TPUAcceleratorManager.get_current_node_num_accelerators() == 1


@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_vfio_missing_sysfs_fails_closed(mock_list, mock_glob):
    # If /sys/kernel/iommu_groups/<group>/devices does not exist (e.g. an
    # unusual sandbox or kernel without IOMMU groups), fail closed and do not
    # count the group as a TPU. False positives here would mask the real
    # accelerators on the node.
    mock_glob.return_value = []
    mock_list.side_effect = [
        ["96"],
        FileNotFoundError,
    ]
    TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
    assert TPUAcceleratorManager.get_current_node_num_accelerators() == 0


@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_vfio_sysfs_eio_fails_closed(mock_list, mock_glob):
    # A bare OSError (e.g. EIO from a wedged sysfs node) must not propagate;
    # that would break Ray startup on hosts with flaky sysfs. Fail closed
    # instead and report zero TPUs for the affected group.
    mock_glob.return_value = []
    mock_list.side_effect = [
        ["96"],
        OSError(5, "Input/output error"),
    ]
    TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
    assert TPUAcceleratorManager.get_current_node_num_accelerators() == 0


@patch("os.listdir")
def test_is_vfio_group_a_tpu_listdir_oserror_fails_closed(mock_list):
    # The helper itself must swallow any OSError from os.listdir — not just
    # FileNotFoundError / PermissionError — otherwise the exception would
    # escape get_current_node_num_accelerators and abort Ray startup.
    mock_list.side_effect = OSError(5, "Input/output error")
    assert tpu._is_vfio_group_a_tpu(96) is False


@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_without_devices(mock_list, mock_glob):
    mock_list.side_effect = FileNotFoundError
    mock_glob.return_value = []
    TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
    assert TPUAcceleratorManager.get_current_node_num_accelerators() == 0


@pytest.mark.parametrize(
    "accelerator_type_version_tuple",
    [
        ("gce", "v2-8", "TPU-V2"),
        ("gce", "v2-32", "TPU-V2"),
        ("gce", "v3-8", "TPU-V3"),
        ("gce", "v3-128", "TPU-V3"),
        ("gce", "v4-8", "TPU-V4"),
        ("gce", "v4-2048", "TPU-V4"),
        ("gce", "v5p-8", "TPU-V5P"),
        ("gce", "v5litepod-8", "TPU-V5LITEPOD"),
        ("gce", "v6e-8", "TPU-V6E"),
        ("gke", "v2-8", "TPU-V2"),
        ("gke", "v2-32", "TPU-V2"),
        ("gke", "v3-8", "TPU-V3"),
        ("gke", "v3-128", "TPU-V3"),
        ("gke", "v4-8", "TPU-V4"),
        ("gke", "v4-2048", "TPU-V4"),
        ("gke", "v5p-8", "TPU-V5P"),
        ("gke", "v5litepod-8", "TPU-V5LITEPOD"),
        ("gke", "v6e-8", "TPU-V6E"),
        ("gke", "tpu7x-16", "TPU-V7X"),
    ],
)
@patch("requests.get")
@patch("os.getenv")
def test_autodetect_tpu_accelerator_type(
    mock_os, mock_request, accelerator_type_version_tuple
):
    gce_or_gke, accelerator_type, expected_version = accelerator_type_version_tuple
    if gce_or_gke == "gce":
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.text = accelerator_type
        mock_request.return_value = mock_response
        mock_os.return_value = None
    else:
        mock_os.return_value = accelerator_type
    assert TPUAcceleratorManager.get_current_node_accelerator_type() == expected_version


@pytest.mark.parametrize(
    "test_case",
    [
        ("gce", "0", 0),
        ("gke", "0", 0),
    ],
)
@patch("requests.get")
@patch("os.getenv")
def test_get_current_node_tpu_worker_id(mock_os, mock_request, test_case):
    gce_or_gke, worker_id, expected_value = test_case
    if gce_or_gke == "gce":
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.text = worker_id
        mock_request.return_value = mock_response
        mock_os.return_value = None
    else:
        mock_os.return_value = worker_id
    assert TPUAcceleratorManager.get_current_node_tpu_worker_id() == expected_value


@pytest.mark.parametrize(
    "test_case",
    [
        ("gce", "my-tpu"),
        ("gke", "my-tpu"),
    ],
)
@patch("requests.get")
@patch("os.getenv")
def test_get_tpu_unique_id(mock_os, mock_request, test_case):
    gce_or_gke, worker_id = test_case
    if gce_or_gke == "gce":
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.text = worker_id
        mock_request.return_value = mock_response
        mock_os.return_value = None
    else:
        mock_os.return_value = worker_id
    assert TPUAcceleratorManager.get_current_node_tpu_name() == worker_id


@pytest.mark.parametrize(
    "test_case",
    [
        ("gce", "not-a-valid-version"),
        ("gce", "vNOTVALID-8"),
        ("gce", "230498230948230948"),
        # From issue #39913
        ("gce", ""),
        ("gke", "not-a-valid-version"),
        ("gke", "vNOTVALID-8"),
        ("gke", "230498230948230948"),
    ],
)
@patch("requests.get")
@patch("os.getenv")
def test_autodetect_invalid_type(mock_os, mock_request, test_case):
    gce_or_gke, accelerator_type = test_case
    if gce_or_gke == "gce":
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.text = accelerator_type
        mock_request.return_value = mock_response
        mock_os.return_value = None
    else:
        mock_os.return_value = accelerator_type
    assert TPUAcceleratorManager.get_current_node_accelerator_type() is None


def test_autodetect_tpu_accelerator_type_fails_gracefully():
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.exceptions.RequestException
        assert TPUAcceleratorManager.get_current_node_accelerator_type() is None


@pytest.mark.parametrize(
    "test_config",
    [
        (1, False),
        (0.5, True),
        (3, True),
    ],
)
def test_validate_resource_request_quantity(test_config):
    num_tpus, expect_error = test_config

    if expect_error:
        assert (
            TPUAcceleratorManager.validate_resource_request_quantity(num_tpus)[0]
            is False
        )
        assert (
            TPUAcceleratorManager.validate_resource_request_quantity(num_tpus)[1]
            is not None
        )
    else:
        assert (
            TPUAcceleratorManager.validate_resource_request_quantity(num_tpus)[0]
            is True
        )
        assert (
            TPUAcceleratorManager.validate_resource_request_quantity(num_tpus)[1]
            is None
        )


@pytest.mark.parametrize(
    "test_case",
    [
        (4, ["0"]),
        (4, ["0", "1"]),
        (4, ["0", "1", "2", "3"]),
        (8, ["0", "1", "2", "3", "4", "5", "6", "7"]),
    ],
)
@patch("glob.glob")
def test_set_tpu_visible_ids_and_bounds(mock_glob, test_case):
    num_devices, tpu_chips = test_case
    mock_glob.return_value = ["/dev/accel" + str(x) for x in range(num_devices)]
    with patch.dict("os.environ", {}, clear=True):
        TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
        TPUAcceleratorManager.set_current_process_visible_accelerator_ids(tpu_chips)
        if len(tpu_chips) == 1:
            assert (
                os.environ[tpu.TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR]
                == tpu.TPU_CHIPS_PER_HOST_BOUNDS_1_CHIP_CONFIG
            )
            assert os.environ[tpu.TPU_HOST_BOUNDS_ENV_VAR] == tpu.TPU_SINGLE_HOST_BOUNDS
            assert os.environ[tpu.TPU_VISIBLE_CHIPS_ENV_VAR] == ",".join(tpu_chips)
        elif len(tpu_chips) == 2:
            assert (
                os.environ[tpu.TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR]
                == tpu.TPU_CHIPS_PER_HOST_BOUNDS_2_CHIP_CONFIG
            )
            assert os.environ[tpu.TPU_HOST_BOUNDS_ENV_VAR] == tpu.TPU_SINGLE_HOST_BOUNDS
            assert os.environ[tpu.TPU_VISIBLE_CHIPS_ENV_VAR] == ",".join(tpu_chips)
        elif len(tpu_chips) == 4:
            # Check that nothing is set, let the ML framework use the defaults.
            assert os.environ.get(tpu.TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR, None) is None
            assert os.environ.get(tpu.TPU_SINGLE_HOST_BOUNDS, None) is None
            assert os.environ.get(tpu.TPU_VISIBLE_CHIPS_ENV_VAR, None) is None
        else:  # len(tpu_chips) == 8
            assert os.environ.get(tpu.TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR, None) is None
            assert os.environ.get(tpu.TPU_SINGLE_HOST_BOUNDS, None) is None
            assert os.environ.get(tpu.TPU_VISIBLE_CHIPS_ENV_VAR, None) is None


@pytest.mark.parametrize(
    "test_config",
    [
        (0, "v4-16", {"TPU-v4-16-head": 1, "my-tpu": 1}),
        (1, "v4-16", {"my-tpu": 1}),
        (0, "tpu7x-16", {"TPU-v7x-16-head": 1, "my-tpu": 1}),
    ],
)
def test_tpu_pod_detect_and_configure_worker(test_config):
    worker_id, pod_type, expected_value = test_config
    final_resources = {}
    with patch(
        "ray._private.accelerators.tpu.TPUAcceleratorManager.get_current_node_tpu_name",
        return_value="my-tpu",
    ):
        with patch(
            "ray._private.accelerators.tpu.TPUAcceleratorManager.get_current_node_tpu_worker_id",
            return_value=worker_id,
        ):
            with patch.dict(os.environ, {"TPU_ACCELERATOR_TYPE": pod_type}):
                final_resources = (
                    TPUAcceleratorManager.get_current_node_additional_resources()
                )

    assert final_resources == expected_value


@pytest.mark.parametrize(
    "accelerator_type, expected",
    [
        ("v2-8", True),
        ("v3-32", True),
        ("v4-8", True),
        ("v5p-8", True),
        ("v5litepod-8", True),
        ("v6e-8", True),
        ("tpu7x-16", True),
        ("v7x-16", True),
        ("v-8", False),
        ("8", False),
        ("tpu-8", False),
        ("v2", False),
        ("v2-", False),
        ("random-string", False),
    ],
)
def test_is_valid_tpu_accelerator_type(accelerator_type, expected):
    assert (
        TPUAcceleratorManager.is_valid_tpu_accelerator_type(accelerator_type)
        == expected
    )


def test_get_total_chips_from_accelerator_type():
    assert tpu.get_total_chips_from_accelerator_type("v6e-16") == 16
    assert tpu.get_total_chips_from_accelerator_type("v6e-8") == 8
    assert (
        tpu.get_total_chips_from_accelerator_type("v7x-16") == 8
    )  # v7x has 2 cores per chip
    assert (
        tpu.get_total_chips_from_accelerator_type("v4-8") == 4
    )  # v4 has 2 cores per chip

    # Test invalid cases
    with pytest.raises(ValueError, match="Accelerator type must include size"):
        tpu.get_total_chips_from_accelerator_type("v6e")

    with pytest.raises(ValueError, match="Invalid accelerator type"):
        tpu.get_total_chips_from_accelerator_type("invalid-8")


def test_get_num_tpu_visible_chips_per_host():
    # v6e multi-host (4 chips per VM)
    assert tpu.get_num_tpu_visible_chips_per_host("v6e-16") == 4
    assert tpu.get_num_tpu_visible_chips_per_host("v6e-32") == 4

    # v6e single-host/sub-host (exact chip count)
    assert tpu.get_num_tpu_visible_chips_per_host("v6e-8") == 8
    assert tpu.get_num_tpu_visible_chips_per_host("v6e-4") == 4
    assert tpu.get_num_tpu_visible_chips_per_host("v6e-1") == 1

    # v5litepod multi-host defaults to 4, single-host is 8 chips
    assert tpu.get_num_tpu_visible_chips_per_host("v5litepod-16") == 4
    assert tpu.get_num_tpu_visible_chips_per_host("v5litepod-8") == 8

    # v5litepod sub-host
    assert tpu.get_num_tpu_visible_chips_per_host("v5litepod-4") == 4
    assert tpu.get_num_tpu_visible_chips_per_host("v5litepod-1") == 1

    # Other TPU generations default to 4
    assert tpu.get_num_tpu_visible_chips_per_host("v4-8") == 4
    assert tpu.get_num_tpu_visible_chips_per_host("v5p-8") == 4


# Subslice topology test helpers


def test_parse_topology_dims():
    """Test topology string parsing."""
    assert tpu._parse_topology_dims("2x4") == (2, 4)
    assert tpu._parse_topology_dims("16x16") == (16, 16)
    assert tpu._parse_topology_dims("2x2x2") == (2, 2, 2)
    assert tpu._parse_topology_dims("4x8x16") == (4, 8, 16)


def test_get_worker_dims_2d():
    """Test worker dimension lookup for 2D topologies."""
    assert tpu._get_worker_dims_for_topology("2x4") == (1, 2)
    assert tpu._get_worker_dims_for_topology("4x4") == (2, 2)
    assert tpu._get_worker_dims_for_topology("8x16") == (4, 8)


def test_get_worker_dims_3d():
    """Test worker dimension lookup for 3D topologies."""
    assert tpu._get_worker_dims_for_topology("2x2x2") == (1, 1, 2)
    assert tpu._get_worker_dims_for_topology("4x4x4") == (2, 2, 4)


def test_get_worker_dims_unknown():
    """Test that unknown topologies raise ValueError."""
    with pytest.raises(ValueError, match="Unknown 2D topology"):
        tpu._get_worker_dims_for_topology("99x99")


def test_get_default_chips_per_vm():
    """Test default chips per VM."""
    # v6e single-host: total chips
    assert tpu._get_default_chips_per_vm("2x4", "v6e") == 8
    assert tpu._get_default_chips_per_vm("2x2", "v6e") == 4
    assert tpu._get_default_chips_per_vm("1x1", "v6e") == 1
    # v6e multi-host: 4 chips
    assert tpu._get_default_chips_per_vm("4x8", "v6e") == 4
    # v4/v5p: always 4
    assert tpu._get_default_chips_per_vm("2x2x2", "v4") == 4


@pytest.mark.parametrize(
    "physical_worker_id, parent_topology, expected_labels",
    [
        # 4x4 parent, 4 workers at positions (0,0), (1,0), (0,1), (1,1)
        (0, "4x4", {"ray.io/tpu-subslice-2x2": "0", "ray.io/tpu-subslice-2x4": "0"}),
        (1, "4x4", {"ray.io/tpu-subslice-2x2": "1", "ray.io/tpu-subslice-2x4": "0"}),
        (2, "4x4", {"ray.io/tpu-subslice-2x2": "2", "ray.io/tpu-subslice-2x4": "1"}),
        (3, "4x4", {"ray.io/tpu-subslice-2x2": "3", "ray.io/tpu-subslice-2x4": "1"}),
    ],
)
def test_build_subslice_labels_2d(physical_worker_id, parent_topology, expected_labels):
    """Test subslice label computation for 2D topologies."""
    labels = tpu._build_subslice_labels(physical_worker_id, parent_topology)
    for key, value in expected_labels.items():
        assert labels[key] == value


def test_build_subslice_labels_3d():
    """Test subslice label computation for 3D."""
    # 4x4x4 parent, 16 workers: (z,y,x)
    # Worker 0 → (0,0,0)
    labels = tpu._build_subslice_labels(0, "4x4x4")
    assert labels["ray.io/tpu-subslice-2x2x1"] == "0"
    assert labels["ray.io/tpu-subslice-2x2x2"] == "0"

    # Worker 8 → (1,0,0): z=1, y=0, x=0
    labels = tpu._build_subslice_labels(8, "4x4x4")
    assert labels["ray.io/tpu-subslice-2x2x1"] == "8"
    assert labels["ray.io/tpu-subslice-2x2x2"] == "4"


@pytest.mark.parametrize(
    "coords, parent_topology, expected_worker_id",
    [
        # 4x4 v6e — 4 workers in a 2×2 mesh
        ([[0, 0], [0, 1], [1, 0], [1, 1]], "4x4", 0),
        ([[2, 0], [2, 1], [3, 0], [3, 1]], "4x4", 1),
        ([[0, 2], [0, 3], [1, 2], [1, 3]], "4x4", 2),
        ([[2, 2], [2, 3], [3, 2], [3, 3]], "4x4", 3),
        # 2x4 single-host v6e (8 chips on one VM — worker at the origin)
        (
            [[0, 0], [0, 1], [0, 2], [0, 3], [1, 0], [1, 1], [1, 2], [1, 3]],
            "2x4",
            0,
        ),
    ],
)
def test_get_physical_worker_id_2d(coords, parent_topology, expected_worker_id):
    """Test physical worker ID computation from 2D chip coordinates."""
    assert (
        tpu._get_physical_worker_id_from_coords(coords, parent_topology)
        == expected_worker_id
    )


@pytest.mark.parametrize(
    "coords, parent_topology, expected_worker_id",
    [
        # 4x4x4: worker grid (z,y,x)=(2,2,4); each worker owns 1 chip in x,
        # 2 in y, 2 in z. Coords are [x, y, z].
        # Worker 0: x=0, y in {0,1}, z in {0,1}.
        ([[0, 0, 0], [0, 1, 0], [0, 0, 1], [0, 1, 1]], "4x4x4", 0),
        # Worker 1: wx=1 (x=1).
        ([[1, 0, 0], [1, 1, 0], [1, 0, 1], [1, 1, 1]], "4x4x4", 1),
        # Worker 8: wz=1 (z in {2,3}) → linear = wz*(dy*dx) = 1*(2*4) = 8.
        ([[0, 0, 2], [0, 1, 2], [0, 0, 3], [0, 1, 3]], "4x4x4", 8),
    ],
)
def test_get_physical_worker_id_3d(coords, parent_topology, expected_worker_id):
    """Test physical worker ID computation from 3D chip coordinates."""
    assert (
        tpu._get_physical_worker_id_from_coords(coords, parent_topology)
        == expected_worker_id
    )


@pytest.mark.parametrize(
    "coords, parent_topology",
    [
        # 2D: x coordinate far outside the 4x4 chip grid → wx out of bounds.
        ([[99, 0], [99, 1]], "4x4"),
        # 3D: z coordinate outside the 4x4x4 grid → wz out of bounds.
        ([[0, 0, 99], [1, 0, 99]], "4x4x4"),
    ],
)
def test_get_physical_worker_id_out_of_bounds(coords, parent_topology):
    """Bad/partial libtpu coordinates that map outside the worker mesh raise
    ValueError in both the 2D and 3D branches, rather than silently producing
    an incorrect subslice label.
    """
    with pytest.raises(ValueError, match="out of bounds"):
        tpu._get_physical_worker_id_from_coords(coords, parent_topology)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
