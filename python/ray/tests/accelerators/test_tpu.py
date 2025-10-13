import os
import sys
from unittest import mock
from unittest.mock import patch

import pytest
import requests

import ray
from ray._private.accelerators import TPUAcceleratorManager, tpu
from ray.tests.conftest import _ray_start_cluster


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


@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_vfio(mock_list, mock_glob):
    mock_glob.return_value = []
    mock_list.return_value = [f"{i}" for i in range(4)]
    TPUAcceleratorManager.get_current_node_num_accelerators.cache_clear()
    assert TPUAcceleratorManager.get_current_node_num_accelerators() == 4


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
        (0, {"TPU-v4-16-head": 1, "my-tpu": 1}),
        (1, {"my-tpu": 1}),
    ],
)
def test_tpu_pod_detect_and_configure_worker(test_config):
    worker_id, expected_value = test_config
    final_resources = {}
    with patch(
        "ray._private.accelerators.tpu.TPUAcceleratorManager.get_current_node_tpu_name",
        return_value="my-tpu",
    ):
        with patch(
            "ray._private.accelerators.tpu.TPUAcceleratorManager."
            "get_current_node_tpu_pod_type",
            return_value="v4-16",
        ):
            with patch(
                "ray._private.accelerators.tpu.TPUAcceleratorManager"
                ".get_current_node_tpu_worker_id",
                return_value=worker_id,
            ):
                final_resources = (
                    TPUAcceleratorManager.get_current_node_additional_resources()
                )

    assert final_resources == expected_value


def test_get_current_pod_name_smoke():
    with patch(
        "ray._private.accelerators.tpu.TPUAcceleratorManager.get_current_node_tpu_name",
        return_value="my-tpu",
    ):
        name = ray.util.accelerators.tpu.get_current_pod_name()
    assert name == "my-tpu"


def test_empty_get_current_pod_name_returns_none():
    with patch(
        "ray._private.accelerators.tpu.TPUAcceleratorManager.get_current_node_tpu_name",
        return_value="",
    ):
        name = ray.util.accelerators.tpu.get_current_pod_name()
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
        worker_count = ray.util.accelerators.tpu.get_current_pod_worker_count()

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
    num_tpu_chips = ray.util.accelerators.tpu.get_num_tpu_chips_on_node()
    assert num_tpu_chips == 4


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
    "topology, accelerator_type, expected_pod_type",
    [
        ("2x4", "TPU-V6E", "v6e-8"),
        ("2x2x2", "TPU-V4", "v4-8"),
        ("2x4x4", "TPU-V3", "v3-32"),
        ("4x4", "TPU-V5P", "v5p-16"),
        ("8x16", "TPU-V6E", "v6e-128"),
        ("", "TPU-V3", None),
        ("4x", "TPU-V3", None),
    ],
)
def test_infer_tpu_pod_type_from_topology(
    topology, accelerator_type, expected_pod_type
):
    assert (
        tpu.infer_tpu_pod_type_from_topology(topology, accelerator_type)
        == expected_pod_type
    )


@pytest.fixture
def ray_start_cpu():
    address_info = ray.init(num_cpus=1)
    yield address_info
    ray.shutdown()


@pytest.fixture
def ray_tpu_cluster(monkeypatch):
    """Start a mock TPU Ray cluster."""
    with _ray_start_cluster() as cluster:
        monkeypatch.setenv("TPU_NAME", "test-slice-0")
        monkeypatch.setenv("TPU_WORKER_ID", "0")
        monkeypatch.setenv("TPU_ACCELERATOR_TYPE", "v4-8")
        monkeypatch.setenv("TPU_TOPOLOGY", "2x2x2")

        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4, "TPU-v4-8-head": 1},
        )
        monkeypatch.setenv("TPU_WORKER_ID", "1")
        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4},
        )
        ray.init(address=cluster.address)

        yield cluster
        ray.shutdown()


def test_fetch_tpu_slice_name_from_pg(ray_tpu_cluster):
    """Tests that the slice name can be fetched from a PG."""
    tpu_head_pg = ray.util.placement_group(bundles=[{"TPU-v4-8-head": 1}])
    ray.get(tpu_head_pg.ready())

    tpu_slice_name = "test-slice-0"
    slice_name = tpu.fetch_tpu_slice_name_from_pg(tpu_head_pg)
    assert slice_name == tpu_slice_name

    ray.util.remove_placement_group(tpu_head_pg)


def test_reserve_tpu_slice(ray_tpu_cluster):
    """Tests that a TPU slice can be successfully reserved."""
    tpu_slice_name = "test-slice-0"
    reserved_name = tpu.reserve_tpu_slice(topology="2x2x2", accelerator_type="TPU-V4")
    assert reserved_name == tpu_slice_name


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
