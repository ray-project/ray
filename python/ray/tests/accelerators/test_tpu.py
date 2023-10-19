import os
import sys
import mock
import pytest
import requests
from unittest.mock import patch

from ray._private.accelerators import TPUAcceleratorManager
from ray._private.accelerators import tpu


@patch("glob.glob")
def test_autodetect_num_tpus_accel(mock_glob):
    mock_glob.return_value = [
        "/dev/accel0",
        "/dev/accel1",
        "/dev/accel2",
        "/dev/accel3",
    ]
    assert TPUAcceleratorManager.get_current_node_num_accelerators() == 4


@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_vfio(mock_list, mock_glob):
    mock_glob.return_value = []
    mock_list.return_value = [f"{i}" for i in range(4)]
    assert TPUAcceleratorManager.get_current_node_num_accelerators() == 4


@patch("glob.glob")
@patch("os.listdir")
def test_autodetect_num_tpus_without_devices(mock_list, mock_glob):
    mock_list.side_effect = FileNotFoundError
    mock_glob.return_value = []
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
        ("gke", "v2-8", "TPU-V2"),
        ("gke", "v2-32", "TPU-V2"),
        ("gke", "v3-8", "TPU-V3"),
        ("gke", "v3-128", "TPU-V3"),
        ("gke", "v4-8", "TPU-V4"),
        ("gke", "v4-2048", "TPU-V4"),
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
    "tpu_chips",
    [
        ["1"],
        ["1", "2"],
        ["1", "2", "3", "4"],
    ],
)
def test_set_tpu_visible_ids_and_bounds(tpu_chips):
    with patch.dict("os.environ", {}, clear=True):
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
        else:  # len(tpu_chips) == 4
            # Check that nothing is set, let the ML framework use the defaults.
            assert os.environ.get(tpu.TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR, None) is None
            assert os.environ.get(tpu.TPU_SINGLE_HOST_BOUNDS, None) is None
            assert os.environ.get(tpu.TPU_VISIBLE_CHIPS_ENV_VAR, None) is None


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
