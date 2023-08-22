import mock
import pytest

import os

import logging
from unittest.mock import patch
import requests

import ray._private.accelerator as accelerator
import ray._private.utils as utils
import ray._private.ray_constants as ray_constants
from ray._private.ray_option_utils import _validate_accelerators
from ray.util.accelerators.accelerators import AWS_NEURON_CORE


def test_configured_aws_neuron_core():
    resources = {"CPU": 1, "neuron_cores": 4}
    accelerator.update_resources_with_accelerator_type(resources)
    assert resources.get(utils.get_neuron_core_constraint_name()) == 4
    assert resources.get(ray_constants.NEURON_CORES) == 4


@patch("ray._private.utils.get_aws_neuron_core_visible_ids", return_value=[0, 1, 2])
def test_aws_neuron_core_with_more_user_configured(mock_get_nc_ids):
    resources = {"CPU": 1, "neuron_cores": 4}
    with pytest.raises(ValueError):
        accelerator.update_resources_with_accelerator_type(resources)
    assert mock_get_nc_ids.called


@patch("ray._private.accelerator._autodetect_aws_neuron_cores", return_value=2)
def test_auto_detect_aws_neuron_core(mock_autodetect_aws_neuron_cores):
    resources = {"CPU": 1}
    accelerator.update_resources_with_accelerator_type(resources)
    assert mock_autodetect_aws_neuron_cores.called
    assert resources.get(utils.get_neuron_core_constraint_name()) == 2
    assert resources.get(ray_constants.NEURON_CORES) == 2


@patch("ray._private.utils.get_aws_neuron_core_visible_ids", return_value=[0, 1, 2])
@patch("ray._private.accelerator._autodetect_aws_neuron_cores", return_value=4)
def test_auto_detect_nc_with_more_user_configured(
    mock_get_nc_ids, mock_autodetect_aws_neuron_cores
):
    resources = {"CPU": 1}
    accelerator.update_resources_with_accelerator_type(resources)
    assert mock_get_nc_ids.called
    assert mock_autodetect_aws_neuron_cores.called
    assert resources.get(utils.get_neuron_core_constraint_name()) == 3
    assert resources.get(ray_constants.NEURON_CORES) == 3


@patch("subprocess.run")
def test_get_neuron_core_count_single_device(mock_subprocess):
    mock_subprocess.return_value.returncode = 0
    mock_subprocess.return_value.stdout = (
        b'[{"neuron_device":0,"bdf":"00:1e.0",'
        b'"connected_to":null,"nc_count":2,'
        b'"memory_size":34359738368,"neuron_processes":[]}]'
    )
    assert accelerator._get_neuron_core_count() == 2
    assert mock_subprocess.called


@patch("subprocess.run")
def test_get_neuron_core_count_multiple_devices(mock_subprocess):
    mock_subprocess.return_value.returncode = 0
    mock_subprocess.return_value.stdout = (
        b'[{"neuron_device":0,"bdf":"00:1e.0",'
        b'"connected_to":null,"nc_count":2,'
        b'"memory_size":34359738368,"neuron_processes":[]},'
        b'{"neuron_device":1,"bdf":"00:1f.0","connected_to":null,'
        b'"nc_count":2,"memory_size":34359738368,"neuron_processes":[]}]'
    )
    assert accelerator._get_neuron_core_count() == 4
    assert mock_subprocess.called


@patch("subprocess.run")
def test_get_neuron_core_count_failure_with_error(mock_subprocess):
    mock_subprocess.return_value.returncode = 1
    mock_subprocess.return_value.stderr = b"AccessDenied"
    assert accelerator._get_neuron_core_count() == 0
    assert mock_subprocess.called


@patch("subprocess.run")
def test_get_neuron_core_count_failure_with_empty_results(mock_subprocess):
    mock_subprocess.return_value.returncode = 0
    mock_subprocess.return_value.stdout = b"[{}]"
    assert accelerator._get_neuron_core_count() == 0
    assert mock_subprocess.called


@patch("glob.glob")
def test_autodetect_num_tpus_accel(mock_glob):
    mock_glob.return_value = [
        "/dev/accel0",
        "/dev/accel1",
        "/dev/accel2",
        "/dev/accel3",
    ]
    assert accelerator.autodetect_num_tpus() == 4


@patch("glob.glob")
def test_autodetect_num_tpus_vfio(mock_glob):
    mock_glob.return_value = [f"/dev/vfio/{i}" for i in range(4)]
    assert accelerator.autodetect_num_tpus() == 4


@pytest.mark.parametrize(
    "accelerator_type_version_tuple",
    [
        ("v2-8", "TPU-V2"),
        ("v2-32", "TPU-V2"),
        ("v3-8", "TPU-V3"),
        ("v3-128", "TPU-V3"),
        ("v4-8", "TPU-V4"),
        ("v4-2048", "TPU-V4"),
    ],
)
@patch("requests.get")
def test_autodetect_tpu_version_gce(mock_request, accelerator_type_version_tuple):
    accelerator_type, expected_version = accelerator_type_version_tuple
    mock_response = mock.MagicMock()
    mock_response.status_code = 200
    mock_response.text = accelerator_type
    mock_request.return_value = mock_response
    assert accelerator.autodetect_tpu_version() == expected_version


@pytest.mark.parametrize(
    "accelerator_type_version_tuple",
    [
        ("v2-8", "TPU-V2"),
        ("v2-32", "TPU-V2"),
        ("v3-8", "TPU-V3"),
        ("v3-128", "TPU-V3"),
        ("v4-8", "TPU-V4"),
        ("v4-2048", "TPU-V4"),
    ],
)
@patch("os.getenv")
def test_autodetect_tpu_version_gke_v2(mock_os, accelerator_type_version_tuple):
    accelerator_type, expected_version = accelerator_type_version_tuple
    mock_os.return_value = accelerator_type
    assert accelerator.autodetect_tpu_version() == expected_version


def test_autodetect_tpu_fails_gracefully():
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.exceptions.RequestException
        tpu_result = accelerator.autodetect_tpu_version()
        assert tpu_result is None


@pytest.mark.parametrize(
    "test_config",
    [
        (1, 0, 0, False, False),
        (0, 1, 0, False, False),
        (0, 0, 1, False, False),
        (0, 0, 0, True, False),
        (1, 1, 0, False, True),
        (0, 1, 1, False, True),
        (0, 1, 0, True, True),
        (1, 0, 0, True, True),
    ],
)
def test_validate_accelerator_options(test_config):
    num_gpus, num_tpus, num_neuron_cores, use_neuron_acc, expect_error = test_config
    options = {
        "num_tpus": num_tpus,
        "num_gpus": num_gpus,
    }

    if use_neuron_acc:
        options["accelerator_type"] = AWS_NEURON_CORE
    if num_neuron_cores > 0:
        options["resources"] = {"neuron_cores": 1}

    if expect_error:
        with pytest.raises(ValueError):
            _validate_accelerators(options)
    else:
        # Should run without raising an error
        _validate_accelerators(options)


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
        utils.set_tpu_visible_ids_and_bounds(tpu_chips=tpu_chips)
        assert os.environ[ray_constants.TPU_VISIBLE_CHIPS_ENV_VAR] == ",".join(
            tpu_chips
        )
        if len(tpu_chips) == 1:
            assert (
                os.environ[ray_constants.TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR]
                == ray_constants.TPU_CHIPS_PER_HOST_BOUNDS_1_CHIP_CONFIG
            )
            assert (
                os.environ[ray_constants.TPU_HOST_BOUNDS_ENV_VAR]
                == ray_constants.TPU_SINGLE_HOST_BOUNDS
            )
        elif len(tpu_chips) == 2:
            assert (
                os.environ[ray_constants.TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR]
                == ray_constants.TPU_CHIPS_PER_HOST_BOUNDS_2_CHIP_CONFIG
            )
            assert (
                os.environ[ray_constants.TPU_HOST_BOUNDS_ENV_VAR]
                == ray_constants.TPU_SINGLE_HOST_BOUNDS
            )
        else:  # len(tpu_chips) == 4
            # Check that nothing is set, let the ML framework use the defaults.
            assert (
                os.environ.get(ray_constants.TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR) is None
            )
            assert os.environ.get(ray_constants.TPU_SINGLE_HOST_BOUNDS, None) is None


@pytest.mark.parametrize("num_tpus", [3, 8, 10, 0.3, 0.2])
def test_invalid_tpu_chip_configuration_warning(propagate_logs, caplog, num_tpus):
    options = {"num_tpus": num_tpus}
    with caplog.at_level(logging.WARNING, logger="ray._private.ray_option_utils"):
        _validate_accelerators(options)
        assert "not a supported chip configuration" in caplog.text


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
