import mock
import pytest

import ray._private.accelerator as accelerator
import ray._private.utils as utils
import ray._private.ray_constants as ray_constants


def test_configured_aws_neuron_core():
    resources = {"CPU": 1, "neuron_cores": 4}
    accelerator.update_resources_with_accelerator_type(resources)
    assert resources.get(utils.get_neuron_core_constraint_name()) == 4
    assert resources.get(ray_constants.NEURON_CORES) == 4


@mock.patch(
    "ray._private.utils.get_aws_neuron_core_visible_ids", return_value=[0, 1, 2]
)
def test_aws_neuron_core_with_more_user_configured(mock_get_nc_ids):
    resources = {"CPU": 1, "neuron_cores": 4}
    with pytest.raises(ValueError):
        accelerator.update_resources_with_accelerator_type(resources)
    assert mock_get_nc_ids.called


@mock.patch("ray._private.accelerator._autodetect_aws_neuron_cores", return_value=2)
def test_auto_detect_aws_neuron_core(mock_autodetect_aws_neuron_cores):
    resources = {"CPU": 1}
    accelerator.update_resources_with_accelerator_type(resources)
    assert mock_autodetect_aws_neuron_cores.called
    assert resources.get(utils.get_neuron_core_constraint_name()) == 2
    assert resources.get(ray_constants.NEURON_CORES) == 2


@mock.patch(
    "ray._private.utils.get_aws_neuron_core_visible_ids", return_value=[0, 1, 2]
)
@mock.patch("ray._private.accelerator._autodetect_aws_neuron_cores", return_value=4)
def test_auto_detect_nc_with_more_user_configured(
    mock_get_nc_ids, mock_autodetect_aws_neuron_cores
):
    resources = {"CPU": 1}
    accelerator.update_resources_with_accelerator_type(resources)
    assert mock_get_nc_ids.called
    assert mock_autodetect_aws_neuron_cores.called
    assert resources.get(utils.get_neuron_core_constraint_name()) == 3
    assert resources.get(ray_constants.NEURON_CORES) == 3


@mock.patch("subprocess.run")
def test_get_neuron_core_count_single_device(mock_subprocess):
    mock_subprocess.return_value.returncode = 0
    mock_subprocess.return_value.stdout = (
        b'[{"neuron_device":0,"bdf":"00:1e.0",'
        b'"connected_to":null,"nc_count":2,'
        b'"memory_size":34359738368,"neuron_processes":[]}]'
    )
    assert accelerator._get_neuron_core_count() == 2
    assert mock_subprocess.called


@mock.patch("subprocess.run")
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


@mock.patch("subprocess.run")
def test_get_neuron_core_count_failure_with_error(mock_subprocess):
    mock_subprocess.return_value.returncode = 1
    mock_subprocess.return_value.stderr = b"AccessDenied"
    assert accelerator._get_neuron_core_count() == 0
    assert mock_subprocess.called


@mock.patch("subprocess.run")
def test_get_neuron_core_count_failure_with_empty_results(mock_subprocess):
    mock_subprocess.return_value.returncode = 0
    mock_subprocess.return_value.stdout = b"[{}]"
    assert accelerator._get_neuron_core_count() == 0
    assert mock_subprocess.called


@mock.patch("glob.glob")
def test_autodetect_num_tpus_accel(mock_glob):
    mock_glob.return_value = ["/dev/accel0", "/dev/accel1", "/dev/accel2", "/dev/accel3"]
    assert accelerator.autodetect_num_tpus() == 4


@mock.patch("glob.glob")
def test_autodetect_num_tpus_accel(mock_glob):
    mock_glob.return_value = [f"/dev/vfio/{i}" for i in range(4)]
    assert accelerator.autodetect_num_tpus() == 4


if __name__ == "__main__":
    import sys
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
