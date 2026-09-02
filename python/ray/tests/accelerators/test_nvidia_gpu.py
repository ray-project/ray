import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from ray._private.accelerators import NvidiaGPUAcceleratorManager, nvidia_gpu
from ray.tests.accelerators.mock_pynvml import (
    DeviceHandleMock,
    PyNVMLMock,
    patch_mock_pynvml,
)

GPU_MOCK_DATA = [
    DeviceHandleMock(
        "Ampere A100-SXM4-40GB",
        "GPU-8eaaebb8-bb64-8489-fda2-62256e821983",
        mig_devices=[
            DeviceHandleMock(
                "Ampere A100-SXM4-40GB MIG 1g.5gb",
                "MIG-c6d4f1ef-42e4-5de3-91c7-45d71c87eb3f",
                gi_id=0,
                ci_instance=0,
            ),
            DeviceHandleMock(
                "Ampere A100-SXM4-40GB MIG 1g.5gb",
                "MIG-0c757cd7-e942-5726-a0b8-0e8fb7067135",
                gi_id=1,
                ci_instance=0,
            ),
        ],
    ),
    DeviceHandleMock(
        "Ampere A100-SXM4-40GB",
        "GPU-8eaaebb8-bb64-8489-fda2-62256e821983",
        mig_devices=[
            DeviceHandleMock(
                "Ampere A100-SXM4-40GB MIG 1g.5gb",
                "MIG-a28ad590-3fda-56dd-84fc-0a0b96edc58d",
                gi_id=0,
                ci_instance=0,
            )
        ],
    ),
    DeviceHandleMock(
        "Tesla V100-SXM2-16GB", "GPU-8eaaebb8-bb64-8489-fda2-62256e821983"
    ),
]

mock_nvml = PyNVMLMock(GPU_MOCK_DATA)

patch_mock_pynvml = patch_mock_pynvml  # avoid format error


@pytest.mark.parametrize("mock_nvml", [mock_nvml])
def test_num_gpus_parsing(patch_mock_pynvml):
    # without mig instance
    assert NvidiaGPUAcceleratorManager.get_current_node_num_accelerators() == len(
        GPU_MOCK_DATA
    )


@pytest.mark.parametrize("mock_nvml", [mock_nvml])
def test_gpu_info_parsing(patch_mock_pynvml):
    assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_type() == "A100"


@pytest.mark.parametrize(
    "name,expected",
    [
        # Legacy datacenter GPU names: keep labels produced by the previous
        # parser stable.
        ("Tesla V100-SXM2-16GB", "V100"),
        ("Tesla P100-PCIE-16GB", "P100"),
        ("Tesla T4", "T4"),
        ("Tesla P4", "P4"),
        ("Tesla K80", "K80"),
        ("NVIDIA A10G", "A10G"),
        ("NVIDIA L4", "L4"),
        ("NVIDIA L40S", "L40S"),
        ("NVIDIA A100-SXM4-40GB", "A100"),
        ("NVIDIA H100 80GB HBM3", "H100"),
        ("NVIDIA H200", "H200"),
        ("NVIDIA H20", "H20"),
        ("NVIDIA B200", "B200"),
        ("NVIDIA B300", "B300"),
        ("NVIDIA GB200", "GB200"),
        ("NVIDIA GB300", "GB300"),
        # Consumer GPUs: the regex does not match the mixed-case product line,
        # so we fall back to a hyphen-joined product name.
        ("NVIDIA GeForce RTX 5090", "GeForce-RTX-5090"),
        ("NVIDIA GeForce RTX 4090", "GeForce-RTX-4090"),
        # RTX PRO cards: "RTX" alone is just a brand prefix, so the model is
        # captured through the first digit-containing token instead of
        # collapsing to the ambiguous "RTX".
        ("NVIDIA RTX PRO 6000 Blackwell Server Edition", "RTX-PRO-6000"),
        # Edge cases.
        (None, None),
        ("", None),
    ],
)
def test_gpu_name_to_accelerator_type(name, expected):
    assert NvidiaGPUAcceleratorManager._gpu_name_to_accelerator_type(name) == expected


def test_generate_cdi_spec_no_nvidia_ctk_binary():
    with patch("shutil.which", return_value=None):
        assert NvidiaGPUAcceleratorManager.generate_cdi_spec() is None


def test_generate_cdi_spec_success():
    """generate_cdi_spec never writes to disk: nvidia-ctk writes to stdout
    (no --output flag), which is parsed directly."""
    fake_result = MagicMock(
        returncode=0, stdout='{"kind": "nvidia.com/gpu", "devices": []}', stderr=""
    )
    with patch("shutil.which", return_value="/usr/bin/nvidia-ctk"), patch(
        "subprocess.run", return_value=fake_result
    ) as mock_run:
        assert NvidiaGPUAcceleratorManager.generate_cdi_spec() == {
            "kind": "nvidia.com/gpu",
            "devices": [],
        }
        args = mock_run.call_args.args[0]
        assert args[0] == "/usr/bin/nvidia-ctk"
        assert "cdi" in args and "generate" in args
        assert not any(a.startswith("--output=") for a in args)

        # A hung/misbehaving nvidia-ctk must not stall the caller, and
        # output/errors must actually be captured rather than inherited.
        kwargs = mock_run.call_args.kwargs
        assert kwargs["timeout"] == nvidia_gpu._NVIDIA_CTK_TIMEOUT_SECONDS
        assert kwargs["check"] is True
        assert kwargs["capture_output"] is True


def test_generate_cdi_spec_unparseable_output():
    with patch("shutil.which", return_value="/usr/bin/nvidia-ctk"), patch(
        "subprocess.run",
        return_value=MagicMock(returncode=0, stdout="not json", stderr=""),
    ):
        assert NvidiaGPUAcceleratorManager.generate_cdi_spec() is None


@pytest.mark.parametrize(
    "side_effect",
    [
        subprocess.CalledProcessError(1, ["nvidia-ctk"], stderr="boom"),
        subprocess.TimeoutExpired(["nvidia-ctk"], 30),
    ],
)
def test_generate_cdi_spec_subprocess_error(side_effect):
    with patch("shutil.which", return_value="/usr/bin/nvidia-ctk"), patch(
        "subprocess.run", side_effect=side_effect
    ):
        assert NvidiaGPUAcceleratorManager.generate_cdi_spec() is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
