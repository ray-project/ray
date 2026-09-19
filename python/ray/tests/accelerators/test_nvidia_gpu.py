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


@pytest.fixture(autouse=True)
def reset_cdi_spec_cache():
    """generate_cdi_spec caches its result for the process lifetime; each
    test needs a fresh nvidia-ctk call rather than a prior test's cache."""
    nvidia_gpu._cdi_spec_cache = None
    yield
    nvidia_gpu._cdi_spec_cache = None


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
        assert "NVIDIA_CTK_CDI_OUTPUT_FILE_PATH" not in kwargs["env"]


def test_generate_cdi_spec_decodes_only_first_of_multiple_documents():
    """If nvidia-ctk ever writes multiple JSON documents to stdout back to
    back with no separator (see the comment on the raw_decode call this
    exercises), only the first (full) document must be decoded, not
    concatenated with or replaced by the trailing ones."""
    fake_result = MagicMock(
        returncode=0,
        stdout=(
            '{"kind": "nvidia.com/gpu", "devices": [{"name": "0"}]}'
            '{"kind": "nvidia.com/gpu", "devices": [{"name": "0"}], "class": "coherent"}'
        ),
        stderr="",
    )
    with patch("shutil.which", return_value="/usr/bin/nvidia-ctk"), patch(
        "subprocess.run", return_value=fake_result
    ):
        assert NvidiaGPUAcceleratorManager.generate_cdi_spec() == {
            "kind": "nvidia.com/gpu",
            "devices": [{"name": "0"}],
        }


def test_generate_cdi_spec_caches_success():
    """A second call reuses the first's result instead of shelling out to
    nvidia-ctk again."""
    fake_result = MagicMock(
        returncode=0, stdout='{"kind": "nvidia.com/gpu", "devices": []}', stderr=""
    )
    with patch("shutil.which", return_value="/usr/bin/nvidia-ctk"), patch(
        "subprocess.run", return_value=fake_result
    ) as mock_run:
        first = NvidiaGPUAcceleratorManager.generate_cdi_spec()
        second = NvidiaGPUAcceleratorManager.generate_cdi_spec()
        assert first == second == {"kind": "nvidia.com/gpu", "devices": []}
        assert mock_run.call_count == 1


def test_generate_cdi_spec_does_not_cache_failure():
    """A failed generation isn't cached, so the next call retries rather
    than getting stuck returning None for the rest of the process."""
    with patch("shutil.which", return_value=None):
        assert NvidiaGPUAcceleratorManager.generate_cdi_spec() is None

    fake_result = MagicMock(
        returncode=0, stdout='{"kind": "nvidia.com/gpu", "devices": []}', stderr=""
    )
    with patch("shutil.which", return_value="/usr/bin/nvidia-ctk"), patch(
        "subprocess.run", return_value=fake_result
    ):
        assert NvidiaGPUAcceleratorManager.generate_cdi_spec() == {
            "kind": "nvidia.com/gpu",
            "devices": [],
        }


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


def test_build_nvidia_ctk_env_parses_comments_and_quotes(tmp_path):
    """Comments, blank lines, and quoted values, matching the systemd
    EnvironmentFile format nvidia-cdi-refresh.env ships in -- bare
    KEY=VALUE, no `export` keyword."""
    env_file = tmp_path / "test.env"
    env_file.write_text(
        "# a comment\n"
        "; also a comment\n"
        "\n"
        "NVIDIA_CTK_DRIVER_ROOT=/usr/local/nvidia\n"
        '  NVIDIA_CTK_DEV_ROOT = "/quoted/path" \n'
        "SINGLE_QUOTED='/other/path'\n"
    )
    with patch.dict("os.environ", {"NVIDIA_CTK_ENV_PATH": str(env_file)}, clear=True):
        env = NvidiaGPUAcceleratorManager._build_nvidia_ctk_env()
    assert env["NVIDIA_CTK_DRIVER_ROOT"] == "/usr/local/nvidia"
    assert env["NVIDIA_CTK_DEV_ROOT"] == "/quoted/path"
    assert env["SINGLE_QUOTED"] == "/other/path"


def test_build_nvidia_ctk_env_no_file():
    """No env file at the default or overridden path: nvidia-ctk just gets
    this process's own environment, minus NVIDIA_CTK_CDI_OUTPUT_FILE_PATH."""
    with patch.dict(
        "os.environ", {"SOME_VAR": "1", "NVIDIA_CTK_ENV_PATH": "/no/such/file"}
    ):
        env = NvidiaGPUAcceleratorManager._build_nvidia_ctk_env()
    assert env["SOME_VAR"] == "1"
    assert "NVIDIA_CTK_CDI_OUTPUT_FILE_PATH" not in env


def test_build_nvidia_ctk_env_overlays_file(tmp_path):
    env_file = tmp_path / "test.env"
    env_file.write_text("NVIDIA_CTK_DRIVER_ROOT=/usr/local/nvidia\n")
    with patch.dict(
        "os.environ", {"NVIDIA_CTK_ENV_PATH": str(env_file), "SOME_VAR": "1"}
    ):
        env = NvidiaGPUAcceleratorManager._build_nvidia_ctk_env()
    assert env["NVIDIA_CTK_DRIVER_ROOT"] == "/usr/local/nvidia"
    assert env["SOME_VAR"] == "1"


def test_build_nvidia_ctk_env_drops_cdi_output_file_path(tmp_path):
    """nvidia-cdi-refresh.env documents NVIDIA_CTK_CDI_OUTPUT_FILE_PATH as
    an option, which nvidia-ctk cdi generate treats as --output, but
    generate_cdi_spec always parses nvidia-ctk's stdout. If a sourced env
    file sets this, it must not survive into nvidia-ctk's environment."""
    env_file = tmp_path / "test.env"
    env_file.write_text("NVIDIA_CTK_CDI_OUTPUT_FILE_PATH=/var/run/cdi/nvidia.yaml\n")
    with patch.dict("os.environ", {"NVIDIA_CTK_ENV_PATH": str(env_file)}):
        env = NvidiaGPUAcceleratorManager._build_nvidia_ctk_env()
    assert "NVIDIA_CTK_CDI_OUTPUT_FILE_PATH" not in env


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
