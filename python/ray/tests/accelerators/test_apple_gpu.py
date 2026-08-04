import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from ray._private.accelerators.apple_gpu import AppleGPUAcceleratorManager


class TestAppleGPUAcceleratorManager:
    """Test suite for Apple GPU (MPS) accelerator manager."""

    def test_get_resource_name(self):
        """Test that the resource name is 'GPU'."""
        assert AppleGPUAcceleratorManager.get_resource_name() == "GPU"

    def test_get_visible_accelerator_ids_env_var(self):
        """Apple Silicon has no visible-devices env var (single unified GPU)."""
        assert AppleGPUAcceleratorManager.get_visible_accelerator_ids_env_var() is None

    def test_get_current_node_additional_resources(self):
        """Test that no additional resources are required."""
        assert (
            AppleGPUAcceleratorManager.get_current_node_additional_resources() is None
        )

    def test_validate_resource_request_quantity_accepts_any(self):
        """Like other GPU managers, any quantity is accepted (scheduler enforces
        capacity). In particular, quantity > 1 must not be rejected, since the manager
        is chosen by the driver node and may front multi-GPU NVIDIA workers."""
        for quantity in (0.0, 0.5, 1.0, 2.0, 8.0):
            assert AppleGPUAcceleratorManager.validate_resource_request_quantity(
                quantity
            ) == (True, None)

    def test_ec2_instance_methods(self):
        """Test that EC2 methods return None (Apple Silicon not available on EC2)."""
        assert (
            AppleGPUAcceleratorManager.get_ec2_instance_num_accelerators("m5.large", {})
            is None
        )
        assert (
            AppleGPUAcceleratorManager.get_ec2_instance_accelerator_type("m5.large", {})
            is None
        )

    @patch("platform.system")
    @patch("platform.machine")
    def test_is_apple_silicon_detection(self, mock_machine, mock_system):
        """Test Apple Silicon platform detection."""
        mock_system.return_value = "Darwin"
        mock_machine.return_value = "arm64"
        assert AppleGPUAcceleratorManager._is_apple_silicon() is True

        mock_system.return_value = "Linux"
        assert AppleGPUAcceleratorManager._is_apple_silicon() is False

        mock_system.return_value = "Darwin"
        mock_machine.return_value = "x86_64"
        assert AppleGPUAcceleratorManager._is_apple_silicon() is False

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_metal_gpu_available"
    )
    def test_get_current_node_num_accelerators(
        self, mock_is_metal_gpu_available, mock_is_apple_silicon
    ):
        """Accelerator count is detected from hardware, not from PyTorch."""
        # Not Apple Silicon
        mock_is_apple_silicon.return_value = False
        assert AppleGPUAcceleratorManager.get_current_node_num_accelerators() == 0

        # Apple Silicon but system_profiler reports no Metal GPU
        mock_is_apple_silicon.return_value = True
        mock_is_metal_gpu_available.return_value = False
        assert AppleGPUAcceleratorManager.get_current_node_num_accelerators() == 0

        # Apple Silicon with a Metal GPU (no PyTorch involved)
        mock_is_apple_silicon.return_value = True
        mock_is_metal_gpu_available.return_value = True
        assert AppleGPUAcceleratorManager.get_current_node_num_accelerators() == 1

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    def test_get_current_node_accelerator_type_not_apple_silicon(
        self, mock_is_apple_silicon
    ):
        """Test that None is returned for accelerator type on non-Apple Silicon."""
        mock_is_apple_silicon.return_value = False

        assert AppleGPUAcceleratorManager.get_current_node_accelerator_type() is None

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._get_apple_chip_type"
    )
    def test_get_current_node_accelerator_type_apple_silicon(
        self, mock_get_chip_type, mock_is_apple_silicon
    ):
        """Test that chip type is returned on Apple Silicon."""
        mock_is_apple_silicon.return_value = True
        mock_get_chip_type.return_value = "M2-Pro"

        assert (
            AppleGPUAcceleratorManager.get_current_node_accelerator_type() == "M2-Pro"
        )

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    def test_get_current_process_visible_accelerator_ids_apple_silicon(
        self, mock_is_apple_silicon
    ):
        """The only visible id on Apple Silicon is ever "0"."""
        mock_is_apple_silicon.return_value = True

        assert (
            AppleGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
            == ["0"]
        )

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    def test_get_current_process_visible_accelerator_ids_not_apple_silicon(
        self, mock_is_apple_silicon
    ):
        """Non-Apple-Silicon hosts report no visible Apple GPU."""
        mock_is_apple_silicon.return_value = False

        assert (
            AppleGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
            is None
        )

    def test_set_current_process_visible_accelerator_ids_is_noop(self):
        """Apple Silicon has no visible-devices env var, so set is a no-op."""
        with patch.dict(os.environ, {}, clear=True):
            AppleGPUAcceleratorManager.set_current_process_visible_accelerator_ids(
                ["0"]
            )
            assert os.environ == {}

    @patch("subprocess.run")
    def test_is_metal_gpu_available(self, mock_run):
        """Metal GPU detection via system_profiler (no framework dependency)."""
        # A Metal-capable GPU is reported.
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            "Graphics/Displays:\n    Apple M3 Pro:\n"
            "      Type: GPU\n      Metal Support: Metal 3\n"
        )
        mock_run.return_value = mock_result
        assert AppleGPUAcceleratorManager._is_metal_gpu_available() is True

        # No Metal GPU in the output.
        mock_result.stdout = "Graphics/Displays:\n"
        assert AppleGPUAcceleratorManager._is_metal_gpu_available() is False

        # system_profiler failing must not raise.
        mock_run.side_effect = Exception("Command failed")
        assert AppleGPUAcceleratorManager._is_metal_gpu_available() is False

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("Apple M1", "M1"),
            ("Apple M1 Pro", "M1-Pro"),
            ("Apple M2 Max", "M2-Max"),
            ("Apple M1 Ultra", "M1-Ultra"),
            ("Apple M3", "M3"),
            ("Apple M4 Pro", "M4-Pro"),
            # Leading/trailing whitespace (as produced by line.split("Chip:")[1]).
            ("   Apple M4 Pro  ", "M4-Pro"),
            # No Apple / no M-series designation -> None.
            ("Intel(R) Core(TM) i9", None),
            ("Apple", None),
            ("", None),
        ],
    )
    def test_parse_apple_chip(self, text, expected):
        """Chip-name parsing, independent of any subprocess."""
        assert AppleGPUAcceleratorManager._parse_apple_chip(text) == expected

    @pytest.mark.parametrize(
        "chip,expected",
        [
            ("Apple M1", "M1"),
            ("Apple M1 Pro", "M1-Pro"),
            ("Apple M2 Max", "M2-Max"),
            ("Apple M1 Ultra", "M1-Ultra"),
            ("Apple M3", "M3"),
            ("Apple M4 Pro", "M4-Pro"),
        ],
    )
    @patch("subprocess.run")
    def test_get_apple_chip_type_from_system_profiler(self, mock_run, chip, expected):
        """Chip type is parsed from system_profiler output (M-series + suffix)."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            f"Hardware:\n\n    Hardware Overview:\n\n      Chip: {chip}\n"
        )
        mock_run.return_value = mock_result

        assert AppleGPUAcceleratorManager._get_apple_chip_type() == expected

    @patch("subprocess.run")
    def test_get_apple_chip_type_sysctl_fallback(self, mock_run):
        """Falls back to sysctl when system_profiler has no Chip line."""
        no_chip = MagicMock(returncode=0, stdout="Hardware:\n  Hardware Overview:\n")
        sysctl = MagicMock(returncode=0, stdout="Apple M2\n")
        mock_run.side_effect = [no_chip, sysctl]

        assert AppleGPUAcceleratorManager._get_apple_chip_type() == "M2"

    @patch("subprocess.run")
    def test_get_apple_chip_type_unknown_returns_none(self, mock_run):
        """Returns None (no generic 'Apple-Silicon' fallback) when undeterminable."""
        no_chip = MagicMock(returncode=0, stdout="Hardware:\n")
        non_apple = MagicMock(returncode=0, stdout="Intel(R) Core(TM) i9\n")
        mock_run.side_effect = [no_chip, non_apple]

        assert AppleGPUAcceleratorManager._get_apple_chip_type() is None

    @patch("subprocess.run")
    def test_get_apple_chip_type_subprocess_error_returns_none(self, mock_run):
        """Returns None when the subprocess calls raise."""
        mock_run.side_effect = Exception("Command failed")

        assert AppleGPUAcceleratorManager._get_apple_chip_type() is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
