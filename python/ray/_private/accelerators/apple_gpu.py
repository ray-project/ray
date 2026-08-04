import logging
import platform
import subprocess
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)


class AppleGPUAcceleratorManager(AcceleratorManager):
    """Apple Silicon GPU (MPS) accelerator manager.

    Unlike Nvidia GPUs and other devices, Apple GPUs only expose a single MPS device per
    host and there is no environment variable to toggle its visibility. Therefore the
    accelerator manager is only used to determine which MPS device is available, and
    enable scheduling it via the "GPU" resource.
    """

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> Optional[str]:
        # Apple Silicon exposes a single unified GPU (id "0") that cannot be
        # selected or hidden, so there is no visible-devices env var (unlike
        # CUDA_VISIBLE_DEVICES). Return None so callers skip env var handling.
        return None

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Detect the number of Apple Silicon GPUs on the current node.

        Apple Silicon has one unified GPU. We detect it via the macOS `system_profiler`
        tool (the analog of NVIDIA's pynvml) rather than importing a framework like
        PyTorch. Gating on PyTorch would make `pip install ray` setups and non-PyTorch
        frameworks (JAX, MLX, ...) advertise GPU=0 despite the hardware being present.
        """
        try:
            if not AppleGPUAcceleratorManager._is_apple_silicon():
                return 0
            return 1 if AppleGPUAcceleratorManager._is_metal_gpu_available() else 0
        except Exception as e:
            logger.debug(f"Error detecting Apple Silicon GPU: {e}")
            return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Get the Apple Silicon chip type (M1, M2, M3, etc.)."""
        try:
            if not AppleGPUAcceleratorManager._is_apple_silicon():
                return None

            return AppleGPUAcceleratorManager._get_apple_chip_type()
        except Exception as e:
            logger.debug(f"Error getting Apple Silicon chip type: {e}")
            return None

    @staticmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        """No additional resources required for Apple Silicon GPUs."""
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        # Like other GPU managers, accept any quantity and leave capacity checks to
        # the scheduler. The accelerator manager is selected based on the driver node,
        # so rejecting quantity > 1 here would wrongly fail multi-GPU requests (e.g.
        # for NVIDIA workers) submitted from an Apple Silicon driver.
        return (True, None)

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        """Get the explicitly-restricted visible GPU device IDs, or None.

        Apple Silicon has no visible-devices env var (see
        get_visible_accelerator_ids_env_var), so a process is never restricted to a
        subset of devices. We therefore always return None ("no restriction"), exactly
        as the NVIDIA manager does when CUDA_VISIBLE_DEVICES is unset. Returning a list
        here (e.g. ["0"]) would incorrectly trigger id remapping and make
        `ray.get_gpu_ids()` yield string ids like "0" instead of the int 0.
        """
        return None

    @staticmethod
    def set_current_process_visible_accelerator_ids(ids: List[str]) -> None:
        """No-op for Apple Silicon.

        Apple Silicon exposes a single unified GPU (id "0") that cannot be selected
        or hidden, so there is no visible-devices env var to set (see
        get_visible_accelerator_ids_env_var).
        """
        return

    @staticmethod
    def get_ec2_instance_num_accelerators(
        instance_type: str, instances: dict
    ) -> Optional[int]:
        """Apple Silicon GPUs are not available on EC2."""
        return None

    @staticmethod
    def get_ec2_instance_accelerator_type(
        instance_type: str, instances: dict
    ) -> Optional[str]:
        """Apple Silicon GPUs are not available on EC2."""
        return None

    @staticmethod
    def _is_apple_silicon() -> bool:
        """Check if running on Apple Silicon (ARM64 macOS)."""
        try:
            return platform.system() == "Darwin" and platform.machine() == "arm64"
        except Exception:
            return False

    @staticmethod
    def _is_metal_gpu_available() -> bool:
        """Detect a Metal-capable GPU via the macOS `system_profiler` tool.

        Uses `system_profiler` (always present on macOS) instead of importing a
        framework like PyTorch, so detection works for `pip install ray` and for
        non-PyTorch frameworks (JAX, MLX, ...).
        """
        try:
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.returncode == 0 and "metal" in result.stdout.lower()
        except Exception:
            return False

    @staticmethod
    def _parse_apple_chip(text: str) -> Optional[str]:
        """Parse an Apple M-series chip name (e.g. "M1-Pro") from a string.

        Returns the normalized chip name, joining any suffix with a dash
        (e.g. "Apple M1 Pro" -> "M1-Pro"), or None if `text` contains no Apple
        M-series designation.
        """
        if "Apple" not in text:
            return None
        parts = text.split()
        for i, part in enumerate(parts):
            if part.startswith("M") and len(part) >= 2:
                if i + 1 < len(parts) and parts[i + 1] in ("Pro", "Max", "Ultra"):
                    return f"{part}-{parts[i + 1]}"
                return part
        return None

    @staticmethod
    def _get_apple_chip_type() -> Optional[str]:
        """Get the specific Apple Silicon chip type (e.g. "M1-Pro"), or None."""
        try:
            # Prefer the "Chip:" line from system_profiler.
            result = subprocess.run(
                ["system_profiler", "SPHardwareDataType"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                for line in result.stdout.split("\n"):
                    if "Chip:" in line:
                        chip = AppleGPUAcceleratorManager._parse_apple_chip(
                            line.split("Chip:")[1]
                        )
                        if chip is not None:
                            return chip

            # Fall back to the sysctl CPU brand string.
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                chip = AppleGPUAcceleratorManager._parse_apple_chip(result.stdout)
                if chip is not None:
                    return chip

            # Couldn't determine the specific chip.
            return None

        except Exception as e:
            logger.debug(f"Error detecting Apple chip type: {e}")
            return None
