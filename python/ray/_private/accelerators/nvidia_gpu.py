import json
import logging
import os
import re
import shutil
import subprocess
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

CUDA_VISIBLE_DEVICES_ENV_VAR = "CUDA_VISIBLE_DEVICES"
NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"

# Capture the accelerator model from the NVML device name: the run of leading
# all-caps tokens (e.g. "RTX", "PRO") up to and including the first token that
# contains a digit. This keeps datacenter cards stable ("Tesla V100-SXM2-16GB"
# -> "V100", "NVIDIA A100-SXM4-40GB" -> "A100") while disambiguating the RTX
# line, whose first token is only a brand prefix ("NVIDIA RTX PRO 6000 Blackwell
# Server Edition" -> "RTX PRO 6000"). A trailing SKU suffix after a hyphen is
# dropped. Mixed-case consumer names ("NVIDIA GeForce RTX 5090") don't match and
# fall back to a hyphen-joined product name in _gpu_name_to_accelerator_type.
NVIDIA_GPU_NAME_PATTERN = re.compile(r"\w+\s+((?:[A-Z]+\s+)*[A-Z0-9]*\d[A-Z0-9]*)")

# Timeout for shelling out to `nvidia-ctk` during CDI spec generation
# (generate_cdi_spec below). This runs synchronously in whichever process
# calls it — each sandbox-creating worker generates and caches its own copy
# in memory (see ray._common.cdi.get_spec / cdi_lib.CDISpec.generate)
# — so it must not be generous: `nvidia-ctk cdi generate` only enumerates
# local driver/device state, no network I/O, and normally completes in
# well under a second. This bounds a hung/misbehaving nvidia-ctk to a
# short, user-visible failure instead of a long stall.
_NVIDIA_CTK_TIMEOUT_SECONDS = 5


class NvidiaGPUAcceleratorManager(AcceleratorManager):
    """NVIDIA GPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return CUDA_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        cuda_visible_devices = os.environ.get(
            NvidiaGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )
        if cuda_visible_devices is None:
            return None

        if cuda_visible_devices == "":
            return []

        if cuda_visible_devices == "NoDevFiles":
            return []

        return list(cuda_visible_devices.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        import ray._private.thirdparty.pynvml as pynvml

        try:
            pynvml.nvmlInit()
        except pynvml.NVMLError:
            return 0  # pynvml init failed
        device_count = pynvml.nvmlDeviceGetCount()
        pynvml.nvmlShutdown()
        return device_count

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        import ray._private.thirdparty.pynvml as pynvml

        try:
            pynvml.nvmlInit()
        except pynvml.NVMLError:
            return None  # pynvml init failed
        device_count = pynvml.nvmlDeviceGetCount()
        cuda_device_type = None
        if device_count > 0:
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            device_name = pynvml.nvmlDeviceGetName(handle)
            if isinstance(device_name, bytes):
                device_name = device_name.decode("utf-8")
            cuda_device_type = (
                NvidiaGPUAcceleratorManager._gpu_name_to_accelerator_type(device_name)
            )
        pynvml.nvmlShutdown()
        return cuda_device_type

    @staticmethod
    def _gpu_name_to_accelerator_type(name):
        if name is None:
            return None
        match = NVIDIA_GPU_NAME_PATTERN.match(name)
        result = match.group(1).replace(" ", "-") if match else None
        if result and len(result) > 1:
            return result
        # The pattern above requires an all-uppercase/numeric model token, which
        # works for datacenter cards ("Tesla V100-SXM2-16GB" -> "V100",
        # "NVIDIA RTX PRO 6000 ..." -> "RTX-PRO-6000") but not for consumer
        # cards whose product line is mixed case ("NVIDIA GeForce RTX 5090").
        # Fall back to a hyphen-joined product name so callers get a useful
        # accelerator_type label like "GeForce-RTX-5090".
        cleaned = re.sub(r"^NVIDIA\s+", "", name).strip()
        return cleaned.replace(" ", "-") if cleaned else None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_cuda_devices: List[str],
    ) -> None:
        if env_bool(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR, False):
            return

        os.environ[
            NvidiaGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_cuda_devices])

    @staticmethod
    def get_ec2_instance_num_accelerators(
        instance_type: str, instances: dict
    ) -> Optional[int]:
        if instance_type not in instances:
            return None

        gpus = instances[instance_type].get("GpuInfo", {}).get("Gpus")
        if gpus is not None:
            # TODO(ameer): currently we support one gpu type per node.
            assert len(gpus) == 1
            return gpus[0]["Count"]
        return None

    @staticmethod
    def get_ec2_instance_accelerator_type(
        instance_type: str, instances: dict
    ) -> Optional[str]:
        if instance_type not in instances:
            return None

        gpus = instances[instance_type].get("GpuInfo", {}).get("Gpus")
        if gpus is not None:
            # TODO(ameer): currently we support one gpu type per node.
            assert len(gpus) == 1
            return gpus[0]["Name"]
        return None

    @staticmethod
    def get_cdi_kind() -> str:
        return "nvidia.com/gpu"

    @staticmethod
    def generate_cdi_spec() -> Optional[Dict]:
        """Generate and return a CDI (Container Device Interface) spec
        describing the node's NVIDIA GPUs, via the `nvidia-ctk` CLI (part
        of nvidia-container-toolkit-base).

        Never written to disk: `nvidia-ctk cdi generate` writes to stdout
        when `--output` is omitted, which is captured and parsed directly.
        Keeps this simple to swap for a real CDI generator library later —
        no file format/location to keep compatible — and sidesteps sharing
        a generated spec across processes (each process that needs one
        just generates its own; `nvidia-ctk cdi generate` normally
        completes in well under a second, so this is cheap enough not to
        need cross-process caching).

        Future improvement: today this shells out to nvidia-ctk, but a
        Python-native generator (NVML enumeration, driver library
        discovery, device node/MIG handling) could replace this method's
        body without touching any caller. That's a substantially bigger
        lift than reimplementing CDI *spec merging* (see the parallel note
        in `ray._common.cdi_lib`) — it means reimplementing logic
        nvidia-container-toolkit maintains and keeps in sync with new
        drivers — and more naturally something to build and push upstream
        (NVIDIA or CNCF) than something Ray owns long-term.

        Consumers (e.g. Ray Sandboxes, via `ray._common.cdi`) merge the
        spec's per-device containerEdits into a container's OCI runtime
        spec themselves; this only produces the parsed spec.

        Returns:
            The parsed CDI spec, or None if `nvidia-ctk` is unavailable,
            generation failed, or it produced unparseable output (logged at
            warning level, since this is only called once a gpu_ids
            request needs it, so a failure here means unmet user intent,
            not routine background noise) — callers should treat this as
            "GPU CDI support is unavailable on this node", not a fatal
            error.
        """
        nvidia_ctk_path = shutil.which("nvidia-ctk")
        if nvidia_ctk_path is None:
            logger.warning(
                "nvidia-ctk not found on PATH; skipping CDI spec generation. "
                "Install nvidia-container-toolkit-base to enable it."
            )
            return None

        # When generating the CDI spec: omits the update-ldcache hook, which
        # gVisor can't run (it needs to mount /proc) and which is redundant
        # here since the CDI-mounted driver libraries already sit on ld.so's
        # default search path. Keeps the nvidia-persistenced/fabricmanager/MPS
        # IPC socket mounts -- fabric-aware GPUs (e.g. NVSwitch systems like
        # GB200) need the fabricmanager socket for CUDA's UVM init to
        # complete at all, not just for multi-node NVLink. Emits both
        # index-named ("0") and UUID-named ("GPU-...") devices for each GPU,
        # so a caller can pass either format as a gpu_id.
        try:
            result = subprocess.run(
                [
                    nvidia_ctk_path,
                    "cdi",
                    "generate",
                    "--format=json",
                    "--disable-hook=update-ldcache",
                    "--device-name-strategy=index",
                    "--device-name-strategy=uuid",
                ],
                capture_output=True,
                timeout=_NVIDIA_CTK_TIMEOUT_SECONDS,
                check=True,
                text=True,
            )
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as e:
            stderr = getattr(e, "stderr", None)
            logger.warning(f"Failed to generate CDI spec: {e}. {stderr}")
            return None

        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as e:
            logger.warning(f"nvidia-ctk produced unparseable CDI spec output: {e}")
            return None
