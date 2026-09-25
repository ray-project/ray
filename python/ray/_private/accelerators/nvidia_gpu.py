import functools
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

# Overridable via NVIDIA_CTK_ENV_PATH. Sourced (if present) before every
# nvidia-ctk invocation, letting NVIDIA_DRIVER_ROOT and other
# nvidia-ctk-recognized env vars be overridden per-node without Ray needing
# to know about any of them.
_DEFAULT_NVIDIA_CTK_ENV_PATH = "/etc/nvidia-container-toolkit/nvidia-cdi-refresh.env"

# Timeout for shelling out to `nvidia-ctk` during CDI spec generation
# (generate_cdi_spec below). Multi-GPU nodes can see driver/device
# probing overhead push past several seconds, but 60s doesn't cost much
# in the common case either. `nvidia-ctk cdi generate` normally
# completes in well under a second, so this only bounds a hung or
# misbehaving nvidia-ctk to a longer-but-still-bounded failure.
_NVIDIA_CTK_TIMEOUT_SECONDS = 60

# The oldest nvidia-ctk whose update-ldcache hook runs under gVisor
# (github.com/NVIDIA/nvidia-container-toolkit/pull/2059).
_MIN_NVIDIA_CTK_VERSION = (1, 20, 1)


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
    def get_current_node_driver_version() -> str:
        import ray._private.thirdparty.pynvml as pynvml

        pynvml.nvmlInit()
        driver_version = pynvml.nvmlSystemGetDriverVersion()
        pynvml.nvmlShutdown()
        return driver_version

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
    def _build_nvidia_ctk_env() -> Dict[str, str]:
        """The environment to run nvidia-ctk in: this process's own
        environment, overlaid with any variables set in NVIDIA_CTK_ENV_PATH
        (default _DEFAULT_NVIDIA_CTK_ENV_PATH), if that file exists.

        That file follows the systemd EnvironmentFile format (see
        systemd.exec(5)'s EnvironmentFile= directive): plain KEY=value
        lines, with "#" or ";" starting a comment line, and an optional
        matching pair of leading and trailing quotes around the value.

        Drops NVIDIA_CTK_CDI_OUTPUT_FILE_PATH unconditionally afterward,
        since it redirects `cdi generate`'s output to a file instead of
        stdout, and generate_cdi_spec always parses stdout.
        """
        env = dict(os.environ)
        env_path = env.get("NVIDIA_CTK_ENV_PATH", _DEFAULT_NVIDIA_CTK_ENV_PATH)
        if os.path.isfile(env_path):
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line[0] in "#;":
                        continue
                    key, sep, value = line.partition("=")
                    if not sep:
                        continue
                    key, value = key.strip(), value.strip()
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                        value = value[1:-1]
                    env[key] = value
        env.pop("NVIDIA_CTK_CDI_OUTPUT_FILE_PATH", None)
        return env

    @staticmethod
    def _get_nvidia_ctk_version(nvidia_ctk_path: str) -> Optional[Tuple[int, int, int]]:
        """Parse `nvidia-ctk --version`'s (major, minor, patch), or None if
        it can't be run or its output doesn't match the expected format.

        Not cached. generate_cdi_spec only calls this when it has no cached
        spec yet, which is exactly when a fresh check matters (e.g. after
        a too-old nvidia-ctk was upgraded in place).
        """
        try:
            result = subprocess.run(
                [nvidia_ctk_path, "--version"],
                capture_output=True,
                timeout=_NVIDIA_CTK_TIMEOUT_SECONDS,
                text=True,
            )
        except (subprocess.TimeoutExpired, OSError):
            return None
        match = re.search(r"version (\d+)\.(\d+)\.(\d+)", result.stdout)
        if not match:
            return None
        return tuple(int(g) for g in match.groups())

    @staticmethod
    @functools.lru_cache(maxsize=None)
    def generate_cdi_spec() -> Dict:
        """Generate and return a CDI (Container Device Interface) spec
        describing the node's NVIDIA GPUs, via the `nvidia-ctk` CLI (part
        of nvidia-container-toolkit-base).

        Never written to disk: `nvidia-ctk cdi generate` writes to stdout
        when `--output` is omitted, which is captured and parsed directly.
        Keeps this simple to swap for a real CDI generator library later —
        no file format/location to keep compatible — and sidesteps sharing
        a generated spec across processes (each process that needs one
        generates its own once, then caches it for its own lifetime).

        Cached for the process's lifetime. A failure isn't cached
        (lru_cache never caches an exception), so the next call retries
        rather than getting stuck failing for the rest of the process.

        Future improvement: today this shells out to nvidia-ctk, but a
        Python-native generator (NVML enumeration, driver library
        discovery, device node/MIG handling) could replace this method's
        body without touching any caller. That's a substantially bigger
        lift than reimplementing CDI *spec merging* (see the parallel note
        in `ray.experimental.sandbox._internal.cdi_lib`) — it means
        reimplementing logic nvidia-container-toolkit maintains and keeps
        in sync with new drivers — and more naturally something to build
        and push upstream (NVIDIA or CNCF) than something Ray owns
        long-term.

        Consumers (e.g. Ray Sandboxes, via
        `ray.experimental.sandbox._internal.cdi`) merge the spec's
        per-device containerEdits into a container's OCI runtime spec
        themselves; this only produces the parsed spec.

        Returns:
            The parsed CDI spec.

        Raises:
            RuntimeError: If `nvidia-ctk` isn't on PATH, is older than
                `_MIN_NVIDIA_CTK_VERSION` (see its comment), `cdi generate`
                itself failed, or it produced unparseable output. This is
                only called once a gpu_ids request actually needs a CDI
                spec, so any failure here means unmet user intent —
                callers should surface it as a fatal sandbox creation
                error, not silently treat it as "GPU CDI support is
                unavailable on this node".
        """
        nvidia_ctk_path = shutil.which("nvidia-ctk")
        if nvidia_ctk_path is None:
            raise RuntimeError(
                "nvidia-ctk not found on PATH. Install "
                "nvidia-container-toolkit-base to enable GPU CDI support."
            )

        version = NvidiaGPUAcceleratorManager._get_nvidia_ctk_version(nvidia_ctk_path)
        if version is None or version < _MIN_NVIDIA_CTK_VERSION:
            min_version_str = ".".join(str(v) for v in _MIN_NVIDIA_CTK_VERSION)
            found = ".".join(str(v) for v in version) if version else "unknown"
            raise RuntimeError(
                f"nvidia-ctk version {found} is older than the minimum "
                f"supported {min_version_str}. Upgrade "
                "nvidia-container-toolkit-base to enable GPU CDI support."
            )

        # Emits both index-named ("0") and UUID-named ("GPU-...") devices
        # for each GPU, so a caller can pass either format as a gpu_id.
        try:
            result = subprocess.run(
                [
                    nvidia_ctk_path,
                    "cdi",
                    "generate",
                    "--format=json",
                    "--device-name-strategy=index",
                    "--device-name-strategy=uuid",
                ],
                capture_output=True,
                timeout=_NVIDIA_CTK_TIMEOUT_SECONDS,
                check=True,
                text=True,
                env=NvidiaGPUAcceleratorManager._build_nvidia_ctk_env(),
            )
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as e:
            stderr = getattr(e, "stderr", None)
            raise RuntimeError(f"Failed to generate CDI spec: {e}. {stderr}") from e

        try:
            # nvidia-ctk can write more than one JSON document to stdout
            # (e.g. --feature-flag=enable-coherent-annotations). The full
            # spec is always written first, so decode only that one.
            return json.JSONDecoder().raw_decode(result.stdout.lstrip())[0]
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"nvidia-ctk produced unparseable CDI spec output: {e}"
            ) from e
