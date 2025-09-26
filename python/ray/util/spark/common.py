"""
Common utilities and helper functions for Ray on Spark.

This module provides shared functionality used across the Ray on Spark
implementation, including resource detection, environment validation,
and common operations.
"""

import logging
import multiprocessing
import os
import shutil
import subprocess
import time
from typing import Dict, List, Optional, Tuple

import psutil

from .config import Config, Defaults, EnvironmentVariables, get_config

_logger = logging.getLogger(__name__)


class ResourceDetector:
    """
    Utility class for detecting system resources in Apache Spark environments.

    This class provides container-aware resource detection that works across
    different deployment environments including Kubernetes, YARN, and standalone
    clusters. It automatically detects container limits and adjusts resource
    allocation accordingly.
    """

    @staticmethod
    def get_cpu_count() -> int:
        """Get the number of available CPU cores."""
        config = get_config()

        # Check for environment variable override
        if EnvironmentVariables.RAY_ON_SPARK_WORKER_CPU_CORES in os.environ:
            try:
                return int(
                    os.environ[EnvironmentVariables.RAY_ON_SPARK_WORKER_CPU_CORES]
                )
            except ValueError:
                _logger.warning(
                    "Invalid CPU count in environment variable, using detected value"
                )

        # Check for container CPU limits
        if config.environment.is_containerized:
            container_cpus = ResourceDetector._get_container_cpu_limit()
            if container_cpus:
                return min(container_cpus, multiprocessing.cpu_count())

        return multiprocessing.cpu_count()

    @staticmethod
    def get_memory_info() -> Dict[str, int]:
        """Get system memory information."""
        config = get_config()

        # Get basic memory info
        vm = psutil.virtual_memory()
        memory_info = {
            "total": vm.total,
            "available": vm.available,
            "used": vm.used,
            "percent": vm.percent,
        }

        # Check for environment variable overrides
        if EnvironmentVariables.RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES in os.environ:
            try:
                memory_info["total"] = int(
                    os.environ[
                        EnvironmentVariables.RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES
                    ]
                )
            except ValueError:
                pass

        # Check for container memory limits
        if config.environment.is_containerized:
            container_memory = ResourceDetector._get_container_memory_limit()
            if container_memory and container_memory < memory_info["total"]:
                memory_info["container_limit"] = container_memory
                memory_info["effective_total"] = container_memory
            else:
                memory_info["effective_total"] = memory_info["total"]
        else:
            memory_info["effective_total"] = memory_info["total"]

        return memory_info

    @staticmethod
    def get_shared_memory_info() -> Dict[str, int]:
        """Get shared memory information."""
        shared_memory_info = {}

        # Check for environment variable override
        if EnvironmentVariables.RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES in os.environ:
            try:
                shared_memory_info["total"] = int(
                    os.environ[
                        EnvironmentVariables.RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES
                    ]
                )
                return shared_memory_info
            except ValueError:
                pass

        # Get shared memory usage
        try:
            shm_usage = shutil.disk_usage("/dev/shm")
            shared_memory_info.update(
                {
                    "total": shm_usage.total,
                    "free": shm_usage.free,
                    "used": shm_usage.used,
                }
            )
        except OSError:
            _logger.warning("Could not detect shared memory information")
            shared_memory_info = {"total": 1024**3}  # 1GB fallback

        return shared_memory_info

    @staticmethod
    def get_gpu_count() -> int:
        """Get the number of available GPUs."""
        # Check for environment variable override
        if EnvironmentVariables.RAY_ON_SPARK_WORKER_GPU_NUM in os.environ:
            try:
                return int(os.environ[EnvironmentVariables.RAY_ON_SPARK_WORKER_GPU_NUM])
            except ValueError:
                _logger.warning("Invalid GPU count in environment variable")

        # Try to detect GPUs
        try:
            import pynvml

            pynvml.nvmlInit()
            return pynvml.nvmlDeviceGetCount()
        except (ImportError, Exception):
            pass

        # Fallback: check for CUDA devices
        try:
            result = subprocess.run(
                ["nvidia-smi", "-L"], capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                return len(
                    [line for line in result.stdout.split("\n") if "GPU" in line]
                )
        except (subprocess.SubprocessError, FileNotFoundError):
            pass

        return 0

    @staticmethod
    def _get_container_cpu_limit() -> Optional[int]:
        """Get container CPU limit from cgroup v1 or v2."""
        try:
            # Try cgroup v1 first
            quota_file = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
            period_file = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"

            if os.path.exists(quota_file) and os.path.exists(period_file):
                with open(quota_file, "r") as f:
                    quota = int(f.read().strip())
                with open(period_file, "r") as f:
                    period = int(f.read().strip())

                if quota > 0 and period > 0:
                    return int(quota / period)

            # Try cgroup v2
            cpu_max_file = "/sys/fs/cgroup/cpu.max"
            if os.path.exists(cpu_max_file):
                with open(cpu_max_file, "r") as f:
                    content = f.read().strip()
                    if content != "max" and " " in content:
                        quota, period = content.split()
                        quota, period = int(quota), int(period)
                        if quota > 0 and period > 0:
                            return int(quota / period)

        except (OSError, ValueError):
            pass

        return None

    @staticmethod
    def _get_container_memory_limit() -> Optional[int]:
        """Get container memory limit from cgroup v1 or v2."""
        try:
            # Try cgroup v1 first
            limit_file = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
            if os.path.exists(limit_file):
                with open(limit_file, "r") as f:
                    limit = int(f.read().strip())
                    # Check if it's a real limit (not the cgroup max)
                    if limit < (1 << 62):
                        return limit

            # Try cgroup v2
            memory_max_file = "/sys/fs/cgroup/memory.max"
            if os.path.exists(memory_max_file):
                with open(memory_max_file, "r") as f:
                    content = f.read().strip()
                    if content != "max":
                        return int(content)

        except (OSError, ValueError):
            pass

        return None


class NetworkDetector:
    """Utility class for network interface detection."""

    @staticmethod
    def get_available_interfaces() -> List[str]:
        """Get list of available network interfaces."""
        interfaces = []

        try:
            with open("/proc/net/dev", "r") as f:
                lines = f.readlines()[2:]  # Skip header lines
                for line in lines:
                    iface = line.split(":")[0].strip()
                    if iface and iface != "lo":  # Skip loopback
                        interfaces.append(iface)
        except OSError:
            # Fallback to common interface names
            interfaces = ["eth0", "ens3", "ens4", "enp0s3", "enp0s8"]

        return interfaces

    @staticmethod
    def get_optimal_interface() -> Optional[str]:
        """Get the optimal network interface for Ray networking."""
        interfaces = NetworkDetector.get_available_interfaces()

        # Prefer ethernet interfaces
        for iface in interfaces:
            if iface.startswith(
                ("eth", "ens", "enp")
            ) and NetworkDetector.is_interface_up(iface):
                return iface

        # Fallback to any available interface
        for iface in interfaces:
            if NetworkDetector.is_interface_up(iface):
                return iface

        return None

    @staticmethod
    def is_interface_up(interface: str) -> bool:
        """Check if network interface is up and has an IP address."""
        try:
            result = subprocess.run(
                ["ip", "addr", "show", interface],
                capture_output=True,
                text=True,
                timeout=2,
            )
            return (
                result.returncode == 0
                and "inet " in result.stdout
                and "UP" in result.stdout
            )
        except (subprocess.SubprocessError, FileNotFoundError):
            return False


class PathUtils:
    """Utility class for path management."""

    @staticmethod
    def get_optimal_temp_dir() -> str:
        """Get the optimal temporary directory for the current environment."""
        config = get_config()

        if config.environment.is_databricks:
            return Defaults.DATABRICKS_DEFAULT_TMP_ROOT_DIR

        # Check for YARN local directories
        if config.environment.is_yarn:
            yarn_dirs = os.environ.get(EnvironmentVariables.YARN_LOCAL_DIRS, "").split(
                ","
            )
            for yarn_dir in yarn_dirs:
                yarn_dir = yarn_dir.strip()
                if (
                    yarn_dir
                    and os.path.exists(yarn_dir)
                    and os.access(yarn_dir, os.W_OK)
                ):
                    return os.path.join(yarn_dir, "ray-spark")

        # Check for Kubernetes-specific paths
        if config.environment.is_kubernetes:
            k8s_temp = "/tmp/ray-spark"
            try:
                os.makedirs(k8s_temp, exist_ok=True)
                return k8s_temp
            except OSError:
                pass

        # Default to system temp directory
        return Defaults.DEFAULT_TEMP_ROOT_DIR

    @staticmethod
    def ensure_directory_exists(path: str) -> bool:
        """Ensure directory exists and is writable."""
        try:
            os.makedirs(path, exist_ok=True)
            return os.access(path, os.W_OK)
        except OSError:
            return False


class ProcessUtils:
    """Utility class for process management."""

    @staticmethod
    def set_cpu_affinity(cpu_list: Optional[List[int]] = None) -> bool:
        """Set CPU affinity for the current process."""
        try:
            import psutil

            if cpu_list is None:
                # Use all available CPUs
                cpu_list = list(range(ResourceDetector.get_cpu_count()))

            current_process = psutil.Process()
            current_process.cpu_affinity(cpu_list)
            return True
        except (ImportError, psutil.AccessDenied, AttributeError):
            return False

    @staticmethod
    def get_process_memory_usage() -> Dict[str, int]:
        """Get current process memory usage."""
        try:
            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()
            return {
                "rss": memory_info.rss,
                "vms": memory_info.vms,
                "percent": process.memory_percent(),
            }
        except (ImportError, psutil.NoSuchProcess):
            return {}


class ValidationUtils:
    """Utility class for validation operations."""

    @staticmethod
    def validate_spark_version(min_version: Tuple[int, int, int] = (3, 3, 0)) -> bool:
        """Validate Spark version meets minimum requirements."""
        try:
            import pyspark
            from packaging.version import Version

            current_version = Version(pyspark.__version__)
            min_version_obj = Version(
                f"{min_version[0]}.{min_version[1]}.{min_version[2]}"
            )

            return current_version >= min_version_obj
        except ImportError:
            return False

    @staticmethod
    def validate_system_requirements() -> List[str]:
        """Validate system requirements and return list of issues."""
        issues = []

        # Check OS
        if os.name != "posix":
            issues.append("Ray on Spark only supports POSIX systems")

        # Check Spark version
        if not ValidationUtils.validate_spark_version():
            issues.append("Ray on Spark requires PySpark >= 3.3.0")

        # Check memory
        memory_info = ResourceDetector.get_memory_info()
        if memory_info["effective_total"] < 1024**3:  # 1GB
            issues.append("Insufficient memory: at least 1GB required")

        # Check disk space
        try:
            temp_dir = PathUtils.get_optimal_temp_dir()
            if os.path.exists(temp_dir):
                usage = shutil.disk_usage(temp_dir)
                if usage.free < 512 * 1024**2:  # 512MB
                    issues.append(
                        f"Insufficient disk space in {temp_dir}: at least 512MB required"
                    )
        except OSError:
            issues.append("Cannot access temporary directory")

        return issues


class LoggingUtils:
    """Utility class for logging configuration."""

    @staticmethod
    def setup_logging(level: str = "INFO", format_string: Optional[str] = None) -> None:
        """Setup logging configuration for Ray on Spark."""
        if format_string is None:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format=format_string,
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Set specific logger levels
        logging.getLogger("ray.util.spark").setLevel(level.upper())

    @staticmethod
    def log_system_info() -> None:
        """Log system information for debugging."""
        config = get_config()

        _logger.info("Ray on Spark System Information:")
        _logger.info(f"  Environment: {config.environment}")
        _logger.info(f"  CPU cores: {ResourceDetector.get_cpu_count()}")

        memory_info = ResourceDetector.get_memory_info()
        _logger.info(f"  Memory: {memory_info['effective_total'] // 1024**3}GB total")

        gpu_count = ResourceDetector.get_gpu_count()
        if gpu_count > 0:
            _logger.info(f"  GPUs: {gpu_count}")

        optimal_iface = NetworkDetector.get_optimal_interface()
        if optimal_iface:
            _logger.info(f"  Network interface: {optimal_iface}")


def wait_for_condition(
    condition_func, timeout: float = 30.0, retry_interval: float = 0.5, *args, **kwargs
) -> bool:
    """Wait for a condition to become true with timeout."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            if condition_func(*args, **kwargs):
                return True
        except Exception as e:
            _logger.debug(f"Condition check failed: {e}")

        time.sleep(retry_interval)

    return False


def execute_command(
    command: List[str], timeout: float = 30.0, check: bool = True
) -> subprocess.CompletedProcess:
    """Execute command with timeout and error handling."""
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=check
        )
        return result
    except subprocess.TimeoutExpired:
        _logger.error(f"Command timed out after {timeout}s: {' '.join(command)}")
        raise
    except subprocess.CalledProcessError as e:
        _logger.error(
            f"Command failed with exit code {e.returncode}: {' '.join(command)}"
        )
        _logger.error(f"stderr: {e.stderr}")
        raise
