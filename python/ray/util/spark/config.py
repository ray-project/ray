"""
Configuration management for Ray on Spark.

This module provides centralized configuration management for all Ray on Spark
functionality, including environment detection, resource limits, and optimization
settings.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union


# Environment variable constants
class EnvironmentVariables:
    """
    Environment variable names used throughout Ray on Spark.

    This class provides a centralized definition of all environment variables
    used by Ray on Spark, making it easier to maintain and avoid naming conflicts.
    """

    # Core Ray on Spark variables
    RAY_ON_SPARK_START_HOOK = "RAY_ON_SPARK_START_HOOK"
    RAY_ON_SPARK_COLLECT_LOG_TO_PATH = "RAY_ON_SPARK_COLLECT_LOG_TO_PATH"
    RAY_ON_SPARK_START_RAY_PARENT_PID = "RAY_ON_SPARK_START_RAY_PARENT_PID"

    # Resource configuration overrides
    RAY_ON_SPARK_WORKER_CPU_CORES = "RAY_ON_SPARK_WORKER_CPU_CORES"
    RAY_ON_SPARK_WORKER_GPU_NUM = "RAY_ON_SPARK_WORKER_GPU_NUM"
    RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES = (
        "RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES"
    )
    RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES = "RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES"
    RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES = (
        "RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES"
    )
    RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES = "RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES"

    # Databricks-specific variables
    DATABRICKS_RUNTIME_VERSION = "DATABRICKS_RUNTIME_VERSION"
    DATABRICKS_HOST = "DATABRICKS_HOST"
    DATABRICKS_TOKEN = "DATABRICKS_TOKEN"
    DATABRICKS_CLIENT_ID = "DATABRICKS_CLIENT_ID"
    DATABRICKS_CLIENT_SECRET = "DATABRICKS_CLIENT_SECRET"
    DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES = (
        "DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES"
    )

    # Container and orchestration variables
    YARN_CONTAINER_ID = "YARN_CONTAINER_ID"
    YARN_LOCAL_DIRS = "YARN_LOCAL_DIRS"
    KUBERNETES_SERVICE_HOST = "KUBERNETES_SERVICE_HOST"


# Default values and constants
class Defaults:
    """
    Default values for Ray on Spark configuration.

    This class centralizes all default values used throughout Ray on Spark,
    making it easier to maintain consistent defaults and tune performance.
    """

    # Memory management
    RAY_ON_SPARK_MAX_OBJECT_STORE_MEMORY_PROPORTION = 0.8
    RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET = 0.8
    OBJECT_STORE_MINIMUM_MEMORY_BYTES = 100 * 1024 * 1024  # 100MB

    # Databricks-specific defaults
    DATABRICKS_DEFAULT_TMP_ROOT_DIR = "/local_disk0/tmp"
    DATABRICKS_AUTO_SHUTDOWN_POLL_INTERVAL_SECONDS = 3
    DATABRICKS_DEFAULT_AUTO_SHUTDOWN_MINUTES = 30

    # Ray cluster defaults
    MAX_NUM_WORKER_NODES = -1
    DEFAULT_TEMP_ROOT_DIR = "/tmp"

    # Network defaults
    DEFAULT_NETWORK_TIMEOUT_SECONDS = 30
    DEFAULT_NETWORK_RETRY_COUNT = 3


@dataclass
class ResourceLimits:
    """Resource limits configuration."""

    cpu_cores: Optional[int] = None
    memory_bytes: Optional[int] = None
    gpu_count: Optional[int] = None
    shared_memory_bytes: Optional[int] = None

    def is_valid(self) -> bool:
        """Check if resource limits are valid."""
        return (
            (self.cpu_cores is None or self.cpu_cores > 0)
            and (self.memory_bytes is None or self.memory_bytes > 0)
            and (self.gpu_count is None or self.gpu_count >= 0)
            and (self.shared_memory_bytes is None or self.shared_memory_bytes > 0)
        )


@dataclass
class EnvironmentInfo:
    """Information about the deployment environment."""

    is_databricks: bool = False
    is_kubernetes: bool = False
    is_yarn: bool = False
    is_containerized: bool = False
    is_local_mode: bool = False

    spark_master: Optional[str] = None
    databricks_runtime_version: Optional[str] = None
    yarn_container_id: Optional[str] = None

    def __post_init__(self):
        """Automatically detect environment information."""
        self.is_databricks = (
            EnvironmentVariables.DATABRICKS_RUNTIME_VERSION in os.environ
        )
        self.is_kubernetes = EnvironmentVariables.KUBERNETES_SERVICE_HOST in os.environ
        self.is_yarn = EnvironmentVariables.YARN_CONTAINER_ID in os.environ
        self.is_containerized = (
            os.path.exists("/.dockerenv")
            or self.is_kubernetes
            or self._detect_container_from_cgroup()
        )

        if self.is_databricks:
            self.databricks_runtime_version = os.environ.get(
                EnvironmentVariables.DATABRICKS_RUNTIME_VERSION
            )

        if self.is_yarn:
            self.yarn_container_id = os.environ.get(
                EnvironmentVariables.YARN_CONTAINER_ID
            )

    def _detect_container_from_cgroup(self) -> bool:
        """Detect if running in container by checking cgroup information."""
        try:
            # Check if we're in a container by looking for container-specific cgroup entries
            with open("/proc/1/cgroup", "r") as f:
                cgroup_content = f.read()
                # Look for container indicators (docker, containerd, crio, etc.)
                container_indicators = [
                    "docker",
                    "containerd",
                    "crio",
                    "lxc",
                    "systemd:/docker",
                ]
                return any(
                    indicator in cgroup_content for indicator in container_indicators
                )
        except (OSError, FileNotFoundError):
            return False


@dataclass
class ClusterConfig:
    """Configuration for Ray cluster on Spark."""

    max_worker_nodes: int
    min_worker_nodes: Optional[int] = None
    num_cpus_worker_node: Optional[int] = None
    num_cpus_head_node: Optional[int] = None
    num_gpus_worker_node: Optional[int] = None
    num_gpus_head_node: Optional[int] = None
    memory_worker_node: Optional[int] = None
    memory_head_node: Optional[int] = None
    object_store_memory_worker_node: Optional[int] = None
    object_store_memory_head_node: Optional[int] = None
    head_node_options: Optional[Dict] = field(default_factory=dict)
    worker_node_options: Optional[Dict] = field(default_factory=dict)
    ray_temp_root_dir: Optional[str] = None
    strict_mode: bool = False
    collect_log_to_path: Optional[str] = None
    autoscale_upscaling_speed: Optional[float] = 1.0
    autoscale_idle_timeout_minutes: Optional[float] = 1.0

    def __post_init__(self):
        """Validate configuration after initialization."""
        if (
            self.max_worker_nodes <= 0
            and self.max_worker_nodes != Defaults.MAX_NUM_WORKER_NODES
        ):
            raise ValueError("max_worker_nodes must be positive or -1 for unlimited")

        if self.min_worker_nodes is not None and self.min_worker_nodes < 0:
            raise ValueError("min_worker_nodes must be non-negative")

        if (
            self.min_worker_nodes is not None
            and self.max_worker_nodes != Defaults.MAX_NUM_WORKER_NODES
            and self.min_worker_nodes > self.max_worker_nodes
        ):
            raise ValueError("min_worker_nodes cannot exceed max_worker_nodes")


@dataclass
class OptimizationConfig:
    """Configuration for OSS Spark optimizations."""

    enable_cpu_optimizations: bool = True
    enable_memory_optimizations: bool = True
    enable_network_optimizations: bool = True
    enable_storage_optimizations: bool = True
    enable_container_detection: bool = True
    enable_numa_optimization: bool = True

    # CPU optimization settings
    cpu_affinity_enabled: bool = True
    thread_pool_optimization: bool = True

    # Memory optimization settings
    gc_optimization_enabled: bool = True
    memory_pressure_handling: bool = True

    # Network optimization settings
    network_interface_detection: bool = True
    network_compression_enabled: bool = True

    # Logging and debugging
    debug_logging_enabled: bool = False
    performance_logging_enabled: bool = True


class Config:
    """Main configuration class for Ray on Spark."""

    def __init__(self):
        self._environment = EnvironmentInfo()
        self._optimization = OptimizationConfig()
        self._resource_limits = ResourceLimits()

    @property
    def environment(self) -> EnvironmentInfo:
        """Get environment information."""
        return self._environment

    @property
    def optimization(self) -> OptimizationConfig:
        """Get optimization configuration."""
        return self._optimization

    @property
    def resource_limits(self) -> ResourceLimits:
        """Get resource limits."""
        return self._resource_limits

    def is_databricks_runtime(self) -> bool:
        """Check if running in Databricks runtime."""
        return self._environment.is_databricks

    def is_oss_spark_environment(self) -> bool:
        """Check if running in OSS Apache Spark environment."""
        return not self._environment.is_databricks

    def get_temp_root_dir(self) -> str:
        """Get appropriate temporary root directory for environment."""
        if self._environment.is_databricks:
            return Defaults.DATABRICKS_DEFAULT_TMP_ROOT_DIR
        else:
            return Defaults.DEFAULT_TEMP_ROOT_DIR

    def get_environment_variables(self) -> Dict[str, str]:
        """Get environment variables for current configuration."""
        env_vars = {}

        # Basic Ray configuration
        env_vars.update(
            {
                "RAY_BACKEND_LOG_LEVEL": (
                    "warning"
                    if not self.optimization.debug_logging_enabled
                    else "debug"
                ),
                "RAY_ENABLE_AUTO_CONNECT": "1",
            }
        )

        # Environment-specific variables
        if self._environment.is_kubernetes:
            env_vars.update(
                {
                    "RAY_ENABLE_KUBERNETES": "1",
                    "RAY_USAGE_STATS_ENABLED": "0",
                }
            )

        if self._environment.is_yarn:
            env_vars.update(
                {
                    "RAY_ENABLE_MULTI_TENANCY": "1",
                }
            )

        return env_vars

    def update_from_environment(self):
        """Update configuration from environment variables."""
        # Update resource limits from environment
        if EnvironmentVariables.RAY_ON_SPARK_WORKER_CPU_CORES in os.environ:
            try:
                self._resource_limits.cpu_cores = int(
                    os.environ[EnvironmentVariables.RAY_ON_SPARK_WORKER_CPU_CORES]
                )
            except ValueError:
                pass

        if EnvironmentVariables.RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES in os.environ:
            try:
                self._resource_limits.memory_bytes = int(
                    os.environ[
                        EnvironmentVariables.RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES
                    ]
                )
            except ValueError:
                pass

        # Update optimization settings
        if "RAY_ON_SPARK_DEBUG" in os.environ:
            self._optimization.debug_logging_enabled = os.environ[
                "RAY_ON_SPARK_DEBUG"
            ].lower() in ("1", "true", "yes")


# Global configuration instance
_config: Optional[Config] = None


def get_config() -> Config:
    """Get global configuration instance."""
    global _config
    if _config is None:
        _config = Config()
        _config.update_from_environment()
    return _config


def reset_config():
    """Reset global configuration (mainly for testing)."""
    global _config
    _config = None
