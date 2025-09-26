import logging
import os
from typing import Dict, Optional

from .common import NetworkDetector, PathUtils, ProcessUtils, ResourceDetector
from .config import get_config
from .start_hook_base import RayOnSparkStartHook
from .utils import get_spark_session

_logger = logging.getLogger(__name__)


class SparkStartHook(RayOnSparkStartHook):
    """
    Start hook for Apache Spark deployments.

    This hook provides performance improvements, resource detection, and
    compatibility for Apache Spark environments including standalone
    clusters, YARN, and Kubernetes deployments.
    """

    def __init__(self, is_global):
        super().__init__(is_global)
        self._spark_version = None
        self._detected_environment = None

    def get_default_temp_root_dir(self):
        """Get temporary directory for Apache Spark environments."""
        return PathUtils.get_optimal_temp_dir()

    def on_ray_dashboard_created(self, port):
        """
        Dashboard notification for Apache Spark environments.

        Provides clear dashboard access information for different Spark deployment modes.
        """
        try:
            spark = get_spark_session()
            driver_host = spark.sparkContext.getConf().get(
                "spark.driver.host", "localhost"
            )

            # Detect deployment environment for better user guidance
            spark_master = spark.sparkContext.master

            if spark_master.startswith("k8s://"):
                deployment_type = "Kubernetes"
                access_note = (
                    "To access the Ray dashboard in Kubernetes, you may need to set up "
                    "port forwarding: kubectl port-forward <driver-pod> {port}:{port}"
                )
            elif spark_master.startswith("yarn"):
                deployment_type = "YARN"
                access_note = (
                    "Ray dashboard is available on the Spark driver node. "
                    "Check your YARN ResourceManager UI for driver node details."
                )
            else:
                deployment_type = "Standalone"
                access_note = "Ray dashboard is accessible directly on the driver node."

            print(
                f"Ray Dashboard ({deployment_type} deployment): http://{driver_host}:{port}"
            )
            print(f"Note: {access_note}")

        except Exception as e:
            _logger.debug(f"Could not determine dashboard access details: {e}")
            print(f"Ray Dashboard available on port {port}")

    def on_cluster_created(self, ray_cluster_handler):
        """
        Cluster creation handling for Apache Spark environments.

        Provides resource monitoring, performance optimization, and
        automatic cleanup for Apache Spark deployments.
        """
        _logger.info(
            "Ray cluster created on Apache Spark. " "Configuration features are active."
        )

        # Apply configuration for Apache Spark environments
        self._configure_for_spark_environment()

        # Apply CPU affinity configuration
        ProcessUtils.set_cpu_affinity()

        _logger.info("Apache Spark configuration applied")

    def _log_deployment_info(self):
        """Log OSS Spark deployment information for debugging."""
        try:
            spark = get_spark_session()

            # Log basic deployment info (not monitoring, just initial setup info)
            executor_memory = spark.sparkContext.getConf().get(
                "spark.executor.memory", "1g"
            )
            executor_cores = spark.sparkContext.getConf().get(
                "spark.executor.cores", "1"
            )
            master = spark.sparkContext.master

            _logger.info(
                f"OSS Spark deployment: master={master}, "
                f"executor_cores={executor_cores}, executor_memory={executor_memory}"
            )

        except Exception as e:
            _logger.debug(f"Could not get deployment info: {e}")

    def _configure_for_spark_environment(self):
        """Apply Apache Spark-specific configuration (configuration only, no monitoring)."""
        try:
            # Log deployment info for context
            self._log_deployment_info()

            # Apply basic Apache Spark configuration (lightweight, no background threads)
            os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")
            os.environ.setdefault("RAY_ENABLE_AUTO_CONNECT", "1")

            _logger.info("Apache Spark environment configuration applied")

        except Exception as e:
            _logger.debug(f"Could not apply Spark configuration: {e}")

    def on_spark_job_created(self, job_group_id):
        """
        Spark job tracking for Apache Spark environments.

        Provides job monitoring and resource tracking for Apache Spark.
        """
        _logger.info(f"Spark job created with group ID: {job_group_id}")

        # Job tracking for Apache Spark environments
        try:
            spark = get_spark_session()
            app_id = spark.sparkContext.applicationId
            app_name = spark.sparkContext.appName

            _logger.info(
                f"Ray on Spark job tracking: App={app_name}, "
                f"AppId={app_id}, JobGroup={job_group_id}"
            )

        except Exception as e:
            _logger.debug(f"Could not get Spark job details: {e}")

    def custom_environment_variables(self) -> Dict[str, str]:
        """
        Environment variables for Apache Spark deployments.

        Returns environment configurations that improve performance
        and compatibility for Apache Spark deployments across different
        environments including standalone, YARN, and Kubernetes.

        Returns:
            Dict[str, str]: Environment variables for Ray configuration
        """
        conf = {
            **super().custom_environment_variables(),
            # Enable automatic connection discovery for Apache Spark
            "RAY_ENABLE_AUTO_CONNECT": "1",
            # Set appropriate logging level for production use
            "RAY_BACKEND_LOG_LEVEL": "warning",
            # Allow object store to use filesystem when shared memory limited
            "RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE": "1",
            # Enable task event recording for better debugging
            "RAY_ENABLE_RECORD_TASK_EVENTS": "1",
            # Set memory monitoring refresh rate for container environments
            "RAY_memory_monitor_refresh_ms": "100",
        }

        # Apply Apache Spark specific configurations
        try:
            # Apply CPU configuration for Apache Spark
            cpu_config = self._get_cpu_config()
            conf.update(cpu_config)

            # Apply memory configuration for Apache Spark
            memory_config = self._get_memory_config()
            conf.update(memory_config)

            # Apply network configuration for Apache Spark
            network_config = self._get_network_config()
            conf.update(network_config)

        except Exception as e:
            _logger.debug(f"Could not apply configurations: {e}")

        # Apply environment-specific configurations from global config
        config = get_config()
        env_vars = config.get_environment_variables()
        conf.update(env_vars)

        return conf

    def _get_cpu_config(self) -> Dict[str, str]:
        """
        Get CPU configuration for Apache Spark environments.

        Configures CPU-related environment variables including thread limits
        and core allocation based on detected system resources.

        Returns:
            Dict[str, str]: CPU configuration environment variables
        """
        cpu_config = {}

        try:
            # Detect effective CPU cores (container-aware detection)
            effective_cores = ResourceDetector.get_cpu_count()

            # Configure thread pools for mathematical libraries
            # Use half the cores to avoid oversubscription in containerized environments
            thread_count = max(1, effective_cores // 2)

            cpu_config.update(
                {
                    # OpenMP thread configuration for numerical libraries
                    "OMP_NUM_THREADS": str(thread_count),
                    # Intel MKL thread configuration
                    "MKL_NUM_THREADS": str(thread_count),
                    # Ray CPU count for resource allocation
                    "RAY_CPU_COUNT": str(effective_cores),
                }
            )

            _logger.debug(f"Apache Spark CPU config: {effective_cores} cores")

        except Exception as e:
            _logger.debug(f"CPU configuration error: {e}")

        return cpu_config

    def _get_memory_config(self) -> Dict[str, str]:
        """
        Get memory configuration for Apache Spark environments.

        Configures memory-related environment variables including garbage
        collection settings and pressure handling based on system resources.

        Returns:
            Dict[str, str]: Memory configuration environment variables
        """
        memory_config = {}

        try:
            # Get system memory information
            memory_info = ResourceDetector.get_memory_info()

            # Configure memory management settings
            memory_config.update(
                {
                    # Allow object store to use slower storage when needed
                    "RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE": "1",
                    # Enable memory pressure handling for containerized environments
                    "RAY_ENABLE_MEMORY_PRESSURE_HANDLING": "1",
                }
            )

            # Apply container-specific memory settings
            if "container_limit" in memory_info:
                memory_config.update(
                    {
                        # Ensure deterministic Python hashing
                        "PYTHONHASHSEED": "0",
                        # More aggressive garbage collection in containers
                        "RAY_GC_THRESHOLD_0": "700",
                    }
                )
                container_gb = memory_info["container_limit"] // 1024**3
                _logger.debug(f"Apache Spark container memory: {container_gb}GB")

        except Exception as e:
            _logger.debug(f"Memory configuration error: {e}")

        return memory_config

    def _get_network_config(self) -> Dict[str, str]:
        """
        Get network configuration for Apache Spark environments.

        Configures network-related environment variables including interface
        detection and timeout settings based on deployment environment.

        Returns:
            Dict[str, str]: Network configuration environment variables
        """
        network_config = {}

        try:
            config = get_config()

            # Configure basic network settings
            network_config.update(
                {
                    # Enable TCP_NODELAY for better latency
                    "RAY_ENABLE_TCP_NODELAY": "1",
                    # Set appropriate network timeout
                    "RAY_NETWORK_TIMEOUT_SECONDS": "30",
                    # Enable automatic connection discovery
                    "RAY_ENABLE_AUTO_CONNECT": "1",
                }
            )

            # Detect and configure network interface
            optimal_iface = NetworkDetector.get_optimal_interface()
            if optimal_iface and optimal_iface != "lo":
                network_config["RAY_NETWORK_INTERFACE"] = optimal_iface
                _logger.debug(f"Apache Spark network interface: {optimal_iface}")

            # Apply environment-specific network settings
            if config.environment.is_kubernetes:
                # Use host networking mode in Kubernetes for better performance
                network_config["RAY_K8S_NETWORK_MODE"] = "host"
            elif config.environment.is_yarn:
                # Use bridge networking mode in YARN environments
                network_config["RAY_YARN_NETWORK_MODE"] = "bridge"

        except Exception as e:
            _logger.debug(f"Network configuration error: {e}")

        return network_config
