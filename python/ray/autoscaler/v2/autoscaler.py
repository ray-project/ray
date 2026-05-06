import logging
from queue import Queue
from typing import List, Optional
from urllib.parse import urlsplit

from ray._raylet import GcsClient
from ray.autoscaler._private.providers import _get_node_provider
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.cloud_provider import (
    KubeRayProvider,
)
from ray.autoscaler.v2.instance_manager.cloud_providers.read_only.cloud_provider import (  # noqa
    ReadOnlyProvider,
)
from ray.autoscaler.v2.instance_manager.config import (
    AutoscalingConfig,
    IConfigReader,
    Provider,
)
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceManager,
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import (
    ICloudInstanceProvider,
    NodeProviderAdapter,
)
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstaller
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
    CloudResourceMonitor,
)
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import RayStopper
from ray.autoscaler.v2.instance_manager.subscribers.threaded_ray_installer import (
    ThreadedRayInstaller,
)
from ray.autoscaler.v2.metrics_reporter import AutoscalerMetricsReporter
from ray.autoscaler.v2.scheduler import ResourceDemandScheduler
from ray.autoscaler.v2.sdk import get_cluster_resource_state
from ray.core.generated.autoscaler_pb2 import (
    AutoscalingState,
    ClusterConfig,
    NodeGroupConfig,
)
from ray.exceptions import AuthenticationError

logger = logging.getLogger(__name__)


class Autoscaler:
    def __init__(
        self,
        session_name: str,
        config_reader: IConfigReader,
        gcs_client: GcsClient,
        event_logger: Optional[AutoscalerEventLogger] = None,
        metrics_reporter: Optional[AutoscalerMetricsReporter] = None,
    ) -> None:
        """
        Args:
            session_name: The current Ray session name.
            config_reader: The config reader.
            gcs_client: The GCS client.
            event_logger: The event logger for emitting cluster events.
            metrics_reporter: The metrics reporter for emitting cluster metrics.
        """

        self._config_reader = config_reader

        config = config_reader.get_cached_autoscaling_config()
        logger.info(f"Using Autoscaling Config: \n{config.dump()}")

        self._gcs_client = gcs_client
        self._cloud_instance_provider = None
        self._instance_manager = None
        self._ray_stop_errors_queue = Queue()
        self._ray_install_errors_queue = Queue()
        self._event_logger = event_logger
        self._metrics_reporter = metrics_reporter

        self._init_cloud_instance_provider(config, config_reader)
        self._cloud_resource_monitor = None
        self._init_instance_manager(
            session_name=session_name,
            config=config,
            cloud_provider=self._cloud_instance_provider,
            gcs_client=self._gcs_client,
        )
        self._scheduler = ResourceDemandScheduler(self._event_logger)

        # Report the initial cluster config to GCS so that consumers
        # (e.g. DefaultClusterAutoscalerV2) can read it via
        # ray._private.state.state.get_cluster_config().
        self._last_reported_cluster_config = None
        self._report_cluster_config(config)

    def _init_cloud_instance_provider(
        self, config: AutoscalingConfig, config_reader: IConfigReader
    ):
        """
        Initialize the cloud provider, and its dependencies (the v1 node provider)

        Args:
            config: The autoscaling config.
            config_reader: The config reader.

        """
        provider_config = config.get_provider_config()
        if provider_config["type"] == "kuberay":
            provider_config["head_node_type"] = config.get_head_node_type()
            self._cloud_instance_provider = KubeRayProvider(
                config.get_config("cluster_name"),
                provider_config,
            )
        elif config.provider == Provider.READ_ONLY:
            provider_config["gcs_address"] = self._gcs_client.address
            self._cloud_instance_provider = ReadOnlyProvider(
                provider_config=provider_config,
            )
        else:
            node_provider_v1 = _get_node_provider(
                provider_config,
                config.get_config("cluster_name"),
            )

            self._cloud_instance_provider = NodeProviderAdapter(
                v1_provider=node_provider_v1,
                config_reader=config_reader,
            )

    def _init_instance_manager(
        self,
        session_name: str,
        cloud_provider: ICloudInstanceProvider,
        gcs_client: GcsClient,
        config: AutoscalingConfig,
    ):
        """
        Initialize the instance manager, and its dependencies.
        """

        instance_storage = InstanceStorage(
            cluster_id=session_name,
            storage=InMemoryStorage(),
        )
        subscribers: List[InstanceUpdatedSubscriber] = []
        subscribers.append(
            CloudInstanceUpdater(
                cloud_provider=cloud_provider,
                metrics_reporter=self._metrics_reporter,
            )
        )
        subscribers.append(
            RayStopper(gcs_client=gcs_client, error_queue=self._ray_stop_errors_queue)
        )
        if not config.disable_node_updaters() and isinstance(
            cloud_provider, NodeProviderAdapter
        ):
            head_node_ip = urlsplit("//" + self._gcs_client.address).hostname
            assert head_node_ip is not None, "Invalid GCS address format"
            subscribers.append(
                ThreadedRayInstaller(
                    head_node_ip=head_node_ip,
                    instance_storage=instance_storage,
                    ray_installer=RayInstaller(
                        provider=cloud_provider.v1_provider,
                        config=config,
                    ),
                    error_queue=self._ray_install_errors_queue,
                    # TODO(rueian): Rewrite the ThreadedRayInstaller and its underlying
                    # NodeUpdater and CommandRunner to use the asyncio, so that we don't
                    # need to use so many threads. We use so many threads now because
                    # they are blocking and letting the new cloud machines to wait for
                    # previous machines to finish installing Ray is quite inefficient.
                    max_concurrent_installs=config.get_max_num_worker_nodes() or 50,
                )
            )
        self._cloud_resource_monitor = CloudResourceMonitor()
        subscribers.append(self._cloud_resource_monitor)

        self._instance_manager = InstanceManager(
            instance_storage=instance_storage,
            instance_status_update_subscribers=subscribers,
        )

    @staticmethod
    def _autoscaling_config_to_cluster_config(
        config: AutoscalingConfig,
    ) -> ClusterConfig:
        """Convert an AutoscalingConfig to a ClusterConfig protobuf.

        The mapping is:
          available_node_types[name].resources  -> NodeGroupConfig.resources
          available_node_types[name].min_workers -> NodeGroupConfig.min_count
          available_node_types[name].max_workers -> NodeGroupConfig.max_count
          node type name                        -> NodeGroupConfig.name

        The head node type is excluded because it is not scalable and the
        downstream consumers (e.g. _get_node_resource_spec_and_count) already
        filter out the head node from running node counts.  Including it would
        create a phantom available slot since the head is excluded from the
        running-node scan but would still appear in the max-count map.
        """
        cluster_config = ClusterConfig()

        node_type_configs = config.get_node_type_configs()
        if not node_type_configs:
            return cluster_config

        head_node_type = config.get_head_node_type()

        for node_type_name, ntc in node_type_configs.items():
            # Skip the head node -- it is not scalable.
            if node_type_name == head_node_type:
                continue

            ngc = NodeGroupConfig()
            ngc.name = node_type_name

            # Convert resource values from float to uint64.
            for resource_name, resource_value in ntc.resources.items():
                ngc.resources[resource_name] = int(resource_value)

            ngc.min_count = ntc.min_worker_nodes
            ngc.max_count = ntc.max_worker_nodes

            cluster_config.node_group_configs.append(ngc)

        return cluster_config

    def _report_cluster_config(self, config: AutoscalingConfig) -> None:
        """Convert the autoscaling config and report it to GCS.

        This writes the ClusterConfig protobuf into GCS Internal KV so that
        other components (e.g. Ray Data's DefaultClusterAutoscalerV2) can read
        it via ray._private.state.state.get_cluster_config().
        """
        try:
            cluster_config = self._autoscaling_config_to_cluster_config(config)
            serialized_config = cluster_config.SerializeToString()
            if serialized_config == self._last_reported_cluster_config:
                return
            self._gcs_client.report_cluster_config(serialized_config)
            self._last_reported_cluster_config = serialized_config
            logger.info(
                "Reported cluster config to GCS with %d node group(s).",
                len(cluster_config.node_group_configs),
            )
        except Exception:
            logger.exception("Failed to report cluster config to GCS.")

    def update_autoscaling_state(
        self,
    ) -> Optional[AutoscalingState]:
        """Update the autoscaling state of the cluster by reconciling the current
        state of the cluster resources, the cloud providers as well as instance
        update subscribers with the desired state.

        Returns:
            AutoscalingState: The new autoscaling state of the cluster or None if
            the state is not updated.

        Raises:
            None: No exception.
        """

        try:
            ray_stop_errors = []
            while not self._ray_stop_errors_queue.empty():
                ray_stop_errors.append(self._ray_stop_errors_queue.get())

            ray_install_errors = []
            while not self._ray_install_errors_queue.empty():
                ray_install_errors.append(self._ray_install_errors_queue.get())

            # Get the current state of the ray cluster resources.
            ray_cluster_resource_state = get_cluster_resource_state(self._gcs_client)

            # Refresh the config from the source
            self._config_reader.refresh_cached_autoscaling_config()
            autoscaling_config = self._config_reader.get_cached_autoscaling_config()

            # Report the (possibly updated) cluster config to GCS so that
            # downstream consumers always see the latest node group configs.
            self._report_cluster_config(autoscaling_config)

            return Reconciler.reconcile(
                instance_manager=self._instance_manager,
                scheduler=self._scheduler,
                cloud_provider=self._cloud_instance_provider,
                cloud_resource_monitor=self._cloud_resource_monitor,
                ray_cluster_resource_state=ray_cluster_resource_state,
                non_terminated_cloud_instances=(
                    self._cloud_instance_provider.get_non_terminated()
                ),
                cloud_provider_errors=self._cloud_instance_provider.poll_errors(),
                ray_install_errors=ray_install_errors,
                ray_stop_errors=ray_stop_errors,
                autoscaling_config=autoscaling_config,
                metrics_reporter=self._metrics_reporter,
            )
        except AuthenticationError as e:
            logger.warning(f"AuthenticationError detected, restarting autoscaler: {e}")
            raise
        except Exception as e:
            logger.exception(e)
            return None
