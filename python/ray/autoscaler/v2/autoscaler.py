import logging
from queue import Queue
from typing import List, Optional

from ray._raylet import GcsClient
from ray.autoscaler._private.providers import _get_node_provider
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig, IConfigReader
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceManager,
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import (
    ICloudInstanceProvider,
    NodeProviderAdapter,
)
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import RayStopper
from ray.autoscaler.v2.metrics_reporter import AutoscalerMetricsReporter
from ray.autoscaler.v2.scheduler import ResourceDemandScheduler
from ray.core.generated.autoscaler_pb2 import (
    AutoscalingState,
    ClusterResourceState,
    GetClusterResourceStateReply,
)

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
            session_name: The name of the ray session.
            config_reader: The config reader.
            gcs_client: The GCS client.
            event_logger: The event logger for emitting cluster events.
            metrics_reporter: The metrics reporter for emitting cluster metrics.
        """

        self._config_reader = config_reader
        config = config_reader.get_autoscaling_config()
        logger.info(config.dump())

        self._gcs_client = gcs_client
        self._cloud_provider = None
        self._instance_manager = None
        self._ray_stop_errors_queue = Queue()
        self._ray_install_errors_queue = Queue()
        self._event_logger = event_logger
        self._metrics_reporter = metrics_reporter

        self._init_cloud_provider(config, config_reader)
        self._init_instance_manager(
            session_name=session_name,
            config=config,
            cloud_provider=self._cloud_provider,
            gcs_client=self._gcs_client,
        )
        self._scheduler = ResourceDemandScheduler(self._event_logger)

    def _init_cloud_provider(
        self, config: AutoscalingConfig, config_reader: IConfigReader
    ):
        """
        Initialize the cloud provider, and its dependencies (the v1 node provider)

        Args:
            config: The autoscaling config.
            config_reader: The config reader.

        """
        node_provider_v1 = _get_node_provider(
            config.get_provider_config(),
            config.get_config("cluster_name"),
        )

        self._cloud_provider = NodeProviderAdapter(
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
        subscribers.append(CloudInstanceUpdater(cloud_provider=cloud_provider))
        subscribers.append(
            RayStopper(gcs_client=gcs_client, error_queue=self._ray_stop_errors_queue)
        )
        if not config.disable_node_updaters():
            # Supporting ray installer is only needed for providers that install
            # and manage ray. Not for providers that only launch instances
            # (e.g. KubeRay)
            raise NotImplementedError("RayInstaller is not implemented yet.")

        self._instance_manager = InstanceManager(
            instance_storage=instance_storage,
            instance_status_update_subscribers=subscribers,
        )

    def _get_cluster_resource_state(self) -> ClusterResourceState:
        """
        Get the current state of the ray cluster resources.

        Returns:
            ClusterResourceState: The current state of the cluster resources.
        """
        str_reply = self._gcs_client.get_cluster_resource_state()
        reply = GetClusterResourceStateReply()
        reply.ParseFromString(str_reply)

        return reply.cluster_resource_state

    def update_autoscaling_state(
        self,
    ) -> Optional[AutoscalingState]:
        """
        Update the autoscaling state of the cluster by reconciling the current
        state of the cluster resources, the cloud providers as well as instance
        update subscribers with the desired state.

        Returns:
            AutoscalingState: The new autoscaling state of the cluster.
        """

        try:
            ray_stop_errors = []
            while not self._ray_stop_errors_queue.empty():
                ray_stop_errors.append(self._ray_stop_errors_queue.get())

            ray_install_errors = []
            while not self._ray_install_errors_queue.empty():
                ray_install_errors.append(self._ray_install_errors_queue.get())

            ray_cluster_resource_state = self._get_cluster_resource_state()

            return Reconciler.reconcile(
                instance_manager=self._instance_manager,
                scheduler=self._scheduler,
                cloud_provider=self._cloud_provider,
                ray_cluster_resource_state=ray_cluster_resource_state,
                non_terminated_cloud_instances=(
                    self._cloud_provider.get_non_terminated()
                ),
                cloud_provider_errors=self._cloud_provider.poll_errors(),
                ray_install_errors=ray_install_errors,
                ray_stop_errors=ray_stop_errors,
                autoscaling_config=self._config_reader.get_autoscaling_config(),
                metrics_reporter=self._metrics_reporter,
            )
        except Exception as e:
            logger.exception(e)
            return None
