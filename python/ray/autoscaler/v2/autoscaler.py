import logging
from queue import Queue
from typing import List, Optional

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
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import RayStopper
from ray.autoscaler.v2.metrics_reporter import AutoscalerMetricsReporter
from ray.autoscaler.v2.scheduler import ResourceDemandScheduler
from ray.autoscaler.v2.sdk import get_cluster_resource_state
from ray.core.generated.autoscaler_pb2 import AutoscalingState

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
        self._init_instance_manager(
            session_name=session_name,
            config=config,
            cloud_provider=self._cloud_instance_provider,
            gcs_client=self._gcs_client,
        )
        self._scheduler = ResourceDemandScheduler(self._event_logger)

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
        subscribers.append(CloudInstanceUpdater(cloud_provider=cloud_provider))
        subscribers.append(
            RayStopper(gcs_client=gcs_client, error_queue=self._ray_stop_errors_queue)
        )
        if not config.disable_node_updaters():
            # Supporting ray installer is only needed for providers that doesn't
            # install or manage ray (e.g. AWS, GCP). These providers will be
            # supported in the future.
            raise NotImplementedError(
                "RayInstaller is not supported yet in current "
                "release of the Autoscaler V2. Therefore, providers "
                "that update nodes (with `disable_node_updaters` set to True) "
                "are not supported yet. Only KubeRay is supported for now which sets "
                "disable_node_updaters to True in provider's config."
            )

        self._instance_manager = InstanceManager(
            instance_storage=instance_storage,
            instance_status_update_subscribers=subscribers,
        )

    def update_autoscaling_state(
        self,
    ) -> Optional[AutoscalingState]:
        """
        Update the autoscaling state of the cluster by reconciling the current
        state of the cluster resources, the cloud providers as well as instance
        update subscribers with the desired state.

        Returns:
            AutoscalingState: The new autoscaling state of the cluster or None if
            the state is not updated.

        Raises:
            No exception.
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

            return Reconciler.reconcile(
                instance_manager=self._instance_manager,
                scheduler=self._scheduler,
                cloud_provider=self._cloud_instance_provider,
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
        except Exception as e:
            logger.exception(e)
            return None
