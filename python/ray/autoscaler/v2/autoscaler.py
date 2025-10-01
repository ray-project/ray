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
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstaller
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import RayStopper
from ray.autoscaler.v2.instance_manager.subscribers.threaded_ray_installer import (
    ThreadedRayInstaller,
)
from ray.autoscaler.v2.metrics_reporter import AutoscalerMetricsReporter
from ray.autoscaler.v2.scheduler import ResourceDemandScheduler
from ray.autoscaler.v2.sdk import get_cluster_resource_state
from ray.core.generated.autoscaler_pb2 import AutoscalingState
from urllib.parse import urlsplit

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

        self._instance_manager = InstanceManager(
            instance_storage=instance_storage,
            instance_status_update_subscribers=subscribers,
        )

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
