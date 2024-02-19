from queue import Queue
from typing import List, Optional
from ray.autoscaler._private.providers import _get_node_provider
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig, IConfigReader
from ray._raylet import GcsClient
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceManager,
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import (
    ICloudInstanceProvider,
    NodeProviderAdapter,
)
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import RayStopper
from ray.autoscaler.v2.scheduler import ResourceDemandScheduler

from ray.core.generated.autoscaler_pb2 import (
    AutoscalingState,
    ClusterResourceState,
    ReportAutoscalingStateReply,
    ReportAutoscalingStateRequest,
    GetClusterResourceStateReply,
)
import logging

logger = logging.getLogger(__name__)


class Autoscaler:
    def __init__(
        self,
        session_name: str,
        config_reader: IConfigReader,
        gcs_client: GcsClient,
    ) -> None:
        super().__init__()

        self._config_reader = config_reader
        config = config_reader.get_autoscaling_config()
        print(config.dump())
        self._gcs_client = gcs_client

        self._cloud_provider = self._init_cloud_provider(config_reader)
        self._instance_manager = self._init_instance_manager(
            session_name=session_name,
            config=config,
            cloud_provider=self._cloud_provider,
            gcs_client=gcs_client,
        )
        self._scheduler = ResourceDemandScheduler()

    def initialize(self):
        Reconciler.initialize_head_node(
            instance_manager=self._instance_manager,
            non_terminated_cloud_instances=self._cloud_provider.get_non_terminated(),
        )

    @staticmethod
    def _init_cloud_provider(config_reader: IConfigReader) -> ICloudInstanceProvider:
        config = config_reader.get_autoscaling_config()
        node_provider_v1 = _get_node_provider(
            config.get_provider_config(),
            config.get_config("cluster_name"),
        )

        return NodeProviderAdapter(
            v1_provider=node_provider_v1,
            config_reader=config_reader,
        )

    @staticmethod
    def _init_instance_manager(
        session_name: str,
        config: AutoscalingConfig,
        cloud_provider: ICloudInstanceProvider,
        gcs_client: GcsClient,
    ) -> InstanceManager:
        instance_storage = InstanceStorage(
            cluster_id=session_name,
            storage=InMemoryStorage(),
        )

        subscribers: List[InstanceUpdatedSubscriber] = []
        subscribers.append(CloudInstanceUpdater(cloud_provider=cloud_provider))
        subscribers.append(RayStopper(gcs_client=gcs_client, error_queue=Queue()))
        if not config.skip_ray_install():
            raise NotImplementedError("RayInstaller is not implemented yet.")

        return InstanceManager(
            instance_storage=instance_storage,
            instance_status_update_subscribers=subscribers,
        )

    def get_cluster_resource_state(self) -> ClusterResourceState:
        str_reply = self._gcs_client.get_cluster_resource_state()
        reply = GetClusterResourceStateReply()
        reply.ParseFromString(str_reply)

        return reply.cluster_resource_state

    def update_autoscaling_state(
        self, ray_cluster_resource_state: ClusterResourceState
    ) -> Optional[AutoscalingState]:

        try:
            return Reconciler.reconcile(
                instance_manager=self._instance_manager,
                scheduler=self._scheduler,
                cloud_provider=self._cloud_provider,
                ray_cluster_resource_state=ray_cluster_resource_state,
                non_terminated_cloud_instances=self._cloud_provider.get_non_terminated(),
                cloud_provider_errors=self._cloud_provider.poll_errors(),
                # TODO
                ray_install_errors=[],
                autoscaling_config=self._config_reader.get_autoscaling_config(),
            )
        except Exception as e:
            logger.exception(e)
            return None
