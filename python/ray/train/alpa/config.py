from dataclasses import dataclass
import logging
from typing import TypeVar

import ray
from ray.train.backend import BackendConfig, Backend
from ray.train._internal.utils import get_address_and_port
from ray.train._internal.worker_group import WorkerGroup
from ray.util import PublicAPI

import alpa
from alpa.util import update_jax_platform
from alpa.device_mesh import VirtualPhysicalMesh

T = TypeVar("T")

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
@dataclass
class AlpaConfig(BackendConfig):
    """Configuration for Alpa process group setup.

    See https://alpa-projects.github.io/ for more info.
    """

    @property
    def backend_cls(self):
        return _AlpaBackend


class _AlpaBackend(Backend):
    share_cuda_visible_devices: bool = True

    def on_start(self, worker_group: WorkerGroup, backend_config: AlpaConfig):
        # Compute URL for initializing distributed setup.
        head_ip, _ = worker_group.execute_single(0, get_address_and_port)

        # Gather host ids
        host_info = []
        host_ips = []
        for node in ray.nodes():
            for key in node["Resources"]:
                if key.startswith("node:"):
                    host_ips.append(key.split("node:")[-1])
                    host_info.append(node)

        num_devices_per_host = worker_group.num_gpus_per_worker
        node_ids = list(range(len(worker_group)))

        # filter by workergourp
        node_ips = [w.metadata.node_ip for w in worker_group.workers]
        host_ip_2_host_info_dict = dict(zip(host_ips, host_info))
        node_info = [host_ip_2_host_info_dict[node_ip] for node_ip in node_ips]

        self.vp_mesh = VirtualPhysicalMesh(
            host_ids=node_ids,
            host_info=node_info,
            head_ip=head_ip,
            num_devices_per_host=num_devices_per_host,
            parent=None,
        )

        alpa.device_mesh.set_global_virtual_physical_mesh(self.vp_mesh)

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: AlpaConfig):
        alpa.device_mesh.global_cluster = None
        update_jax_platform("gpu")

        if self.vp_mesh:
            if self.vp_mesh.launched_physical_mesh_group:
                self.vp_mesh.launched_physical_mesh_group.shutdown()
            alpa.device_mesh.global_virtual_physical_mesh = None
