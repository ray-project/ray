import logging
import os
import socket
from dataclasses import dataclass
from typing import Dict

import ray
from horovod.ray.runner import Coordinator
from horovod.ray.utils import detect_nics, nics_to_env_var
from ray.util.sgd.v2.backends.backend import BackendConfig, BackendInterface
from ray.util.sgd.v2.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


@dataclass
class HorovodConfig(BackendConfig):
    """Configurations for Horovod setup.

    See https://github.com/horovod/horovod/blob/master/horovod/runner/common/util/settings.py # noqa: E501

    Args:
        nics (int): Network interfaces that can be used for communication.
        verbose (int): Horovod logging verbosity.
    """
    # timeout_s: int = 300
    nics: set = None
    verbose: int = 1

    @property
    def backend_cls(self):
        return HorovodBackend


def get_node_id() -> str:
    return ray.get_runtime_context().node_id.hex()


def get_hostname() -> str:
    # TODO: This is probably not the right way to retrieve
    # the intended hostname.
    return socket.gethostname()


def update_env_vars(env_vars: Dict[str, str]):
    """Update the env vars in the actor process."""
    sanitized = {k: str(v) for k, v in env_vars.items()}
    os.environ.update(sanitized)


def init_env_vars(world_rank: int, world_size: int):
    """Initialize Horovod environment variables."""
    os.environ["HOROVOD_HOSTNAME"] = get_node_id()
    os.environ["HOROVOD_RANK"] = str(world_rank)
    os.environ["HOROVOD_SIZE"] = str(world_size)


class HorovodBackend(BackendInterface):
    def on_start(self, worker_group: WorkerGroup,
                 backend_config: HorovodConfig):

        # TODO(matt): Implement placement group strategies.

        # Initialize workers with Horovod environment variables
        setup_futures = []
        for rank in range(len(worker_group)):
            setup_futures.append(
                worker_group.execute_single_async(rank, init_env_vars, rank,
                                                  len(worker_group)))
        ray.get(setup_futures)

        # Use Horovod Ray Coordinator
        # backend_config as settings
        self.coordinator = Coordinator(backend_config)

        # Get all the hostnames of all workers
        node_ids = worker_group.execute(get_node_id)
        hostnames = worker_group.execute(get_hostname)
        # Register each hostname to the coordinator. assumes the hostname
        # ordering is the same.
        for rank, (hostname, node_id) in enumerate(zip(hostnames, node_ids)):
            self.coordinator.register(hostname, node_id, rank)
        all_info = self.coordinator.finalize_registration()

        setup_futures = []
        for rank, local_cross_env_var in all_info.items():
            setup_futures.append(
                worker_group.execute_single_async(rank, update_env_vars,
                                                  local_cross_env_var))
        ray.get(setup_futures)

        coordinator_envs = self.coordinator.establish_rendezvous()

        nics = detect_nics(
            backend_config,
            all_host_names=list(self.coordinator.hostnames),
            node_workers=worker_group.workers)
        coordinator_envs.update(nics_to_env_var(nics))

        worker_group.execute(update_env_vars, coordinator_envs)

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: HorovodConfig):
        # Currently no additional steps are needed
        pass
