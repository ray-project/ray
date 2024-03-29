import os
from dataclasses import dataclass
from typing import Optional, Set

from horovod.ray.runner import Coordinator
from horovod.ray.utils import detect_nics, nics_to_env_var
from horovod.runner.common.util import secret, timeout

import ray
from ray.train._internal.utils import update_env_vars
from ray.train._internal.worker_group import Worker, WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.util import PublicAPI


@PublicAPI(stability="beta")
@dataclass
class HorovodConfig(BackendConfig):
    """Configurations for Horovod setup.

    See https://github.com/horovod/horovod/blob/master/horovod/runner/common/util/settings.py # noqa: E501

    Args:
        nics (Optional[Set[str]): Network interfaces that can be used for
            communication.
        verbose: Horovod logging verbosity.
        key (Optional[str]): Secret used for communication between workers.
        ssh_port (Optional[int]): Port for SSH server running on worker nodes.
        ssh_identity_file (Optional[str]): Path to the identity file to
            ssh into different hosts on the cluster.
        ssh_str (Optional[str]): CAUTION WHEN USING THIS. Private key
            file contents. Writes the private key to ssh_identity_file.
        timeout_s: Timeout parameter for Gloo rendezvous.
        placement_group_timeout_s: Timeout parameter for Ray
            Placement Group creation. Currently unused.
    """

    nics: Optional[Set[str]] = None
    verbose: int = 1
    key: Optional[str] = None
    ssh_port: Optional[int] = None
    ssh_identity_file: Optional[str] = None
    ssh_str: Optional[str] = None
    timeout_s: int = 300
    placement_group_timeout_s: int = 100

    @property
    def start_timeout(self):
        return timeout.Timeout(
            self.timeout_s,
            message="Timed out waiting for {activity}. Please "
            "check connectivity between servers. You "
            "may need to increase the --start-timeout "
            "parameter if you have too many servers.",
        )

    def __post_init__(self):
        if self.ssh_str and not os.path.exists(self.ssh_identity_file):
            with open(self.ssh_identity_file, "w") as f:
                os.chmod(self.ssh_identity_file, 0o600)
                f.write(self.ssh_str)

        if self.key is None:
            self.key = secret.make_secret_key()

    @property
    def backend_cls(self):
        return _HorovodBackend


class _HorovodBackend(Backend):
    share_cuda_visible_devices: bool = True

    def on_start(self, worker_group: WorkerGroup, backend_config: HorovodConfig):
        # TODO(matt): Implement placement group strategies in BackendExecutor.

        # Initialize workers with Horovod environment variables
        setup_futures = []
        for rank in range(len(worker_group)):
            worker_node_id = worker_group.workers[rank].metadata.node_id
            setup_futures.append(
                worker_group.execute_single_async(
                    rank,
                    _init_env_vars,
                    rank,
                    len(worker_group),
                    worker_node_id,
                )
            )
        ray.get(setup_futures)

        # Use Horovod Ray Coordinator
        # backend_config as settings
        self.coordinator = Coordinator(backend_config)

        # Get all the hostnames of all workers
        node_ids = [w.metadata.node_id for w in worker_group.workers]
        hostnames = [w.metadata.hostname for w in worker_group.workers]
        # Register each hostname to the coordinator. assumes the hostname
        # ordering is the same.
        for rank, (hostname, node_id) in enumerate(zip(hostnames, node_ids)):
            self.coordinator.register(hostname, node_id, rank)
        all_info = self.coordinator.finalize_registration()

        setup_futures = []
        for rank, local_cross_env_var in all_info.items():
            setup_futures.append(
                worker_group.execute_single_async(
                    rank, update_env_vars, local_cross_env_var
                )
            )
        ray.get(setup_futures)

        coordinator_envs = self.coordinator.establish_rendezvous()

        # Get one worker from each host/node.
        node_worker_indexes = [node_ids.index(node_id) for node_id in set(node_ids)]
        node_workers = [
            _HorovodWorkerWrapper(worker_group.workers[worker_index])
            for worker_index in node_worker_indexes
        ]
        assert len(node_workers) == len(self.coordinator.hostnames)

        nics = detect_nics(
            backend_config,
            all_host_names=list(self.coordinator.hostnames),
            node_workers=node_workers,
        )
        coordinator_envs.update(nics_to_env_var(nics))

        worker_group.execute(update_env_vars, coordinator_envs)


def _init_env_vars(world_rank: int, world_size: int, node_id: str):
    """Initialize Horovod environment variables."""
    os.environ["HOROVOD_HOSTNAME"] = node_id
    os.environ["HOROVOD_RANK"] = str(world_rank)
    os.environ["HOROVOD_SIZE"] = str(world_size)


# TODO(tgaddair): temporary workaround for Horovod's worker discovery logic,
#  which requires passing in an extra parameter as part of the RayExecutor
#  API. This will be removed in the future as we migrate more of the
#  RayExecutor utils into Ray Train.
#  See: https://github.com/horovod/horovod/blob/v0.23.0/horovod/ray/driver_service.py#L9 # noqa: E501
@dataclass
class _HorovodWorkerWrapper:
    w: Worker

    @property
    def execute(self):
        w = self.w

        class ExecuteHandle:
            def remote(self, func, *args, **kwargs):
                _ = None
                return w.actor._RayTrainWorker__execute.remote(func, _, *args, **kwargs)

        return ExecuteHandle()
