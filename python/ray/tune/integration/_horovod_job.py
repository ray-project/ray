from collections import defaultdict
from dataclasses import dataclass
import os
from typing import Dict, Callable, Any, Optional, List
import logging

import ray

Settings = None
secret = None
timeout = None
hosts = None
driver_service = None
RendezvousServer = None

logger = logging.getLogger(__name__)


def is_horovod_available():
    try:
        import horovod  # noqa: F401
        return True
    except ImportError:
        return False


if is_horovod_available():
    from horovod.runner.common.util import Settings, secret, timeout, hosts
    from horovod.runner.driver import driver_service
    from horovod.runner.http.http_server import RendezvousServer
else:
    logger.info("Run `HOROVOD_WITH_GLOO=1 HOROVOD_WITHOUT_MPI=1 "
                "pip install horovod` to try out Horovod with Ray.")


@dataclass
class MiniSettings:
    nics: set = None
    verbose: int = 1
    key = secret.make_secret_key() if secret else None
    ssh_port = None
    ssh_identity_file = None
    timeout_s: int = 300

    @property
    def start_timeout(self):
        return timeout.Timeout(
            self.timeout_s,
            message="Timed out waiting for {activity}. Please "
            "check connectivity between servers. You "
            "may need to increase the --start-timeout "
            "parameter if you have too many servers.")


def map_blocking(fn, collection):
    return ray.get([fn(w) for w in collection])


class BaseHorovodWorker:
    def hostname(self) -> str:
        # TODO: This is probably not the right way to retrieve
        # the intended hostname.
        return ray.services.get_node_ip_address()

    def update_env_vars(self, env_vars: Dict[str, str]):
        """Update the env vars in the actor process."""
        sanitized = {k: str(v) for k, v in env_vars.items()}
        os.environ.update(sanitized)

    def env_vars(self):
        """Check the env vars in the actor process."""
        return dict(os.environ)

    def execute(self, func):
        """Executes an arbitrary function on self."""
        return func(self)


@ray.remote
class NodeColocator:
    """Responsible for colocation of child actors.

    These actors are given resources equal to the sum of their children.
    This is a mechanism for gang-scheduling and could be replaced
    later on with placement groups. Gang-scheduling must occur because
    otherwise another concurrent group could be placed on this node.

    Right now, the only resources that are explicitly propogated to
    underlying colocated workers are cuda visible devices.
    """

    def __init__(self, num_workers: int, use_gpu: bool):
        self.num_workers = num_workers
        if use_gpu:
            gpu_ids = ray.get_gpu_ids(as_str=True)
            assert len(gpu_ids) == num_workers, gpu_ids
        self.workers = []

    def create_workers(self, worker_cls: type, worker_init_args: list):
        """Colocates a number of workers."""
        # Create a node ip resource label so that we can pin
        # all of the child actors to the same node. This ensures
        # colocation and balanced training.
        node_id = f"node:{ray.services.get_node_ip_address()}"
        remote_cls = ray.remote(worker_cls)
        remote_cls = remote_cls.options(
            num_cpus=0, num_gpus=0, resources={node_id: 0.01})

        self.workers = [
            remote_cls.remote(**worker_init_args)
            for rank in range(self.num_workers)
        ]

        # Propogate cuda visible devices to the underlying
        # colocated workers.
        gpu_ids = ray.get_gpu_ids(as_str=True)
        for worker, gpu_id in zip(self.workers, gpu_ids):
            worker.update_env_vars.remote({"CUDA_VISIBLE_DEVICES": gpu_id})
        return node_id

    def get_workers(self) -> List:
        return self.workers


class Coordinator:
    rendezvous = None
    global_rendezv_port = None
    nics = None
    hostnames = None

    def __init__(
            self,
            settings: Settings,
    ):
        self.settings = settings
        self.hostnames_by_rank = defaultdict(list)

    @property
    def world_size(self) -> int:
        return sum(len(ranks) for ranks in self.hostnames_by_rank.values())

    @property
    def hoststring(self) -> str:
        return ",".join([
            f"{host}:{len(ranks)}"
            for host, ranks in self.hostnames_by_rank.items()
        ])

    def register(self, hostname: str, world_rank: int):
        self.hostnames_by_rank[hostname].append(world_rank)

    def finalize_registration(self) -> dict:
        """Return a dictionary for all ranks."""
        rank_to_info = {}
        for node_world_rank, (hostname, ranks) in enumerate(
                self.hostnames_by_rank.items()):
            for local_rank, world_rank in enumerate(ranks):
                rank_to_info[world_rank] = dict(
                    node_world_rank=node_world_rank,
                    node_world_size=len(self.hostnames_by_rank),
                    local_rank=local_rank,
                    local_size=len(ranks))
        return rank_to_info

    def establish_rendezvous(self) -> Dict[str, str]:
        """Creates the rendezvous server and identifies the nics to be used.

        Returns:
            Environment variables for each worker. """

        # start global rendezvous server and get port that it is listening on
        self.rendezvous = RendezvousServer(self.settings.verbose)

        # allocate processes into slots
        # hosts = parse_hosts(hosts_string="10.11.11.11:4,10.11.11.12:4")
        parsed_hosts = hosts.parse_hosts(hosts_string=self.hoststring)
        host_alloc_plan = hosts.get_host_assignments(parsed_hosts,
                                                     self.world_size)

        # start global rendezvous server and get port that it is listening on
        self.global_rendezv_port = self.rendezvous.start()
        self.rendezvous.init(host_alloc_plan)
        # remote_host_names = network.filter_local_addresses()
        self.nics = driver_service.get_common_interfaces(
            self.settings, list(self.hostnames_by_rank))

        return {
            # "HOROVOD_LOG_LEVEL": "DEBUG",
            "HOROVOD_GLOO_RENDEZVOUS_ADDR": ray.services.get_node_ip_address(),
            "HOROVOD_GLOO_RENDEZVOUS_PORT": str(self.global_rendezv_port),
            "HOROVOD_CONTROLLER": "gloo",
            "HOROVOD_CPU_OPERATIONS": "gloo",
            "HOROVOD_GLOO_IFACE": str(list(self.nics)[0]),  # TODO
            "NCCL_SOCKET_IFNAME": ",".join(self.nics),  # TDOO
        }


class HorovodJob:
    """Job class for Horovod + Ray integration.

    Args:
        settings (horovod.Settings): Configuration for job setup. You can
            use a standard Horovod Settings object or create one directly
            from HorovodJob.create_settings.
        num_hosts (int): Number of machines to execute the job on.
        num_slots (int): Humber of workers to be placed on each machine.
        use_gpu (bool): Whether to use GPU for allocation. TODO: this
            can be removed.
    """

    @classmethod
    def create_settings(cls, timeout_s, ssh_identity_file=None, ssh_str=None):
        """Create a mini setting object.

        Args:
            timeout_s (int): Tiemout parameter for Gloo rendezvous.
            ssh_identity_file (str): Path to the identity file to
                ssh into different hosts on the cluster.
            ssh_str (str): CAUTION WHEN USING THIS. Private key
                file contents. Writes the private key to ssh_identity_file.

        Returns:
            MiniSettings object.
        """
        # Very hacky - maybe instead want to recommend
        # an autoscaler mechanism for doing this.
        if ssh_str and not os.path.exists(ssh_identity_file):
            with open(ssh_identity_file, "w") as f:
                os.chmod(ssh_identity_file, 0o600)
                f.write(ssh_str)

        return MiniSettings(
            timeout_s=timeout_s, ssh_identity_file=ssh_identity_file)

    def __init__(self, settings, num_hosts: int = 1, num_slots: int = 1):
        self.settings = settings
        self.num_hosts = num_hosts
        self.num_slots = num_slots

    def _create_workers(self, host_resources, worker_cls, worker_init_args):
        colocator_cls = NodeColocator.options(**host_resources)
        # Create a number of coordinators.
        colocators = [
            colocator_cls.remote(
                self.num_slots, use_gpu=host_resources["num_gpus"] > 0)
            for i in range(self.num_hosts)
        ]
        # We must save a pointer to each colocator to prevent their resource
        # allocation from being released, along with their children from
        # going out of scope.
        self.colocators = colocators

        node_ids = map_blocking(
            lambda a: a.create_workers.remote(worker_cls, *worker_init_args),
            colocators)
        assert len(set(node_ids)) == len(node_ids), (
            "Colocator actors must "
            f"be placed on unique nodes! Got: {node_ids}")

        # Obtain handles to the workers
        workers = map_blocking(lambda w: w.get_workers.remote(), colocators)
        return sum(workers, [])

    def _mixin_horovod_worker(self, worker_cls=None):
        """Mixes in BaseHorovodWorker if not a current subclass."""
        worker_cls = worker_cls or BaseHorovodWorker
        if issubclass(worker_cls, BaseHorovodWorker):
            return worker_cls

        class MixedHorovodWorker(worker_cls, BaseHorovodWorker):
            pass

        return MixedHorovodWorker

    def start(self,
              worker_cls: type = None,
              worker_init_args: Optional[List] = None,
              cpus_per_worker: int = 1,
              use_gpu: bool = False):
        """Starts the workers and colocates them on all machines.

            We implement a node grouping because it seems like
            our implementation doesn't quite work for imbalanced nodes.
            Also, colocation performance is typically much better than
            non-colocated workers.
        Args:
            worker_cls (type): The class that will be created as an actor. This
                will be automatically mixed in with BaseHorovodWorker
                to allow Horovod to establish its connections and set env
                vars.
            worker_init_args (List): Arguments to be passed into the
                worker class upon initialization.
            cpus_per_worker (int): Number of CPU resources to allocate to
                each worker.
            use_gpu (bool): Whether to allocate GPU resources or not.

        """

        def resources_per_host():
            num_cpus = cpus_per_worker * self.num_slots
            num_gpus = self.num_slots * int(use_gpu)
            return dict(num_cpus=num_cpus, num_gpus=num_gpus)

        worker_cls = self._mixin_horovod_worker(worker_cls)

        self.coordinator = Coordinator(self.settings)
        worker_init_args = worker_init_args or []
        self.workers = self._create_workers(
            resources_per_host(),
            worker_cls=worker_cls,
            worker_init_args=worker_init_args)

        # Update the environment variables.
        ray.get([
            w.update_env_vars.remote(
                dict(world_rank=i, world_size=self.num_workers))
            for i, w in enumerate(self.workers)
        ])
        # Get all the hostnames of all workers
        hostnames = map_blocking(lambda w: w.hostname.remote(), self.workers)
        # Register each hostname to the coordinator. assumes the hostname
        # ordering is the same.
        for rank, hostname in enumerate(hostnames):
            self.coordinator.register(hostname, rank)
        all_info = self.coordinator.finalize_registration()

        indexed_runners = dict(enumerate(self.workers))
        for rank, local_cross_env_var in all_info.items():
            indexed_runners[rank].update_env_vars.remote(local_cross_env_var)

        coordinator_envs = self.coordinator.establish_rendezvous()

        map_blocking(lambda w: w.update_env_vars.remote(coordinator_envs),
                     self.workers)

    def run(self, fn: Callable[["worker_cls"], Any]):
        """Executes an arbitrary function on the remote workers."""
        return map_blocking(lambda w: w.execute.remote(fn), self.workers)

    def execute(self,
                fn: Callable[["ActorHandle"], List["ObjectID"]],
                blocking: bool = True) -> List["ObjectID"]:
        """Executes the provided function on all workers.

        Args:
            fn: Target function
            blocking (bool): Whether to execute and block before returning.
                Otherwise, returns a list of object IDs.

        Returns:
            List of object IDs.
        """
        result_ids = [fn(worker) for worker in self.workers]
        if blocking:
            remaining = result_ids
            while remaining:
                remaining, done = ray.wait(remaining)
        return result_ids

    def execute_single(self,
                       fn: Callable[["ActorHandle"], "ObjectID"],
                       blocking: bool = True) -> Any:
        """Executes the provided function on the rank 0 worker (chief).

        Args:
            fn: Target function
            blocking (bool): Whether to execute and block before returning.
                Otherwise, returns a list of object IDs.

        Returns:
            ObjectID of the executed function.
        """
        result_id = fn(self.workers[0])
        if blocking:
            ray.wait([result_id])
            return result_id
        else:
            return result_id

    def shutdown(self):
        """Destroys the provided workers."""
        for colocator in self.colocators:
            del colocator

        for worker in self.workers:
            del worker

        self.colocators = []
        self.workers = []


if __name__ == "__main__":
    ray.init(address="auto")
    setting = HorovodJob.create_settings(
        timeout_s=30, ssh=os.path.expanduser("~/ray_bootstrap_key.pem"))
    hjob = HorovodJob(setting, num_hosts=1, num_slots=4)
    hjob.start()
    hostnames = ray.get(hjob.execute(lambda w: w.hostname.remote()))
    assert set(hostnames) == 1
