from collections import defaultdict
from dataclasses import dataclass
import os
from typing import List, Dict
import logging

import ray
from ray import tune
from ray.tune.resources import Resources
from ray.tune.trainable import TrainableUtil
from ray.tune.result import RESULT_DUPLICATE
from ray.tune.logger import NoopLogger

from ray.tune.function_runner import wrap_function

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


def map_blocking(fn, collection):
    return ray.get([fn(w) for w in collection])


def logger_creator(log_config, logdir, index):
    worker_dir = os.path.join(logdir, "worker_{}".format(index))
    os.makedirs(worker_dir, exist_ok=True)
    return NoopLogger(log_config, worker_dir)


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


class HorovodMixin:
    def hostname(self) -> str:
        # This is probably not the right way to retrieve
        # the intended hostname.
        return ray.services.get_node_ip_address()

    def update_env_vars(self, env_vars: Dict[str, str]):
        """Update the env vars in the actor process."""
        sanitized = {k: str(v) for k, v in env_vars.items()}
        os.environ.update(sanitized)


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

    def create_workers(self, trainable: type, config: dict, logdir: str):
        """Colocates a number of workers."""
        # Create a node ip resource label so that we can pin
        # all of the child actors to the same node. This ensures
        # colocation and balanced training.
        node_id = f"node:{ray.services.get_node_ip_address()}"
        remote_cls = ray.remote(trainable)
        remote_cls = remote_cls.options(
            num_cpus=0, num_gpus=0, resources={node_id: 0.01})

        # Create Tune trainable actors.
        self.workers = [
            remote_cls.remote(
                config=config,
                logger_creator=lambda cfg: logger_creator(cfg, logdir, rank))
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


class _HorovodTrainable(tune.Trainable):
    """Abstract Trainable class for Horovod."""
    _num_nodes: int = None
    _num_workers_per_node: int = None
    _num_cpus_per_worker: int = None
    _use_gpu: bool = False
    _finished: bool = False
    workers: List = None

    @property
    def num_workers(self):
        return self._num_nodes * self._num_workers_per_node

    @property
    def settings(self):
        return MiniSettings()

    @classmethod
    def get_node_resources(self):
        """Get resources per node.

        We implement a node grouping because it seems like
        our implementation doesn't quite work for imbalanced nodes.
        Also, colocation performance is typically much better than
        non-colocated workers.
        """
        num_cpus = self._num_cpus_per_worker * self._num_workers_per_node
        num_gpus = self._num_workers_per_node * int(self._use_gpu)
        return dict(num_cpus=num_cpus, num_gpus=num_gpus)

    def setup(self, config):
        self.coordinator = Coordinator(self.settings)
        colocator_cls = NodeColocator.options(**self.get_node_resources())
        # Create a number of coordinators.
        colocators = [
            colocator_cls.remote(self._num_workers_per_node, self._use_gpu)
            for i in range(self._num_nodes)
        ]
        # We must save a pointer to each colocator to prevent their resource
        # allocation from being released, along with their children from
        # going out of scope.
        self.colocators = colocators

        trainable = wrap_function(self.__class__._function)

        node_ids = map_blocking(
            lambda a: a.create_workers.remote(trainable, config, self.logdir),
            colocators)
        assert len(set(node_ids)) == len(node_ids), (
            "Colocator actors must "
            f"be placed on unique nodes! Got: {node_ids}")

        # Obtain handles to the workers
        workers = map_blocking(lambda w: w.get_workers.remote(), colocators)
        self.workers = sum(workers, [])

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

    def step(self):
        if self._finished:
            raise RuntimeError("Training has already finished.")
        result = map_blocking(lambda w: w.step.remote(), self.workers)[0]
        if RESULT_DUPLICATE in result:
            self._finished = True
        return result

    def save_checkpoint(self, checkpoint_dir):
        # TODO: optimize if colocated
        save_obj = ray.get(self.workers[0].save_to_object.remote())
        checkpoint_path = TrainableUtil.create_from_pickle(
            save_obj, checkpoint_dir)
        return checkpoint_path

    def load_checkpoint(self, checkpoint_dir):
        checkpoint_obj = TrainableUtil.checkpoint_to_object(checkpoint_dir)
        return map_blocking(
            lambda w: w.restore_from_object.remote(checkpoint_obj),
            self.workers)

    def stop(self):
        map_blocking(lambda w: w.stop.remote(), self.workers)


def DistributedTrainableCreator(func,
                                use_gpu=False,
                                num_nodes=1,
                                num_workers_per_node=1,
                                num_cpus_per_worker=1,
                                timeout_s=30,
                                replicate_pem=False):
    """Horovod Tune Converter.

    Uses gloo as the underlying communication primitive.
    Fault tolerance is handled at the Tune level and is disregarded
    at the underlying trial level.

    Args:
        use_gpu (bool); Whether to allocate a GPU per worker.
        num_cpus_per_worker (int): Number of CPUs to request
            from Ray per worker.
        num_nodes (int): Number of nodes that each trial is expected
            to use.
        num_workers_per_node (int): Number of workers to start on each node.
        timeout_s (int): Seconds for Horovod rendezvous to timeout.
        replicate_pem (bool): Whether to replicate the underlying Ray
            cluster ssh key across all nodes. This may be insecure.


    Returns:
        Trainable class that can be passed into `tune.run`.

    Example:

    .. code-block::

        def train(config):
            horovod.init()
            horovod.allreduce()

        from ray.tune.integration.horovod import DistributedTrainableCreator
        trainable_cls = DistributedTrainableCreator(
            train, num_nodes=1, num_workers_per_node=2, use_gpu=True)

        tune.run(trainable_cls)


    Notes:
        This currently does not support function checkpointing.
    """
    func.__mixins__ = (HorovodMixin, )
    ssh_identity_file = None
    sshkeystr = None

    if replicate_pem:
        from ray.tune.cluster_info import get_ssh_key
        ssh_identity_file = get_ssh_key()
        if os.path.exists(ssh_identity_file):
            # For now, we assume that you're on a Ray cluster.
            with open(ssh_identity_file) as f:
                sshkeystr = f.read()

    class WrappedDistributedTorchTrainable(_HorovodTrainable):
        _function = func
        _num_nodes = num_nodes
        _num_workers_per_node = num_workers_per_node
        _num_cpus_per_worker = num_cpus_per_worker
        _use_gpu = use_gpu
        _ssh_identity_file = ssh_identity_file
        _ssh_str = sshkeystr

        @property
        def settings(self):
            from filelock import FileLock
            # Very hacky - maybe instead want to recommend
            # an autoscaler mechanism for doing this.
            if replicate_pem:
                with FileLock(self._ssh_identity_file + ".lock"):
                    if not os.path.exists(self._ssh_key_path):
                        with open(self._ssh_key_path, "w") as f:
                            os.chmod(self._ssh_key_path, 0o600)
                            f.write(self._ssh_str)

            return MiniSettings(
                timeout_s=timeout_s, ssh_identity_file=self._ssh_identity_file)

        @classmethod
        def default_resource_request(cls, config):
            extra_gpu = int(num_nodes * num_workers_per_node) * int(use_gpu)
            extra_cpu = int(
                num_nodes * num_workers_per_node * num_cpus_per_worker)

            return Resources(
                cpu=0,
                gpu=0,
                extra_cpu=extra_cpu,
                extra_gpu=extra_gpu,
            )

    return WrappedDistributedTorchTrainable


def _train_simple(config):
    import horovod.torch as hvd
    hvd.init()
    from ray import tune
    for i in range(config.get("epochs", 2)):
        import time
        time.sleep(1)
        tune.report(test=1, rank=hvd.rank())
