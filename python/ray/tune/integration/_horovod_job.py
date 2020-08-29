
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



class HorovodMixin:
    def hostname(self) -> str:
        # This is probably not the right way to retrieve
        # the intended hostname.
        return ray.services.get_node_ip_address()

    def update_env_vars(self, env_vars: Dict[str, str]):
        """Update the env vars in the actor process."""
        sanitized = {k: str(v) for k, v in env_vars.items()}
        os.environ.update(sanitized)

    def env_vars(self):
        """Check the env vars in the actor process."""
        return dict(os.environ)


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
    @classmethod
    def create_settings(cls, timeout_s, ssh_identity_file, ssh_str):
        from filelock import FileLock
        # Very hacky - maybe instead want to recommend
        # an autoscaler mechanism for doing this.
        if ssh_str:
            with FileLock(ssh_identity_file + ".lock"):
                if not os.path.exists(ssh_key_path):
                    with open(ssh_key_path, "w") as f:
                        os.chmod(ssh_key_path, 0o600)
                        f.write(ssh_str)

        return MiniSettings(
            timeout_s=timeout_s, ssh_identity_file=ssh_identity_file)

    def __init__(self, settings, num_hosts, num_slots, use_gpu):
        self.settings = settings
        self.num_hosts = num_hosts
        self.num_slots = num_slots
        self.use_gpu = use_gpu  # can remove if resources has gpu already
        self.worker_cls = worker_cls

    def _create_workers(self, node_resources, worker_cls, worker_init_args):
        colocator_cls = NodeColocator.options(**self.node_resources)
        # Create a number of coordinators.
        colocators = [
            colocator_cls.remote(self.num_slots, self._use_gpu)
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

    def start(self, node_resources, worker_cls, worker_init_args=None):

        class MixedHorovodWorker(worker_cls, HorovodMixin):
            pass

        self.coordinator = Coordinator(self.settings)
        worker_init_args = worker_init_args or []
        self.workers = self._create_workers(MixedWorkerClass, worker_init_args)

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

    def execute(self, fn, blocking=True):
        if blocking:
            return map_blocking(fn, self.workers)
        else:
            return [fn(worker) for worker in self.workers]

    def execute_chief(self, fn):
        return ray.get(fn(self.workers[0]))

    def shutdown(self):
        for colocator in self.colocators:
            del colocator

        for worker in self.workers:
            del worker