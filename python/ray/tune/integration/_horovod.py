from collections import defaultdict
from typing import Callable, List
import os
import ray
from ray import tune
from ray.tune.resources import Resources
from ray.tune.trainable import TrainableUtil
from ray.tune.result import RESULT_DUPLICATE
from ray.tune.logger import NoopLogger

from ray.tune.function_runner import (wrap_function,
                                      detect_checkpoint_function)
from horovod.runner.common.util import settings as hvd_settings
from horovod.runner.driver import driver_service
from horovod.runner.http.http_server import RendezvousServer
from horovod.runner.common.util.hosts import get_host_assignments, parse_hosts
from horovod.runner.common.util import secret, timeout
from horovod.runner.util import network
from dataclasses import dataclass


def logger_creator(log_config, logdir, index):
    worker_dir = os.path.join(logdir, "worker_{}".format(index))
    os.makedirs(worker_dir, exist_ok=True)
    return NoopLogger(log_config, worker_dir)


@ray.remote
class PlacerActor:
    """Responsible for colocation of child actors."""

    def __init__(self, num_workers: int, use_gpu: bool):
        self.num_workers = num_workers
        if use_gpu:
            gpu_ids = ray.get_gpu_ids(as_str=True)
            assert len(gpu_ids) == num_workers, gpu_ids
        self.workers = []

    def configure(self, trainable: object, config: dict, logdir: Callable):
        node_id = f"node:{ray.services.get_node_ip_address()}"
        remote_cls = ray.remote(trainable)
        remote_cls = remote_cls.options(
            num_cpus=0, num_gpus=0, resources={node_id: 0.01})
        self.workers = [
            remote_cls.remote(
                config=config,
                logger_creator=lambda cfg: logger_creator(cfg, logdir, rank))
            for rank in range(self.num_workers)
        ]

        gpu_ids = ray.get_gpu_ids(as_str=True)
        for worker, gpu_id in zip(self.workers, gpu_ids):
            worker.update_env_vars.remote({"CUDA_VISIBLE_DEVICES": gpu_id})
        return node_id

    def get_workers(self) -> List:
        return self.workers


class _HorovodTrainable(tune.Trainable):
    _num_nodes: int = None
    _num_workers_per_node: int = None
    _num_cpus_per_worker: int = None
    _use_gpu: bool = False
    _finished: bool = False

    @property
    def num_workers(self):
        return self._num_nodes * self._num_workers_per_node

    @property
    def settings(self):
        return MiniSettings()

    @classmethod
    def get_node_options(self):
        num_cpus = self._num_cpus_per_worker * self._num_workers_per_node
        num_gpus = self._num_workers_per_node * int(self._use_gpu)
        return dict(num_cpus=num_cpus, num_gpus=num_gpus)

    def setup(self, config):
        self.coordinator = Coordinator(self.settings)
        placer_actors = [
            PlacerActor.options(**self.get_node_options()).remote(
                self._num_workers_per_node, self._use_gpu)
            for i in range(self._num_nodes)
        ]
        self.placer_actors = placer_actors

        func_trainable = wrap_function(self.__class__._function)
        node_ids = ray.get([
            actor.configure.remote(func_trainable, config, self.logdir)
            for actor in placer_actors
        ])
        assert len(set(node_ids)) == len(node_ids), node_ids

        # Create workers
        workers = ray.get(
            [actor.get_workers.remote() for actor in placer_actors])
        self.workers = sum(workers, [])
        ray.get([
            w.update_env_vars.remote(
                dict(world_rank=i, world_size=self.num_workers))
            for i, w in enumerate(self.workers)
        ])
        hostnames = ray.get([w.hostname.remote() for w in self.workers])
        for rank, hostname in enumerate(hostnames):
            self.coordinator.register(hostname, rank)
        all_info = self.coordinator.finalize_registration()
        # import yaml
        # print(yaml.dump(all_info, default_flow_style=False))

        # print("Update world info on workers")
        indexed_runners = dict(enumerate(self.workers))
        for i, local_cross_env_var in all_info.items():
            indexed_runners[i].update_env_vars.remote(local_cross_env_var)

        self.coordinator.establish_rendezvous()
        c_envs = self.coordinator.get_coordinator_envs()
        ray.get([w.update_env_vars.remote(c_envs) for w in self.workers])

    def step(self):
        if self._finished:
            raise RuntimeError("Training has already finished.")
        result = ray.get([w.step.remote() for w in self.workers])[0]
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
        return ray.get(
            w.restore_from_object.remote(checkpoint_obj) for w in self.workers)

    def stop(self):
        ray.get([worker.stop.remote() for worker in self.workers])


@dataclass
class MiniSettings:
    nics: set = None
    verbose: int = 2
    key = secret.make_secret_key()
    ssh_port = None
    ssh_identity_file = os.path.expanduser("~/ray_bootstrap_key.pem")
    timeout_s: int = 300

    @property
    def start_timeout(self):
        return timeout.Timeout(
            self.timeout_s,
            message="Timed out waiting for {activity}. Please "
            "check connectivity between servers. You "
            "may need to increase the --start-timeout "
            "parameter if you have too many servers.")


def DistributedTrainableCreator(func,
                                use_gpu=False,
                                num_nodes=1,
                                num_workers_per_node=4,
                                num_cpus_per_worker=2,
                                timeout_s=30):
    # detect_checkpoint_function(func, abort=True)
    func.__mixins__ = (HorovodMixin, )

    class WrappedDistributedTorchTrainable(_HorovodTrainable):
        _function = func
        _num_nodes = num_nodes
        _num_workers_per_node = num_workers_per_node
        _num_cpus_per_worker = num_cpus_per_worker
        _use_gpu = use_gpu

        @property
        def settings(self):
            return MiniSettings(timeout_s=timeout_s)

        @classmethod
        def default_resource_request(cls, config):
            head_ip = ray.services.get_node_ip_address()
            extra_gpu = int(num_nodes * num_workers_per_node) * int(use_gpu)
            extra_cpu = int(
                num_nodes * num_workers_per_node * num_cpus_per_worker)

            return Resources(
                cpu=0,
                gpu=0,
                extra_cpu=extra_cpu,
                extra_gpu=extra_gpu,
                custom_resources={f"node:{head_ip}": 0.01})

    return WrappedDistributedTorchTrainable


class Coordinator:
    rendezvous = None
    global_rendezv_port = None
    nics = None
    hostnames = None

    def __init__(self, settings):
        # self._registered_count = 0
        # self._world_size = world_size
        self.settings = settings
        self.hostnames_by_rank = defaultdict(list)

    @property
    def world_size(self):
        return sum(len(ranks) for ranks in self.hostnames_by_rank.values())

    @property
    def hoststring(self):
        return ",".join([
            f"{host}:{len(ranks)}"
            for host, ranks in self.hostnames_by_rank.items()
        ])

    def register(self, hostname, world_rank):
        self.hostnames_by_rank[hostname].append(world_rank)

    def finalize_registration(self) -> dict:
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

    def establish_rendezvous(self):
        # start global rendezvous server and get port that it is listening on
        self.rendezvous = RendezvousServer(self.settings.verbose)

        # allocate processes into slots
        # hosts = parse_hosts(hosts_string="10.11.11.11:4,10.11.11.12:4")
        hosts = parse_hosts(hosts_string=self.hoststring)
        host_alloc_plan = get_host_assignments(hosts, self.world_size)

        # start global rendezvous server and get port that it is listening on
        print("start rendezvous")
        self.global_rendezv_port = self.rendezvous.start()
        self.rendezvous.init(host_alloc_plan)
        # remote_host_names = network.filter_local_addresses()
        self.nics = driver_service.get_common_interfaces(
            self.settings, list(self.hostnames_by_rank))

        return self.get_coordinator_envs()

    def get_coordinator_envs(self):
        return {
            "HOROVOD_GLOO_RENDEZVOUS_ADDR": str(
                network.get_driver_ip(self.nics)),
            "HOROVOD_GLOO_RENDEZVOUS_PORT": str(self.global_rendezv_port),
            "HOROVOD_CONTROLLER": "gloo",
            "HOROVOD_CPU_OPERATIONS": "gloo",
            "HOROVOD_GLOO_IFACE": str(list(self.nics)[0]),  # TODO
            "NCCL_SOCKET_IFNAME": ",".join(self.nics),  # TDOO
        }


class HorovodMixin:
    def hostname(self) -> str:
        return ray.services.get_node_ip_address()

    def update_env_vars(self, env_vars: dict):
        sanitized = {k: str(v) for k, v in env_vars.items()}
        os.environ.update(sanitized)

    def execute(self, fn, args=None, kwargs=None):
        args = args or []
        kwargs = kwargs or {}
        return fn(*args, **kwargs)


if __name__ == "__main__":
    import ray
    ray.init(address="auto")  #, log_to_driver=False)

    def train(args):
        import horovod.torch as hvd
        print("Training is initializing")
        hvd.init()
        import torch
        device = torch.device('cuda' if torch.cuda.is_available() else "cpu")
        print(f"creating model: {args}")
        import time
        time.sleep(1)
        tune.report(reports=1)
        time.sleep(1)
        tune.report(reports=1)

    horovod_trainable = DistributedTrainableCreator(
        train, use_gpu=True, num_nodes=1, num_workers_per_node=2)
    analysis = tune.run(
        horovod_trainable, config={"lr": tune.uniform(0.1, 1)}, num_samples=10)
    config = analysis.get_best_config(metric="reports")
