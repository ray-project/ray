import io
import logging
import time
from datetime import timedelta

import ray
import torch
from ray.exceptions import RayActorError
from ray.util.sgd.torch.distributed_torch_runner import \
    LocalDistributedRunner, DistributedTorchRunner
from ray.util.sgd.torch.torch_runner import TorchRunner
from ray.util.sgd.torch.utils import setup_address
from ray.util.sgd.utils import check_for_failure

RESIZE_COOLDOWN_S = 10
logger = logging.getLogger(__name__)


class BaseWorkerGroup:
    def start_workers(self, num_workers, params, initialization_hook,
                      timeout_s, num_cpus_per_worker, use_gpu):
        raise NotImplementedError

    def apply_all_operators(self, fn):
        raise NotImplementedError

    def apply_all_workers(self, fn):
        raise NotImplementedError

    def get_local_operator(self):
        raise NotImplementedError

    def get_model(self):
        raise NotImplementedError

    def load_state_dict(self, state_dict, blocking=False):
        raise NotImplementedError

    def shutdown(self, force=False):
        raise NotImplementedError

    def state_dict(self):
        raise NotImplementedError

    def train(self, num_steps=None, profile=False, info=None, dataset=None):
        raise NotImplementedError

    def validate(self, num_steps=None, profile=False, info=None):
        raise NotImplementedError

    def should_resize(self):
        raise NotImplementedError

    def resize_workers(self, max_retries=10):
        raise NotImplementedError

    def get_num_workers(self):
        raise NotImplementedError


class RemoteWorkerGroup(BaseWorkerGroup):
    """Groups together multiple DistributedTorchRunner workers."""

    def __init__(self):
        self.remote_workers = []
        self.num_workers = 0
        self._last_resize = float("-inf")

    def _init_workers(self, num_workers, params, num_cpus_per_worker, use_gpu):
        self.num_workers = num_workers
        self.num_cpus_per_worker = num_cpus_per_worker
        self.use_gpu = use_gpu

        # Generate actor class
        RemoteRunner = ray.remote(
            num_cpus=num_cpus_per_worker,
            num_gpus=int(use_gpu))(DistributedTorchRunner)

        # Start workers
        self.remote_workers = [
            RemoteRunner.remote(**params) for _ in range(num_workers)
        ]

    def _setup_process_group(self, address, timeout_s, num_workers):
        # Setup the process group among all workers.
        remote_pgroup_setups = [
            worker.setup_process_group.remote(address, i, num_workers,
                                              timedelta(timeout_s))
            for i, worker in enumerate(self.remote_workers)
        ]
        return remote_pgroup_setups

    def _setup_operator(self):
        # Runs code that requires all creator functions to have run.
        remote_operator_setups = [
            worker.setup_operator.remote()
            for worker in self.remote_workers
        ]
        return remote_operator_setups

    def start_workers(self, num_workers, params, initialization_hook,
                      timeout_s, num_cpus_per_worker, use_gpu):
        self.num_workers = num_workers
        self.params = params
        self.initialization_hook = initialization_hook
        self.timeout_s = timeout_s
        self.num_cpus_per_worker = num_cpus_per_worker
        self.use_gpu = use_gpu
        if num_workers == 1:
            RemoteRunner = ray.remote(
                num_cpus=num_cpus_per_worker,
                num_gpus=int(use_gpu))(TorchRunner)
            self.remote_workers = [RemoteRunner.remote(**params)]
            self.num_workers = num_workers
            ray.get(self.remote_workers[0].setup_operator.remote())
        else:
            self._init_workers(num_workers, params, num_cpus_per_worker,
                               use_gpu)
            self.num_workers = num_workers

            if initialization_hook:
                self.apply_all_workers(initialization_hook)

            address = setup_address()

            ray.get(
                self._setup_process_group(address, timeout_s,
                                          self.num_workers))

            ray.get(self._setup_operator())

    def _apply_all_operators(self, fn):
        remote_calls = [
            w.apply_operator.remote(fn) for w in self.remote_workers
        ]
        return remote_calls

    def apply_all_operators(self, fn):
        return ray.get(self._apply_all_operators(fn))

    def _apply_all_workers(self, fn):
        return [w.apply.remote(fn) for w in self.remote_workers]

    def apply_all_workers(self, fn):
        return ray.get(self._apply_all_workers(fn))

    def get_local_operator(self):
        raise NotImplementedError(
            "Cannot return a local operators if all "
            "workers are remote. Set use_local to True in"
            "TorchTrainer to access a local operator.")

    def get_model(self):
        ready, _ = ray.wait(
            [r.get_models.remote() for r in self.remote_workers])
        models = ray.get(ready[0])
        return models

    def _load_state_id(self, state_id):
        remote_calls = [
            worker.load_state_stream.remote(state_id)
            for worker in self.remote_workers
        ]
        return remote_calls

    def load_state_dict(self, state_dict, blocking=False):
        _buffer = io.BytesIO()
        torch.save(state_dict, _buffer)
        stream = _buffer.getvalue()
        state_id = ray.put(stream)

        remote_calls = self._load_state_id(state_id)

        if blocking:
            ray.get(remote_calls)

    def state_dict(self):
        ready, _ = ray.wait(
            [r.state_dict.remote() for r in self.remote_workers],
            num_returns=1)
        return ray.get(ready[0])

    def _train(self, num_steps, profile, info, dataset=None):
        remote_worker_stats = []
        for i, w in enumerate(self.remote_workers):
            params = dict(num_steps=num_steps, profile=profile, info=info)
            if dataset:
                params["iterator"] = dataset.get_shard(i)
            stats = w.train_epoch.remote(**params)
            remote_worker_stats.append(stats)
        return remote_worker_stats

    def train(self, num_steps=None, profile=False, info=None, dataset=None):
        if dataset:
            dataset.set_num_shards(self.num_workers)
        remote_worker_stats = self._train(num_steps, profile, info, dataset)
        success = check_for_failure(remote_worker_stats)
        if success:
            return success, ray.get(remote_worker_stats)
        return success, None

    def _validate(self, params):
        remote_worker_stats = [
            w.validate.remote(**params) for w in self.remote_workers
        ]
        return remote_worker_stats

    def validate(self, num_steps=None, profile=False, info=None):
        params = dict(num_steps=num_steps, profile=profile, info=info)
        remote_worker_stats = self._validate(params)
        return ray.get(remote_worker_stats)

    def _shutdown_remote_workers(self):
        cleanup = [worker.shutdown.remote() for worker in self.remote_workers]
        return cleanup

    def _terminate_remote_workers(self, cleanup):
        try:
            ray.get(cleanup)
            [
                worker.__ray_terminate__.remote() for worker in self.remote_workers
            ]
        except RayActorError:
            logger.warning("Failed to shutdown gracefully, forcing a "
                           "shutdown.")
            self._reset()


    def shutdown(self, force=False):
        if not force:
            cleanup = [worker.shutdown.remote() for worker in self.remote_workers]
            self._terminate_remote_workers(cleanup)
        else:
            self._reset()
        self.remote_workers = []

    def _reset(self):
        for worker in self.remote_workers:
            logger.debug(f"Killing worker {worker}.")
            ray.kill(worker)
        self.remote_workers = []

    def should_resize(self):
        """Returns True if past cooldown and exists resources to scale up."""
        worker_gap = self.num_workers - len(self.remote_workers)
        past_cooldown = (time.time() - self._last_resize) > RESIZE_COOLDOWN_S
        if past_cooldown and worker_gap:
            # Assume 1 resource is already reserved for local worker.
            potential_remote_size = self._check_potential_remote_workers_size()
            return potential_remote_size > 0
        return False

    def _check_potential_remote_workers_size(self):
        # ASSUME 1 GPU + 1 CPU is already reserved for the local worker
        remote_resources = ray.available_resources()
        max_remote_workers = self.num_workers
        new_remote_workers = min(
            remote_resources.get("CPU", 0), max_remote_workers)
        if self.use_gpu:
            new_remote_workers = min(
                remote_resources.get("GPU", 0), new_remote_workers)
        return new_remote_workers

    def resize_workers(self, max_retries=10):
        self._reset()

        time.sleep(1)
        for i in range(max_retries):
            new_remote_workers = self._check_potential_remote_workers_size()
            if new_remote_workers:
                self._last_resize = time.time()
                self.start_workers(int(new_remote_workers),
                                   params=self.params,
                                   initialization_hook=self.initialization_hook,
                                   num_cpus_per_worker=self.num_cpus_per_worker,
                                   use_gpu=self.use_gpu,
                                   timeout_s=self.timeout_s)
                self.load_state_dict(self.state_dict())
                return
            else:
                delay = 2**i
                logger.warning(
                    "No new workers found. Retrying in %d sec." % delay)
                time.sleep(delay)
        raise RuntimeError("Exceeded max_retries for relaunching workers.")

    def get_num_workers(self):
        return len(self.remote_workers)


class LocalWorkerGroup(BaseWorkerGroup):
    def __init__(self):
        self.local_worker = None
        self.remote_worker_group = RemoteWorkerGroup()
        self.num_workers = 0

    def start_workers(self, num_workers, params, initialization_hook,
                      timeout_s, num_cpus_per_worker, use_gpu):
        logger.debug(f"start_workers: Setting %d workers." % num_workers)

        self.num_workers = num_workers
        self.params = params
        self.initialization_hook = initialization_hook
        self.timeout_s = timeout_s
        self.num_cpus_per_worker = num_cpus_per_worker
        self.use_gpu = use_gpu

        if num_workers == 1:
            self.local_worker = TorchRunner(**params)
            if initialization_hook:
                self.apply_all_workers(initialization_hook)
            self.local_worker.setup_operator()
        else:
            # Start local worker
            self.local_worker = LocalDistributedRunner(
                num_cpus=num_cpus_per_worker, num_gpus=int(use_gpu), **params)
            self.remote_worker_group._init_workers(
                num_workers - 1, params, num_cpus_per_worker, use_gpu)
            if initialization_hook:
                self.apply_all_workers(initialization_hook)

            # Compute URL for initializing distributed PyTorch
            address = setup_address()

            remote_pgs = self.remote_worker_group._setup_process_group(
                address, timeout_s, num_workers)
            self.local_worker.setup_process_group(address, num_workers - 1,
                                                  num_workers,
                                                  timedelta(timeout_s))
            ray.get(remote_pgs)

            remote_operators = self.remote_worker_group._setup_operator()
            self.local_worker.setup_operator()
            ray.get(remote_operators)

    def apply_all_operators(self, fn):
        remote_calls = self.remote_worker_group._apply_all_operators(fn)
        local_call = self.local_worker.apply_operator(fn)
        return [local_call] + ray.get(remote_calls)

    def apply_all_workers(self, fn):
        remote_calls = self.remote_worker_group.apply_all_workers(fn)
        local_call = self.local_worker.apply(fn)
        return [local_call] + ray.get(remote_calls)

    def get_local_operator(self):
        return self.local_worker.training_operator

    def get_model(self):
        return self.local_worker.models

    def load_state_dict(self, state_dict, blocking=False):
        # This is not the most efficient because you have to wait for
        # the local worker to save then dump to buffer.
        self.local_worker.load_state_dict(state_dict)
        state_id = ray.put(self.local_worker.state_stream())

        remote_calls = self.remote_worker_group._load_state_id(state_id)
        if blocking:
            ray.get(remote_calls)

    def state_dict(self):
        return self.local_worker.state_dict()

    def should_resize(self):
        """Returns True if past cooldown and exists resources to scale up."""
        return self.remote_worker_group.should_resize()

    def _reset(self):
        """Terminates models without giving up local resource reservation."""
        self.local_worker.shutdown(cleanup=False)
        self.remote_worker_group._reset()

        self.local_worker = None
        self.remote_worker_group = RemoteWorkerGroup()

    def resize_workers(self, max_retries=10):
        self._reset()

        time.sleep(1)
        for i in range(max_retries):
            new_remote_workers = \
                self.remote_worker_group._check_potential_remote_workers_size()
            if new_remote_workers:
                self._last_resize = time.time()
                self.start_workers(int(new_remote_workers) + 1, params=self.params,
                                   initialization_hook=self.initialization_hook,
                                   num_cpus_per_worker=self.num_cpus_per_worker,
                                   use_gpu=self.use_gpu,
                                   timeout_s=self.timeout_s)
                self.load_state_dict(self.state_dict())
                return
            else:
                delay = 2**i
                logger.warning(
                    "No new workers found. Retrying in %d sec." % delay)
                time.sleep(delay)
        raise RuntimeError("Exceeded max_retries for relaunching workers.")

    def train(self, num_steps=None, profile=False, info=None, dataset=None):
        params = dict(num_steps=num_steps, profile=profile, info=info)
        if dataset:
            dataset.set_num_shards(self.num_workers)

        remote_worker_stats = self.remote_worker_group._train(
            num_steps, profile, info, dataset)
        try:
            if dataset:
                params["iterator"] = dataset.get_shard(self.num_workers - 1)
            local_worker_stats = self.local_worker.train_epoch(**params)
        except RuntimeError as err:
            if "gloo" in err.args[0] and "Timed out" in err.args[0]:
                logger.warning(err)
                return False, None
            if "NCCL" in err.args[0]:  # there is no specific error message
                logger.warning(err)
                return False, None

            raise err

        success = check_for_failure(remote_worker_stats)
        if success:
            return success, [local_worker_stats] + ray.get(remote_worker_stats)

        return success, None

    def validate(self, num_steps=None, profile=False, info=None):
        params = dict(num_steps=num_steps, profile=profile, info=info)

        remote_worker_stats = self.remote_worker_group._validate(params)
        local_worker_stats = self.local_worker.validate(**params)
        worker_stats = [local_worker_stats] + ray.get(remote_worker_stats)
        return worker_stats

    def shutdown(self, force=False):
        if not force:
            cleanup = self.remote_worker_group._shutdown_remote_workers()
            self.local_worker.shutdown()
            self.remote_worker_group._terminate_remote_workers(cleanup)
        else:
            self.local_worker.shutdown()
            self.remote_worker_group._reset()

        self.local_worker = None
        self.remote_worker_group = RemoteWorkerGroup()

    def get_num_workers(self):
        return self.remote_worker_group.get_num_workers() + 1

class DeactivatedWorkerGroup:
    def __getattr__(self, *args, **kwargs):
        raise RuntimeError(
            "This TorchTrainer is not active (it is likely shutdown already). "
            "Create a new TorchTrainer.")

