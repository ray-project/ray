import logging
import time
from datetime import timedelta

import ray
import torch
from ray.exceptions import RayActorError
from ray.util.sgd.torch.distributed_torch_runner import LocalDistributedRunner, \
    DistributedTorchRunner, DeactivatedRunner
from ray.util.sgd.torch.torch_runner import TorchRunner
from ray.util.sgd.torch.utils import setup_address
from ray.util.sgd.utils import check_for_failure

RESIZE_COOLDOWN_S = 10
logger = logging.getLogger(__name__)

class BaseWorkerGroup:
    def apply_all_operators(self, fn):
        raise NotImplementedError

    def apply_all_workers(self, fn):
        raise NotImplementedError

    def get_local_operator(self):
        raise NotImplementedError

    def get_model(self):
        raise NotImplementedError

    def load(self, checkpoint):
        raise NotImplementedError

    def load_state_dict(self, state_dict, blocking=False):
        raise NotImplementedError

    def restore(self, *args):
        raise NotImplementedError

    def save(self, checkpoint):
        raise NotImplementedError

    def shutdown(self, force=False):
        raise NotImplementedError

    def state_dict(self):
        raise NotImplementedError

    def train(self, num_steps=None, profile=False, info=None, dataset=None):
        raise NotImplementedError

    def update_scheduler(self, metric):
        raise NotImplementedError

    def validate(self, num_steps=None, profile=False,
                 info=None):
        raise NotImplementedError

class RemoteWorkerGroup(BaseWorkerGroup):
    """Groups together multiple DistributedTorchRunner workers."""
    def __init__(self):
        self.remote_workers = []

    def _init_workers(self, num_workers, params, num_cpus_per_worker, use_gpu,
                      initialization_hook=None):
        self.num_workers = num_workers

        # Generate actor class
        RemoteRunner = ray.remote(num_cpus=num_cpus_per_worker,
                                  num_gpus=int(use_gpu))(DistributedTorchRunner)

        # Start workers
        self.remote_workers = [
            RemoteRunner.remote(**params) for _ in range(num_workers)
        ]

        if initialization_hook:
            self.apply_all_workers(initialization_hook)

    def _setup_components(self):
        # Runs the creator functions.
        remote_component_setup = [
            worker.setup_components.remote()
            for i, worker in enumerate(self.remote_workers)
        ]
        return remote_component_setup
        # Get setup tasks in order to throw errors on failure
        ray.get(remote_component_setup)

    def _setup_process_group(self, address, timeout_s):
        # Setup the process group among all workers.
        remote_pgroup_setups = [
            worker.setup_process_group.remote(address, i + 1, self.num_workers,
                                              timedelta(timeout_s))
            for i, worker in enumerate(self.remote_workers)
        ]
        return remote_pgroup_setups

    def _setup_operator(self):
        # Runs code that requires all creator functions to have run.
        remote_operator_setups = [
            worker.setup_ddp_and_operator.remote()
            for worker in self.remote_workers
        ]
        return remote_operator_setups

    def _start_workers(self, num_workers, params, initialization_hook,
                       timeout_s, num_cpus_per_worker, use_gpu, address):
        self._init_workers(num_workers, params, num_cpus_per_worker,
                           use_gpu, initialization_hook)

        remote_component_setup = self._setup_components()
        ray.get(remote_component_setup)

        remote_pgroup_setups = self._setup_process_group(address, timeout_s)
        ray.get(remote_pgroup_setups)

        remote_operator_setups = self._setup_operator()
        ray.get(remote_operator_setups)

    def _apply_all_operators(self, fn):
        remote_calls = [
            w.apply_operator.remote(fn) for w in self.remote_workers
        ]
        return remote_calls

    def apply_all_operators(self, fn):
        return ray.get(self._apply_all_operators(fn))

class LocalWorkerGroup(BaseWorkerGroup):
    def __init__(self):
        self.local_worker = DeactivatedRunner()
        self.remote_worker_group = RemoteWorkerGroup()

    def _start_workers(self, num_workers, params,
                 initialization_hook,
                 timeout_s, num_cpus_per_worker, use_gpu, address=None):
        logger.debug(f"start_workers: Setting %d workers." % num_workers)

        if num_workers == 1:
            self.local_worker = TorchRunner(**params)
        else:
            # Start local worker
            self.local_worker = LocalDistributedRunner(
                num_cpus=num_cpus_per_worker,
                num_gpus=int(use_gpu),
                **params)
            self.remote_worker_group._start_workers(num_workers-1, params,
                                                    initialization_hook, )

        if not address:
            # Compute URL for initializing distributed PyTorch
            address = setup_address()


        self.local_worker.setup_components()



        self.local_worker.setup_process_group(address, 0, num_workers,
                                              timedelta(self.timeout_s))



        self.local_worker.setup_ddp_and_operator()
        # Get setup tasks in order to throw errors on failure
        ray.get(remote_operator_setups)

    def apply_all_operators(self, fn):
        remote_calls = [
            w.apply_operator.remote(fn) for w in self.remote_workers
        ]
        local_call = self.local_worker.apply_operator(fn)
        return [local_call] + ray.get(remote_calls)

    def apply_all_workers(self, fn):
        remote_calls = [w.apply.remote(fn) for w in self.remote_workers]
        local_call = self.local_worker.apply(fn)
        return [local_call] + ray.get(remote_calls)

    def get_local_operator(self):
        return self.local_worker.training_operator

    def get_model(self):
        unwrapped = []
        for model in self.local_worker.models:
            unwrapped += [model.module if hasattr(model, "module") else model]
        if len(unwrapped) == 1:
            return unwrapped[0]
        return unwrapped

    def load(self, checkpoint):
        state_dict = torch.load(checkpoint)
        self.load_state_dict(state_dict)

    def load_state_dict(self, state_dict, blocking=False):
        # This is not the most efficient because you have to wait for
        # the local worker to save then dump to buffer.
        self.local_worker.load_state_dict(state_dict)
        state_id = ray.put(self.local_worker.state_stream())

        remote_calls = [
            worker.load_state_stream.remote(state_id)
            for worker in self.remote_workers
        ]
        if blocking:
            ray.get(remote_calls)

    def save(self, checkpoint):
        torch.save(self.state_dict(), checkpoint)
        return checkpoint

    def state_dict(self):
        return self.local_worker.state_dict()

    def _should_resize(self):
        """Returns True if past cooldown and exists resources to scale up."""
        worker_gap = self.num_workers - 1 - len(self.remote_workers)
        past_cooldown = (time.time() - self._last_resize) > RESIZE_COOLDOWN_S
        if past_cooldown and worker_gap:
            # Assume 1 resource is already reserved for local worker.
            potential_remote_size = self._check_potential_remote_workers_size()
            return potential_remote_size > 0
        return False

    def _reset(self):
        """Terminates models without giving up local resource reservation."""
        self.local_worker.shutdown(cleanup=False)
        for worker in self.remote_workers:
            logger.debug(f"Killing worker {worker}.")
            ray.kill(worker)
        self.local_worker = DeactivatedRunner()
        self.remote_workers = []

    def _check_potential_remote_workers_size(self):
        # ASSUME 1 GPU + 1 CPU is already reserved for the local worker
        remote_resources = ray.available_resources()
        max_remote_workers = self.num_workers - 1
        new_remote_workers = min(
            remote_resources.get("CPU", 0), max_remote_workers)
        if self.use_gpu:
            new_remote_workers = min(
                remote_resources.get("GPU", 0), new_remote_workers)
        return new_remote_workers

    def _resize_workers(self, max_retries=10):
        self._reset()

        time.sleep(1)
        for i in range(max_retries):
            new_remote_workers = self._check_potential_remote_workers_size()
            if new_remote_workers:
                self._last_resize = time.time()
                self._start_workers(int(new_remote_workers) + 1)
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
        remote_worker_stats = []
        if dataset:
            dataset.set_num_shards(self.num_workers)
        for i, w in enumerate(self.remote_workers):
            params = dict(num_steps=num_steps, profile=profile, info=info)
            if dataset:
                params["iterator"] = dataset.get_shard(i)
            stats = w.train_epoch.remote(**params)
            remote_worker_stats.append(stats)

        try:
            if dataset:
                params["iterator"] = dataset.get_shard(
                    len(self.remote_workers))
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

        remote_worker_stats = [
            w.validate.remote(**params) for w in self.remote_workers
        ]
        local_worker_stats = self.local_worker.validate(**params)
        worker_stats = [local_worker_stats] + ray.get(remote_worker_stats)
        return worker_stats

    def update_scheduler(self, metric):
        self.apply_all_operators(
            lambda op: [sched.step(metric) for sched in op.schedulers])

    def shutdown(self, force=False):
        if not force:
            cleanup = [
                worker.shutdown.remote() for worker in self.remote_workers
            ]
            self.local_worker.shutdown()
            try:
                ray.get(cleanup)
                [
                    worker.__ray_terminate__.remote()
                    for worker in self.remote_workers
                ]
            except RayActorError:
                logger.warning(
                    "Failed to shutdown gracefully, forcing a shutdown.")

                for worker in self.remote_workers:
                    logger.warning(f"Killing worker {worker}.")
                    ray.kill(worker)
        else:
            self.local_worker.shutdown()
            for worker in self.remote_workers:
                logger.debug(f"Killing worker {worker}.")
                ray.kill(worker)

        self.local_worker = DeactivatedRunner()
        self.remote_workers = []

