import logging
from datetime import timedelta

import ray
import torch
from ray.util.sgd.torch.distributed_torch_runner import LocalDistributedRunner, \
    DistributedTorchRunner, DeactivatedRunner
from ray.util.sgd.torch.utils import setup_address

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

    def train(self, num_steps=None, profile=False, reduce_results=True,
              max_retries=3, info=None, dataset=None):
        raise NotImplementedError

    def update_scheduler(self, metric):
        raise NotImplementedError

    def validate(self, num_steps=None, profile=False, reduce_results=True,
                 info=None):
        raise NotImplementedError

class LocalWorkerGroup(BaseWorkerGroup):
    def __init__(self):
        self.local_worker = DeactivatedRunner()
        self.remote_workers = []


    def start_workers(self, num_workers, params,
                 initialization_hook,
                 timeout_s, num_cpus_per_worker, use_gpu):
        logger.debug(f"start_workers: Setting %d workers." % num_workers)
        self.timeout_s = timeout_s
        self.num_cpus_per_worker = num_cpus_per_worker
        self.use_gpu = use_gpu
        # Start local worker
        self.local_worker = LocalDistributedRunner(
            num_cpus=self.num_cpus_per_worker,
            num_gpus=int(self.use_gpu),
            **params)

        # Generate actor class
        RemoteRunner = ray.remote(
            num_cpus=self.num_cpus_per_worker,
            num_gpus=int(self.use_gpu))(DistributedTorchRunner)
        # Start workers
        self.remote_workers = [
            RemoteRunner.remote(**params) for _ in range(num_workers - 1)
        ]
        if initialization_hook:
            self.apply_all_workers(initialization_hook)

        # Compute URL for initializing distributed PyTorch
        address = setup_address()

        # Runs the creator functions.
        remote_component_setup = [
            worker.setup_components.remote()
            for i, worker in enumerate(self.remote_workers)
        ]
        self.local_worker.setup_components()
        # Get setup tasks in order to throw errors on failure
        ray.get(remote_component_setup)

        # Setup the process group among all workers.
        remote_pgroup_setups = [
            worker.setup_process_group.remote(address, i + 1, num_workers,
                                              timedelta(self.timeout_s))
            for i, worker in enumerate(self.remote_workers)
        ]
        self.local_worker.setup_process_group(address, 0, num_workers,
                                              timedelta(self.timeout_s))
        # Get setup tasks in order to throw errors on failure
        ray.get(remote_pgroup_setups)

        # Runs code that requires all creator functions to have run.
        remote_operator_setups = [
            worker.setup_ddp_and_operator.remote()
            for worker in self.remote_workers
        ]
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

    def train(self, num_steps=None, profile=False, reduce_results=True,
              max_retries=3, info=None, dataset=None):


