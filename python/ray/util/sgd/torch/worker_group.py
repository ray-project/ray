import io
import logging
import time
from collections import defaultdict
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


class WorkerGroupInterface:
    """Manages a group of TorchRunner workers."""

    def start_workers(self, num_workers):
        """Start workers for training.

        This method has 4 steps.
            1. Creates `num_workers` TorchRunner objects, either all as remote
                actors or 1 locally and all but one remote.
            2. If necessary, applies an initialization hook to all the
                workers.
            3. Sets up the process group for all workers if running in
                distributed setting.
            4. Instantiates the user provided TrainingOperator on all
                workers to set up training state.
        """
        raise NotImplementedError

    def apply_all_operators(self, fn):
        """See TorchTrainer.apply_all_operators."""
        raise NotImplementedError

    def apply_all_workers(self, fn):
        """See TorchTrainer.apply_all_workers."""
        raise NotImplementedError

    def get_local_operator(self):
        """See TorchTrainer.get_local_operator."""
        raise NotImplementedError

    def get_model(self):
        """See TorchTrainer.get_model."""
        raise NotImplementedError

    def load_state_dict(self, state_dict, blocking=False):
        """See TorchTrainer.load_state_dict."""
        raise NotImplementedError

    def new_workers_size(self):
        """Returns number of workers to create based on available resources.
        Total number of workers will never exceed `max_workers` amount.
        """
        raise NotImplementedError

    def reset(self):
        """Resets worker group."""
        raise NotImplementedError

    def should_scale_up(self):
        """Returns whether to scale up the number of remote workers.

        This method returns True if current number of workers is less than
        max_workers provided during startup, enough resources are
        available to scale up, and a sufficient cooldown period has passed.
        """
        raise NotImplementedError

    def shutdown(self, force=False):
        """See TorchTrainer.shutdown."""
        raise NotImplementedError

    def state_dict(self):
        """See TorchTrainer.state_dict."""
        raise NotImplementedError

    def train(self, num_steps=None, profile=False, info=None, dataset=None):
        """Runs one epoch of training on all workers.
        Args:
            See TorchTrainer.train.
        Returns:
            Tuple of (bool, list). First value is True if training was
                successful and False otherwise. Second value is list of
                training results from all workers if training was successful,
                and None otherwise.
        """
        raise NotImplementedError

    def validate(self, num_steps=None, profile=False, info=None):
        """Runs validation for all workers.
        Args:
            See TorchTrainer.validate.
        Return:
            List of validation results for each worker.
        """
        raise NotImplementedError


class RemoteWorkerGroup(WorkerGroupInterface):
    """A group of TorchRunner workers that are all remote Ray actors.

    Args:
        max_workers (int): Maximum number of workers to use.
        params (dict): Parameters to pass into a TorchRunner worker.
        dist_params (dict): Additional parameters for distributed training
            to pass into a DistributedTorchRunner worker.
        initialization_hook (Callable): See TorchTrainer.__init__.
        timeout_s (float): See TorchTrainer.__init__.
        num_cpus_per_worker (int): See TorchTrainer.__init__.
        use_gpu (bool): See TorchTrainer.__init__.

    """

    def __init__(self, max_workers, params, dist_params, initialization_hook,
                 timeout_s, num_cpus_per_worker, use_gpu):
        # Invariant: These variables should never change state!
        self._max_workers = max_workers
        self._params = params
        self._dist_params = dist_params
        self._initialization_hook = initialization_hook
        self._timeout_s = timeout_s
        self._num_cpus_per_worker = num_cpus_per_worker
        self._use_gpu = use_gpu

        self.remote_workers = []

        # The last time when this worker group was resized.
        self._last_resize = float("-inf")

    def _init_dist_workers(self, num_workers):
        """Create `num_workers` remote workers."""
        # Generate actor class
        RemoteRunner = ray.remote(
            num_cpus=self._num_cpus_per_worker,
            num_gpus=int(self._use_gpu))(DistributedTorchRunner)

        # Start workers
        self.remote_workers = [
            RemoteRunner.remote(**{
                **self._params,
                **self._dist_params
            }) for _ in range(num_workers)
        ]

    def _setup_process_group(self, address, world_size, starting_rank=0):
        """Sets up process group for all workers.

        Args:
            address (str): Address to use for TCP process group setup. The
                provided address must use the IP address of the node that the
                rank 0 worker is located on.
            world_size (int): Total number of training workers in the
                process group. This may differ from self.num_workers if
                there are additional workers outside this worker group class.
            starting_rank (int): The rank to use for the first worker.
                Worker ranks will be in [starting_rank,
                len(self.remote_workers)+starting_rank). This is useful if
                you want to use worker outside of this group as the rank 0
                worker.

        Returns:
            List of process group set up promises.
        """
        # Setup the process group among all workers.
        remote_pgroup_setups = [
            worker.setup_process_group.remote(
                url=address,
                world_rank=i + starting_rank,
                world_size=world_size,
                timeout=timedelta(seconds=self._timeout_s))
            for i, worker in enumerate(self.remote_workers)
        ]
        return remote_pgroup_setups

    def _setup_operator(self):
        """Instantiates user provided TrainingOperator on all workers.

        Returns:
            List of operator setup promises.
        """
        # Runs code that requires all creator functions to have run.
        remote_operator_setups = [
            worker.setup_operator.remote() for worker in self.remote_workers
        ]
        return remote_operator_setups

    def _setup_local_rank(self, rank_counter_dict=None):
        """Sets local rank for all workers."""
        if rank_counter_dict is None:
            rank_counter_dict = defaultdict(int)
        node_ips = ray.get(
            [w.get_node_ip.remote() for w in self.remote_workers])
        futures = []
        for ip, worker in zip(node_ips, self.remote_workers):
            rank = rank_counter_dict[ip]
            futures.append(worker.set_local_rank.remote(rank))
            rank_counter_dict[ip] += 1
        return futures

    def start_workers(self, num_workers):
        logger.debug(f"start_workers: Setting %d workers." % num_workers)
        if num_workers == 1:
            RemoteRunner = ray.remote(
                num_cpus=self._num_cpus_per_worker,
                num_gpus=int(self._use_gpu))(TorchRunner)
            self.remote_workers = [RemoteRunner.remote(**self._params)]
            ray.get(self.remote_workers[0].setup_operator.remote())
        else:
            self._init_dist_workers(num_workers)

            if self._initialization_hook:
                self.apply_all_workers(self._initialization_hook)

            # Make sure to get the IP address of the rank 0 worker node.
            address = ray.get(self.remote_workers[0].setup_address.remote())

            ray.get(
                self._setup_process_group(
                    address=address, world_size=num_workers))

            ray.get(self._setup_local_rank())

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
        """Loads the object with id `state_id` to all workers."""
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
        # This is needed to handle calling ray.get on a dead actor.
        buffer_object = None
        futures = {r.state_stream.remote() for r in self.remote_workers}
        while len(futures) > 0:
            ready, _ = ray.wait(list(futures), num_returns=1)
            object_ref = ready[0]
            try:
                buffer_object = ray.get(object_ref)
            except RayActorError:
                futures.remove(object_ref)
            else:
                break
        if buffer_object is None:
            raise RuntimeError("Obtaining state_dict from remote workers is "
                               "unsuccessful since all workers have died.")
        to_gpu = self._use_gpu and torch.cuda.is_available()
        _buffer = io.BytesIO(buffer_object)
        state_dict = torch.load(
            _buffer,
            map_location=("cpu" if not to_gpu else
                          lambda storage, loc: storage.cuda()))
        return state_dict

    def _train(self, num_steps, profile, info, dataset=None):
        """Runs 1 epoch of training on all workers.
        Returns training result for all workers as promises.
        """
        remote_worker_stats = []
        for i, w in enumerate(self.remote_workers):
            params = dict(num_steps=num_steps, profile=profile, info=info)
            if dataset:
                params["iterator"] = dataset.get_shard(i)
            stats = w.train_epoch.remote(**params)
            remote_worker_stats.append(stats)
        return remote_worker_stats

    def train(self, num_steps=None, profile=False, info=None, dataset=None):
        """Runs 1 epoch of training on all workers.

        Has additional logic to check for worker failure.
        """
        if dataset:
            dataset.set_num_shards(self.num_workers)
        remote_worker_stats = self._train(num_steps, profile, info, dataset)
        # Check if each worker has failed before calling ray.get.
        success = check_for_failure(remote_worker_stats)
        if success:
            return success, ray.get(remote_worker_stats)
        return success, None

    def _validate(self, params):
        """Runs validation for each worker. Returns results as promises."""
        remote_worker_stats = [
            w.validate.remote(**params) for w in self.remote_workers
        ]
        return remote_worker_stats

    def validate(self, num_steps=None, profile=False, info=None):
        params = dict(num_steps=num_steps, profile=profile, info=info)
        remote_worker_stats = self._validate(params)
        return ray.get(remote_worker_stats)

    def _shutdown_remote_workers(self):
        """Shuts down each worker and returns a list of cleanup promises."""
        cleanup = [worker.shutdown.remote() for worker in self.remote_workers]
        return cleanup

    def _terminate_remote_workers(self, cleanup):
        """Blocks on worker shutdown and then terminates each worker actor.

        If graceful shutdown fails, forcefully kills all actors.
        """
        try:
            ray.get(cleanup)
            [
                worker.__ray_terminate__.remote()
                for worker in self.remote_workers
            ]
        except RayActorError:
            logger.warning("Failed to shutdown gracefully, forcing a "
                           "shutdown.")
            self.reset()

    def shutdown(self, force=False):
        if not force:
            cleanup = [
                worker.shutdown.remote() for worker in self.remote_workers
            ]
            self._terminate_remote_workers(cleanup)
        else:
            self.reset()
        self.remote_workers = []

    def reset(self):
        for worker in self.remote_workers:
            logger.debug(f"Killing worker {worker}.")
            ray.kill(worker)
        self.remote_workers = []

    def should_scale_up(self):
        worker_gap = self._max_workers - self.num_workers
        past_cooldown = (time.time() - self._last_resize) > RESIZE_COOLDOWN_S
        if past_cooldown and worker_gap:
            # Assume 1 resource is already reserved for local worker.
            potential_remote_size = self.new_workers_size()
            return potential_remote_size > 0
        return False

    def new_workers_size(self):
        """Returns number of workers to create based on available resources."""
        remote_resources = ray.available_resources()
        max_remote_workers = self._max_workers
        new_remote_workers = min(
            remote_resources.get("CPU", 0), max_remote_workers)
        if self._use_gpu:
            new_remote_workers = min(
                remote_resources.get("GPU", 0), new_remote_workers)
        return new_remote_workers

    @property
    def num_workers(self):
        """Current number of workers being used for training.
        This may differ from self.num_workers if self.resize_workers has
        been called.
        """
        return len(self.remote_workers)


class LocalWorkerGroup(WorkerGroupInterface):
    """A group of TorchRunner workers.
    1 worker runs locally, and all the other workers are remote actors.

    Args:
        Same as RemoteWorkerGroup.
    """

    def __init__(self, max_workers, params, dist_params, initialization_hook,
                 timeout_s, num_cpus_per_worker, use_gpu):

        # Invariant: These variables should never change state!
        self._max_workers = max_workers
        self._params = params
        self._dist_params = dist_params
        self._initialization_hook = initialization_hook
        self._timeout_s = timeout_s
        self._num_cpus_per_worker = num_cpus_per_worker
        self._use_gpu = use_gpu

        self.local_worker = None
        self.remote_worker_group = RemoteWorkerGroup(
            max_workers=max_workers - 1,
            params=params,
            dist_params=dist_params,
            initialization_hook=initialization_hook,
            timeout_s=timeout_s,
            num_cpus_per_worker=num_cpus_per_worker,
            use_gpu=use_gpu)

    def start_workers(self, num_workers):
        logger.debug(f"start_workers: Setting %d workers." % num_workers)

        if num_workers == 1:
            self.local_worker = TorchRunner(**self._params)
            if self._initialization_hook:
                self.apply_all_workers(self._initialization_hook)
            self.local_worker.setup_operator()
        else:

            # Start local worker
            self.local_worker = LocalDistributedRunner(
                num_cpus=self._num_cpus_per_worker,
                num_gpus=int(self._use_gpu),
                **{
                    **self._params,
                    **self._dist_params
                })
            self.remote_worker_group._init_dist_workers(num_workers - 1)
            if self._initialization_hook:
                self.apply_all_workers(self._initialization_hook)

            # Compute URL for initializing distributed PyTorch.
            address = setup_address()

            remote_pgs = self.remote_worker_group._setup_process_group(
                address=address, world_size=num_workers, starting_rank=1)
            # Use the local worker as rank 0. This will help with debugging.
            self.local_worker.setup_process_group(
                url=address,
                world_rank=0,
                world_size=num_workers,
                timeout=timedelta(seconds=self._timeout_s))
            ray.get(remote_pgs)

            local_node_ip = ray.services.get_node_ip_address()
            rank_dict = defaultdict(int)
            self.local_worker.set_local_rank(local_rank=0)
            rank_dict[local_node_ip] += 1
            self.remote_worker_group._setup_local_rank(rank_dict)

            remote_operators = self.remote_worker_group._setup_operator()
            self.local_worker.setup_operator()
            ray.get(remote_operators)

    def apply_all_operators(self, fn):
        remote_calls = self.remote_worker_group._apply_all_operators(fn)
        local_call = self.local_worker.apply_operator(fn)
        return [local_call] + ray.get(remote_calls)

    def apply_all_workers(self, fn):
        remote_calls = self.remote_worker_group._apply_all_workers(fn)
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

    def should_scale_up(self):
        return self.remote_worker_group.should_scale_up()

    def reset(self):
        """Terminates models without giving up local resource reservation."""
        if not isinstance(self.local_worker, LocalDistributedRunner):
            self.local_worker.shutdown()
        else:
            self.local_worker.shutdown(cleanup=False)
        self.remote_worker_group.reset()

        self.local_worker = None
        self.remote_worker_group = RemoteWorkerGroup(
            max_workers=self._max_workers - 1,
            params=self._params,
            dist_params=self._dist_params,
            initialization_hook=self._initialization_hook,
            num_cpus_per_worker=self._num_cpus_per_worker,
            use_gpu=self._use_gpu,
            timeout_s=self._timeout_s)

    def new_workers_size(self):
        return self.remote_worker_group.new_workers_size() + 1

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
            if "Connection closed by peer" in err.args[0]:
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
            self.remote_worker_group.reset()

        self.local_worker = None
        self.remote_worker_group = DeactivatedWorkerGroup()

    @property
    def num_workers(self):
        return self.remote_worker_group.num_workers + 1

    @property
    def remote_workers(self):
        return self.remote_worker_group.remote_workers


class DeactivatedWorkerGroup:
    def __getattr__(self, *args, **kwargs):
        raise RuntimeError(
            "This TorchTrainer is not active (it is likely shutdown already). "
            "Create a new TorchTrainer.")
