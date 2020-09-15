# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
from contextlib import contextmanager
import os
import logging
import shutil
import tempfile
import torch
from datetime import timedelta

import ray
from ray import tune
from ray.tune.result import RESULT_DUPLICATE
from ray.tune.logger import NoopLogger
from ray.tune.function_runner import wrap_function
from ray.tune.resources import Resources
from ray.tune.trainable import TrainableUtil
from ray.tune.utils import detect_checkpoint_function
from ray.util.sgd.torch.utils import setup_process_group, setup_address
from ray.util.sgd.torch.constants import NCCL_TIMEOUT_S

logger = logging.getLogger(__name__)

_distributed_enabled = False


def is_distributed_trainable():
    """Returns True if executing within a DistributedTrainable."""
    return _distributed_enabled


def enable_distributed_trainable():
    global _distributed_enabled
    _distributed_enabled = True


def logger_creator(log_config, logdir, rank):
    worker_dir = os.path.join(logdir, "worker_{}".format(rank))
    os.makedirs(worker_dir, exist_ok=True)
    return NoopLogger(log_config, worker_dir)


class _TorchTrainable(tune.Trainable):
    """Base class for distributed training on Tune.

    A wrapper class is needed to actually create a working
    version of this trainable.
    """
    _function = None
    _num_workers = None
    _use_gpu = None
    _num_cpus_per_worker = None

    __slots__ = ["workers", "_finished"]

    @classmethod
    def default_process_group_parameters(self):
        return dict(timeout=timedelta(NCCL_TIMEOUT_S), backend="gloo")

    @classmethod
    def get_remote_worker_options(self):
        num_gpus = 1 if self._use_gpu else 0
        num_cpus = int(self._num_cpus_per_worker or 1)
        return dict(num_cpus=num_cpus, num_gpus=num_gpus)

    def setup(self, config):
        self._finished = False
        num_workers = self._num_workers
        logdir = self.logdir
        assert self._function

        func_trainable = wrap_function(self.__class__._function)

        remote_trainable = ray.remote(func_trainable)
        remote_trainable = remote_trainable.options(
            **self.get_remote_worker_options())

        self.workers = [
            remote_trainable.remote(
                config=config,
                logger_creator=lambda cfg: logger_creator(cfg, logdir, rank))
            for rank in range(num_workers)
        ]

        # Address has to be IP of rank 0 worker's node.
        address = ray.get(
            self.workers[0].execute.remote(lambda _: setup_address()))

        pgroup_params = self.default_process_group_parameters()
        from functools import partial
        setup_on_worker = partial(
            setup_process_group,
            url=address,
            world_size=num_workers,
            **pgroup_params)
        ray.get([
            w.execute.remote(lambda _: setup_on_worker(world_rank=rank))
            for rank, w in enumerate(self.workers)
        ])

        ray.get([
            w.execute.remote(lambda _: enable_distributed_trainable())
            for rank, w in enumerate(self.workers)
        ])

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


def DistributedTrainableCreator(func,
                                use_gpu=False,
                                num_workers=1,
                                num_cpus_per_worker=1,
                                backend="gloo",
                                timeout_s=NCCL_TIMEOUT_S):
    """Creates a class that executes distributed training.

    Similar to running `torch.distributed.launch`.

    Note that you typically should not instantiate the object
    created.

    Args:
        func (callable): This function is a Tune trainable function.
            This function must have 2 args in the signature, and the
            latter arg must contain `checkpoint_dir`. For example:
            `func(config, checkpoint_dir=None)`.
        use_gpu (bool): Sets resource allocation for workers to 1 GPU
            if true. Also automatically sets CUDA_VISIBLE_DEVICES
            for each training worker.
        num_workers (int): Number of training workers to include in
            world.
        num_cpus_per_worker (int): Number of CPU resources to reserve
            per training worker.
        backend (str): One of "gloo", "nccl".
        timeout_s (float): Seconds before the torch process group
            times out. Useful when machines are unreliable. Defaults
            to 60 seconds.

    Returns:
        A trainable class object that can be passed to Tune. Resources
            are automatically set within the object, so users do
            not need to set `resources_per_trainable`.

    Example:

    .. code-block:: python

        trainable_cls = DistributedTrainableCreator(
            train_func, num_workers=2)
        analysis = tune.run(trainable_cls)
    """
    detect_checkpoint_function(func, abort=True)

    class WrappedDistributedTorchTrainable(_TorchTrainable):
        _function = func
        _num_workers = num_workers
        _use_gpu = use_gpu
        _num_cpus_per_worker = num_cpus_per_worker

        @classmethod
        def default_process_group_parameters(self):
            return dict(timeout=timedelta(timeout_s), backend=backend)

        @classmethod
        def default_resource_request(cls, config):
            num_workers_ = int(config.get("num_workers", num_workers))
            num_cpus = int(
                config.get("num_cpus_per_worker", num_cpus_per_worker))
            use_gpu_ = config.get("use_gpu", use_gpu)

            return Resources(
                cpu=0,
                gpu=0,
                extra_cpu=num_cpus * num_workers_,
                extra_gpu=num_workers_ if use_gpu_ else 0)

    return WrappedDistributedTorchTrainable


@contextmanager
def distributed_checkpoint_dir(step, disable=False):
    """ContextManager for creating a distributed checkpoint.

    Only checkpoints a file on the "main" training actor, avoiding
    redundant work.

    Args:
        step (int): Used to label the checkpoint
        disable (bool): Disable for prototyping.

    Yields:
        path (str): A path to a directory. This path will be used
            again when invoking the training_function.
    Example:

    .. code-block:: python

        def train_func(config, checkpoint_dir):
            if checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                model_state_dict = torch.load(path)

            if epoch % 3 == 0:
                with distributed_checkpoint_dir(step=epoch) as checkpoint_dir:
                    path = os.path.join(checkpoint_dir, "checkpoint")
                    torch.save(model.state_dict(), path)
    """

    if torch.distributed.get_rank() == 0 and not disable:
        with tune.checkpoint_dir(step=step) as checkpoint_dir:
            yield checkpoint_dir
    else:
        path = tempfile.mkdtemp()
        yield path
        shutil.rmtree(path)


def _train_check_global(config, checkpoint_dir=None):
    """For testing only. Putting this here because Ray has problems
    serializing within the test file."""
    assert is_distributed_trainable()
    import time
    time.sleep(0.1)
    tune.report(is_distributed=True)


def _train_simple(config, checkpoint_dir=None):
    """For testing only. Putting this here because Ray has problems
    serializing within the test file."""
    import torch.nn as nn
    from torch.nn.parallel import DistributedDataParallel
    import torch.optim as optim
    # N is batch size; D_in is input dimension;
    # H is hidden dimension; D_out is output dimension.
    N, D_in, H, D_out = 8, 5, 5, 5

    # Create random Tensors to hold inputs and outputs
    x = torch.randn(N, D_in)
    y = torch.randn(N, D_out)
    loss_fn = nn.MSELoss()

    # Use the nn package to define our model and loss function.
    model = torch.nn.Sequential(
        torch.nn.Linear(D_in, H),
        torch.nn.ReLU(),
        torch.nn.Linear(H, D_out),
    )
    optimizer = optim.SGD(model.parameters(), lr=0.1)

    if checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
            model_state, optimizer_state = torch.load(f)

        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    model = DistributedDataParallel(model)

    for epoch in range(config.get("epochs", 10)):
        optimizer.zero_grad()
        output = model(x)
        loss = loss_fn(output, y)
        loss.backward()
        optimizer.step()

        if epoch % 3 == 0:
            if config.get("enable_checkpoint", True):
                with distributed_checkpoint_dir(step=epoch) as checkpoint_dir:
                    path = os.path.join(checkpoint_dir, "checkpoint")
                    torch.save((model.state_dict(), optimizer.state_dict()),
                               path)
        tune.report(mean_loss=loss.item())
