# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
import os
import logging
import torch
from datetime import timedelta

import ray
from ray import tune
from ray.tune.result import RESULT_DUPLICATE
from ray.tune.logger import NoopLogger
from ray.tune.function_runner import wrap_function
from ray.tune.resources import Resources
from ray.tune.trainable import TrainableUtil
from ray.util.sgd.torch.utils import setup_process_group
from ray.util.sgd.torch.constants import NCCL_TIMEOUT_S
from ray.util.sgd.torch.utils import setup_address

logger = logging.getLogger(__name__)


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

        address = setup_address()
        self.workers = [
            remote_trainable.remote(
                config=config,
                logger_creator=lambda cfg: logger_creator(cfg, logdir, rank))
            for rank in range(num_workers)
        ]

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

    Note that you typically should not instantiate the object
    created.

    Example:

    .. code-block::

        trainable_cls = DistributedTrainableCreator(
            train_func, num_workers=2)
        analysis = tune.run(trainable_cls)
    """

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


class distributed_checkpoint:
    """ContextManager for creating a distributed checkpoint.

    Only checkpoints a file on the "main" training actor, avoiding
    redundant work.

    Args:
        label (int | str): Used to label the checkpoint
        disable (bool): Disable for prototyping.

    Example:

    .. code-block::

        if epoch % 3 == 0:
            with distributed_checkpoint(label=epoch) as path:
                torch.save(model.state_dict(), path)
    """

    def __init__(self, label, disable=False):
        self.label = label
        self.file = None
        self.disable = disable

    def __enter__(self):
        if torch.distributed.get_rank() == 0 and not self.disable:
            checkpoint_dir = tune.make_checkpoint_dir(step=self.label)
            path = os.path.join(checkpoint_dir, "checkpoint")
        else:
            path = os.devnull
        self.file = path
        return path

    def __exit__(self, type, value, traceback):
        if torch.distributed.get_rank() == 0 and not self.disable:
            tune.save_checkpoint(self.file)


def _train_simple(config, checkpoint=False):
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

    if checkpoint:
        with open(checkpoint) as f:
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
                with distributed_checkpoint(label=epoch) as path:
                    torch.save((model.state_dict(), optimizer.state_dict()),
                               path)
        tune.report(mean_loss=loss.item())
