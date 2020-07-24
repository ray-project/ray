# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
import os
import logging
import torch
from datetime import timedelta

import ray
from ray import tune
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
    _function = None
    _num_workers = None

    __slots__ = ["workers"]

    @classmethod
    def default_process_group_parameters(self):
        return dict(timeout=timedelta(NCCL_TIMEOUT_S), backend="gloo")

    def setup(self, config):
        num_workers = self._num_workers
        logdir = self.logdir
        assert self._function

        func_trainable = wrap_function(self.__class__._function)
        remote_trainable = ray.remote(func_trainable)
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
            address=address,
            num_workers=num_workers,
            **pgroup_params)
        ray.get([
            w.execute.remote(lambda _: setup_on_worker(rank=rank))
            for rank, w in enumerate(self.workers)
        ])

    def step(self):
        result = ray.get([w.step.remote() for w in self.workers])
        return result[0]

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
    class WrappedDistributedTorchTrainable(_TorchTrainable):
        _function = func
        _num_workers = num_workers

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
    """ContextManager for creating a checkpoint.

    Only checkpoints a file on the "main" training actor, avoiding
    redundant work.

    Args:
        label (int | str): Used to label the checkpoint

    Example:

    .. code-block::

        if epoch % 3 == 0:
            with distributed_checkpoint(label=epoch) as f:
                torch.save(model.state_dict(), f)
    """

    def __init__(self, label):
        self.label = label
        self.file = None

    def __enter__(self):
        if torch.distributed.get_rank() == 0:
            checkpoint_dir = tune.make_checkpoint_dir(step=self.label)
            path = os.path.join(checkpoint_dir, "checkpoint")
        else:
            path = "/dev/null"
        self.file = open(path, "wb")
        return self.file

    def __exit__(self, type, value, traceback):
        self.file.close()
        if torch.distributed.get_rank() == 0:
            tune.save_checkpoint(self.file.name)
