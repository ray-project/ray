import json
import logging

import ray
import os
from ray import tune
from ray.tune.result import RESULT_DUPLICATE
from ray.tune.function_runner import wrap_function
from ray.tune.resources import Resources
from ray.util.sgd.utils import find_free_port
from ray.util.placement_group import remove_placement_group
from ray.tune.utils.trainable import PlacementGroupUtil, TrainableUtil
from ray.tune.utils.util import detect_checkpoint_function
from typing import Callable, Dict, Type, Optional

logger = logging.getLogger(__name__)


def setup_process_group(worker_addresses, index):
    """Set up distributed training info for training task.

    Args:
        worker_addresses (list): addresses of the workers.
        index (int): index of current worker
    """
    tf_config = {
        "cluster": {
            "worker": worker_addresses
        },
        "task": {
            "type": "worker",
            "index": index
        }
    }
    os.environ["TF_CONFIG"] = json.dumps(tf_config)


def setup_address():
    ip = ray.util.get_node_ip_address()
    port = find_free_port()
    return f"{ip}:{port}"


class _TensorFlowTrainable(tune.Trainable):
    """Base class for distributed training on Tune."""
    _function = None
    _num_workers = None
    _num_cpus_per_worker = None
    _num_gpus_per_worker = None
    _num_workers_per_host = None
    _placement_group = None
    _timeout_s = None

    __slots__ = ["workers", "_finished"]

    @property
    def should_colocate(self) -> bool:
        return self._num_workers_per_host is not None

    def setup(self, config: Dict):
        self._finished = False
        num_workers = self._num_workers
        assert self._function

        func_trainable = wrap_function(self.__class__._function)
        remote_trainable = ray.remote(func_trainable)
        remote_option, self._placement_group =\
            PlacementGroupUtil.get_remote_worker_options(
                self._num_workers, self._num_cpus_per_worker,
                self._num_gpus_per_worker,
                self._num_workers_per_host, self._timeout_s)
        remote_trainable = \
            remote_trainable.options(**remote_option)
        self.workers = [
            remote_trainable.remote(config=config, )
            for _ in range(num_workers)
        ]

        addresses = [
            ray.get(worker.execute.remote(lambda _: setup_address()))
            for worker in self.workers
        ]

        from functools import partial
        setup_on_worker = partial(
            setup_process_group, worker_addresses=addresses)
        ray.get([
            w.execute.remote(lambda _: setup_on_worker(index=index))
            for index, w in enumerate(self.workers)
        ])

    def step(self) -> Dict:
        if self._finished:
            raise RuntimeError("Training has already finished.")
        result = ray.get([w.step.remote() for w in self.workers])[0]
        if RESULT_DUPLICATE in result:
            self._finished = True
        return result

    def save_checkpoint(self, checkpoint_dir: str) -> str:
        # TODO: optimize if colocated
        save_obj = ray.get(self.workers[0].save_to_object.remote())
        checkpoint_path = TrainableUtil.create_from_pickle(
            save_obj, checkpoint_dir)
        return checkpoint_path

    def load_checkpoint(self, checkpoint_dir: str):
        checkpoint_obj = TrainableUtil.checkpoint_to_object(checkpoint_dir)
        return ray.get(
            w.restore_from_object.remote(checkpoint_obj) for w in self.workers)

    def stop(self):
        ray.get([worker.stop.remote() for worker in self.workers])
        if self.should_colocate:
            remove_placement_group(self._placement_group)


def DistributedTrainableCreator(
        func: Callable,
        num_workers: int = 2,
        num_gpus_per_worker: int = 0,
        num_cpus_per_worker: int = 1,
        num_workers_per_host: Optional[int] = None,
        timeout_s: int = 15 * 60) -> Type[_TensorFlowTrainable]:
    """Converts TensorFlow MultiWorkerMirror training to be executable by Tune.

    Requires TensorFlow > 2.0 to work, recommends TensorFlow > 2.2.

    This function wraps and sets resources for a TF distributed training
    function to be used with Tune. It generates a TensorFlow Trainable
    which can be a distributed training job.

    Note: there is no fault tolerance at the moment.

    Args:
        func (Callable[[dict], None]): A training function that takes in
            a config dict for hyperparameters and should initialize
            horovod via horovod.init.
        num_gpus_per_worker (int); Number of GPUs to request
            from Ray per worker.
        num_cpus_per_worker (int): Number of CPUs to request
            from Ray per worker.
        num_workers (int): Number of hosts that each trial is expected
            to use.
        num_workers_per_host (Optional[int]): Number of workers to
            colocate per host. None if not specified.
        timeout_s (float): Seconds before triggering placement timeouts
            if forcing colocation. Default to 15 minutes.


    Returns:
        Trainable class that can be passed into `tune.run`.

    .. versionadded:: 1.1.0

    Example:

    .. code-block:: python

        # Please refer to full example in tf_distributed_keras_example.py
        tf_trainable = DistributedTrainableCreator(
            train_mnist,
            num_workers=2)
        tune.run(tf_trainable,
                 num_samples=1)
    """
    detect_checkpoint_function(func, abort=True)
    if num_workers_per_host:
        if num_workers % num_workers_per_host:
            raise ValueError("`num_workers` must be an integer multiple "
                             f"of num_workers_per_host. Got: "
                             f"num_workers: {num_workers}, "
                             f"num_workers_per_host: {num_workers_per_host}")

    class WrappedDistributedTensorFlowTrainable(_TensorFlowTrainable):
        _function = func
        _num_workers = num_workers
        _num_cpus_per_worker = num_cpus_per_worker
        _num_workers_per_host = num_workers_per_host
        _num_gpus_per_worker = num_gpus_per_worker
        _timeout_s = timeout_s

        @classmethod
        def default_resource_request(cls, config: Dict) -> Resources:
            return Resources(
                cpu=0,
                gpu=0,
                extra_cpu=num_workers * num_cpus_per_worker,
                extra_gpu=num_workers * num_gpus_per_worker)

    return WrappedDistributedTensorFlowTrainable


def get_num_workers():
    """Retrieve the number of workers in the training job."""
    tf_config = json.loads(os.environ["TF_CONFIG"])
    num_workers = len(tf_config["cluster"]["worker"])
    return num_workers
