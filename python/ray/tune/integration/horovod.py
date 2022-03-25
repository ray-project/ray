from typing import Callable, Dict, Type, Optional

from contextlib import contextmanager
import os
import logging
import shutil
import tempfile

from filelock import FileLock

import ray
from ray import tune
from ray.tune.function_runner import wrap_function
from ray.tune.logger import NoopLogger
from ray.tune.result import RESULT_DUPLICATE
from ray.tune.trainable import DistributedTrainable
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.tune.utils.trainable import TrainableUtil

from horovod.ray import RayExecutor

logger = logging.getLogger(__name__)


def get_rank() -> str:
    """Returns rank of worker."""
    return os.environ["HOROVOD_RANK"]


def logger_creator(log_config: Dict, logdir: str) -> NoopLogger:
    """Simple NOOP logger for worker trainables."""
    index = get_rank()
    worker_dir = os.path.join(logdir, "worker_{}".format(index))
    os.makedirs(worker_dir, exist_ok=True)
    return NoopLogger(log_config, worker_dir)


@contextmanager
def distributed_checkpoint_dir(step: int, disable: bool = False):
    """ContextManager for creating a distributed checkpoint.

    Only checkpoints a file on the "main" training actor, avoiding
    redundant work.

    Args:
        step: Used to label the checkpoint
        disable: Disable for prototyping.

    Yields:
        str: A path to a directory. This path will be used
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

    if int(get_rank()) == 0 and not disable:
        with tune.checkpoint_dir(step=step) as checkpoint_dir:
            yield checkpoint_dir
    else:
        path = tempfile.mkdtemp()
        yield path
        shutil.rmtree(path)


class _HorovodTrainable(DistributedTrainable):
    """Abstract Trainable class for Horovod."""

    # Callable function for training.
    _function = None
    # Number of workers to allocate per trial.
    _num_workers: Optional[int] = (None,)
    # Number of hosts (nodes) to allocate per trial
    _num_hosts: Optional[int] = (None,)
    # Number of CPU resources to reserve for each worker.
    _num_cpus_per_worker: int = 1
    # Whether to reserve and pass GPU resources through.
    _use_gpu: bool = False
    # bool: Whether a the function has completed training
    _finished: bool = False

    # Horovod settings
    _ssh_str: str = None
    _ssh_identity_file: str = None
    _timeout_s: int = 30

    @property
    def num_workers(self):
        return self._num_workers

    def setup(self, config: Dict):
        trainable = wrap_function(self.__class__._function)
        # We use a filelock here to ensure that the file-writing
        # process is safe across different trainables.
        if self._ssh_identity_file:
            with FileLock(self._ssh_identity_file + ".lock"):
                settings = RayExecutor.create_settings(
                    self._timeout_s, self._ssh_identity_file, self._ssh_str
                )
        else:
            settings = RayExecutor.create_settings(
                self._timeout_s, self._ssh_identity_file, self._ssh_str
            )

        self.executor = RayExecutor(
            settings,
            cpus_per_worker=self._num_cpus_per_worker,
            use_gpu=self._use_gpu,
            num_workers=self._num_workers,
        )

        new_config = DistributedTrainable.build_config(self, config)

        # We can't put `self` in the lambda closure, so we
        # resolve the variable ahead of time.
        logdir_ = str(self.logdir)

        # Starts the workers as specified by the resources above.
        self.executor.start(
            executable_cls=trainable,
            executable_kwargs={
                "config": new_config,
                "logger_creator": lambda cfg: logger_creator(cfg, logdir_),
            },
        )

    def step(self) -> Dict:
        if self._finished:
            raise RuntimeError("Training has already finished.")
        result = self.executor.execute(lambda w: w.step())[0]
        if RESULT_DUPLICATE in result:
            self._finished = True
        return result

    def save_checkpoint(self, checkpoint_dir: str) -> str:
        # TODO: optimize if colocated
        save_obj = self.executor.execute_single(lambda w: w.save_to_object())
        checkpoint_path = TrainableUtil.create_from_pickle(save_obj, checkpoint_dir)
        return checkpoint_path

    def load_checkpoint(self, checkpoint_dir: str):
        checkpoint_obj = TrainableUtil.checkpoint_to_object(checkpoint_dir)
        x_id = ray.put(checkpoint_obj)
        return self.executor.execute(lambda w: w.restore_from_object(ray.get(x_id)))

    def stop(self):
        self.executor.execute(lambda w: w.stop())
        self.executor.shutdown()


def DistributedTrainableCreator(
    func: Callable[[Dict], None],
    use_gpu: bool = False,
    num_hosts: Optional[int] = None,
    num_workers: int = 1,
    num_cpus_per_worker: int = 1,
    timeout_s: int = 30,
    replicate_pem: bool = False,
) -> Type[_HorovodTrainable]:
    """Converts Horovod functions to be executable by Tune.

    Requires horovod > 0.19 to work.

    This function wraps and sets the resources for a given Horovod
    function to be used with Tune. It generates a Horovod Trainable (trial)
    which can itself be a distributed training job. One basic assumption of
    this implementation is that all sub-workers
    of a trial will be placed evenly across different machines.

    It is recommended that if `num_hosts` per trial > 1, you set
    num_workers == the size (or number of GPUs) of a single host.
    If num_hosts == 1, then you can set num_workers to be <=
    the size (number of GPUs) of a single host.

    This above assumption can be relaxed - please file a feature request
    on Github to inform the maintainers.

    Another assumption is that this API requires gloo as the underlying
    communication primitive. You will need to install Horovod with
    `HOROVOD_WITH_GLOO` enabled.

    *Fault Tolerance:* The trial workers themselves are not fault tolerant.
    When a host of a trial fails, all workers of a trial are expected to
    die, and the trial is expected to restart. This currently does not
    support function checkpointing.

    Args:
        func: A training function that takes in
            a config dict for hyperparameters and should initialize
            horovod via horovod.init.
        use_gpu: Whether to allocate a GPU per worker.
        num_cpus_per_worker: Number of CPUs to request
            from Ray per worker.
        num_hosts: Number of hosts that each trial is expected
            to use.
        num_workers: Number of workers to start on each host.
        timeout_s: Seconds for Horovod rendezvous to timeout.
        replicate_pem: THIS MAY BE INSECURE. If true, this will
            replicate the underlying Ray cluster ssh key across all hosts.
            This may be useful if using the Ray Autoscaler.

    Returns:
        Trainable class that can be passed into `tune.run`.

    Example:

    .. code-block:: python

        def train(config):
            horovod.init()
            horovod.allreduce()

        from ray.tune.integration.horovod import DistributedTrainableCreator
        trainable_cls = DistributedTrainableCreator(
            train, num_hosts=1, num_workers=2, use_gpu=True)

        tune.run(trainable_cls)

    .. versionadded:: 1.0.0
    """
    ssh_identity_file = None
    sshkeystr = None

    if replicate_pem:
        from ray.tune.cluster_info import get_ssh_key

        ssh_identity_file = get_ssh_key()
        if os.path.exists(ssh_identity_file):
            # For now, we assume that you're on a Ray cluster.
            with open(ssh_identity_file) as f:
                sshkeystr = f.read()

    class WrappedHorovodTrainable(_HorovodTrainable):
        _function = func
        _num_hosts = num_hosts
        _num_workers = num_workers
        _num_cpus_per_worker = num_cpus_per_worker
        _use_gpu = use_gpu
        _ssh_identity_file = ssh_identity_file
        _ssh_str = sshkeystr
        _timeout_s = timeout_s

        @classmethod
        def default_resource_request(cls, config: Dict):
            return PlacementGroupFactory(
                [{}]
                + [{"CPU": cls._num_cpus_per_worker, "GPU": int(use_gpu)}]
                * (num_workers)
            )

    return WrappedHorovodTrainable


# pytest presents a bunch of serialization problems
# that force us to include mocks as part of the module.


def _train_simple(config: Dict):
    import horovod.torch as hvd

    hvd.init()
    from ray import tune

    for i in range(config.get("epochs", 2)):
        import time

        time.sleep(1)
        if config.get("enable_checkpoint", True):
            with distributed_checkpoint_dir(step=i) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                import pickle

                with open(path, "wb") as f:
                    pickle.dump("hi", f)
        tune.report(test=1, rank=hvd.rank())


def _train_validate_session(config: Dict):
    current_session = tune.session.get_session()
    assert current_session is not None
    assert current_session.trial_id != "default"
    assert current_session.trial_name != "default"
