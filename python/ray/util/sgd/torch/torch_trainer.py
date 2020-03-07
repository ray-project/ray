import numpy as np
import os
import logging
import numbers
import tempfile
import time
import torch
import torch.distributed as dist

import ray

from ray.tune import Trainable
from ray.tune.trial import Resources
from ray.util.sgd.torch.distributed_torch_runner import (
    DistributedTorchRunner)
from ray.util.sgd import utils
from ray.util.sgd.utils import NUM_SAMPLES, BATCH_SIZE
from ray.util.sgd.torch.torch_runner import TorchRunner
from ray.util.sgd.torch.constants import VALID_SCHEDULER_STEP

logger = logging.getLogger(__name__)
RESIZE_COOLDOWN_S = 10


def _validate_scheduler_step_freq(scheduler_step_freq):
    if scheduler_step_freq:
        if scheduler_step_freq not in VALID_SCHEDULER_STEP:
            raise ValueError(
                "Scheduler step freq must be in {}. Got {}".format(
                    VALID_SCHEDULER_STEP, scheduler_step_freq))


class TorchTrainer:
    """Train a PyTorch model using distributed PyTorch.

    Launches a set of actors which connect via distributed PyTorch and
    coordinate gradient updates to train the provided model.

    .. code-block:: python

        def model_creator(config):
            return nn.Linear(1, 1)


        def optimizer_creator(model, config):
            return torch.optim.SGD(
                model.parameters(), lr=config.get("lr", 1e-4))


        def data_creator(config):
            return LinearDataset(2, 5), LinearDataset(2, 5, size=400)

        trainer = TorchTrainer(
            model_creator,
            data_creator,
            optimizer_creator,
            loss_creator=nn.MSELoss,
            use_gpu=True
        )
        for i in range(4):
            trainer.train()


    Args:
        model_creator (dict -> Model(s)): Constructor function that takes in
            config and returns the model(s) to be optimized. These must be
            ``torch.nn.Module`` objects. If multiple models are returned,
            a ``training_operator_cls`` must be specified. You do not need to
            handle GPU/devices in this function; RaySGD will do that under
            the hood.
        data_creator (dict -> Iterable(s)): Constructor function
            that takes in the passed config and returns one or
            two Iterable objects. Note that even though two Iterable objects
            can be returned, only one will be used for training, and the
            other will be used for validation. If not provided, you must
            provide a custom TrainingOperator.
        optimizer_creator ((models, dict) -> optimizers): Constructor
            function that takes in the return values from
            ``model_creator`` and the passed config and returns One or
            more Torch optimizer objects. You do not need to handle
            GPU/devices in this function; ``RaySGD`` will do that for you.
        loss_creator (torch.nn.*Loss class | dict -> loss): A constructor
            function for the training loss. This can be either a function that
            takes in the provided config for customization or a subclass
            of ``torch.nn.modules.loss._Loss``, which is most Pytorch
            loss classes. For example, ``loss_creator=torch.nn.BCELoss``.
            If not provided, you must provide a custom TrainingOperator.
        scheduler_creator ((optimizers, dict) -> scheduler):
            A constructor function for the torch scheduler. This is
            a function that takes in the generated optimizers (from
            ``optimizer_creator``) provided config for customization.
            Be sure to set ``scheduler_step_freq`` to increment the
            scheduler correctly.
        training_operator_cls (type): Custom training operator class
            that subclasses the TrainingOperator class. This class
            will be copied onto all remote workers and used to specify
            custom training and validation operations. Defaults to
            TrainingOperator.
        config (dict): Custom configuration value to be passed to
            all creator and operator constructors.
        num_workers (int): the number of workers used in distributed
            training. If 1, the worker will not be wrapped with
            DistributedDataParallel.
        use_gpu (bool): Sets resource allocation for workers to 1 GPU
            if true, and automatically moves both the model and optimizer
            to the available CUDA device.
        backend (string): backend used by distributed PyTorch. Currently
            support "nccl", "gloo", and "auto". If "auto", RaySGD will
            automatically use "nccl" if `use_gpu` is True, and "gloo"
            otherwise.
        use_fp16 (bool): Enables mixed precision training via apex if apex
            is installed. This is automatically done after the model and
            optimizers are constructed and will work for multi-model training.
            Please see https://github.com/NVIDIA/apex for more details.
        apex_args (dict|None): Dict containing keyword args for amp.initialize.
            See https://nvidia.github.io/apex/amp.html#module-apex.amp. By
            default, the models and optimizers are passed in. Consider using
            "num_losses" if operating over multiple models and optimizers.
        scheduler_step_freq: "batch", "epoch", or None. This will
            determine when ``scheduler.step`` is called. If "batch",
            ``step`` will be called after every optimizer step. If "epoch",
            ``step`` will be called after one pass of the DataLoader.

    """

    def __init__(self,
                 *,
                 model_creator=None,
                 data_creator=None,
                 optimizer_creator=None,
                 loss_creator=None,
                 scheduler_creator=None,
                 training_operator_cls=None,
                 initialization_hook=None,
                 config=None,
                 num_workers=1,
                 use_gpu=False,
                 backend="auto",
                 use_fp16=False,
                 apex_args=None,
                 scheduler_step_freq="batch"):
        if num_workers > 1 and not dist.is_available():
            raise ValueError(
                ("Distributed PyTorch is not supported on macOS. "
                 "To run without distributed PyTorch, set 'num_workers=1'. "
                 "For more information, see "
                 "https://github.com/pytorch/examples/issues/467."))

        if not (model_creator and optimizer_creator and data_creator):
            raise ValueError("Must provide a Model, Optimizer, Data creator.")
        self.model_creator = model_creator
        self.optimizer_creator = optimizer_creator
        self.loss_creator = loss_creator
        self.data_creator = data_creator
        self.scheduler_creator = scheduler_creator
        self.training_operator_cls = training_operator_cls

        if not training_operator_cls and not loss_creator:
            raise ValueError("If a loss_creator is not provided, you must "
                             "provide a custom training operator.")

        self.initialization_hook = initialization_hook
        self.config = {} if config is None else config

        if backend == "auto":
            backend = "nccl" if use_gpu else "gloo"

        logger.debug("Using {} as backend.".format(backend))
        self.backend = backend

        # TODO: Have an auto "use_gpu" option to detect and use GPUs.
        self.use_gpu = use_gpu
        self.max_replicas = num_workers

        self.use_fp16 = use_fp16

        if apex_args and not isinstance(apex_args, dict):
            raise ValueError("apex_args needs to be a dict object.")

        self.apex_args = apex_args
        self.temp_dir = tempfile.mkdtemp(prefix="raysgd")
        self._num_failures = 0
        self._last_resize = float("-inf")

        _validate_scheduler_step_freq(scheduler_step_freq)
        self.scheduler_step_freq = scheduler_step_freq

        self._start_workers(self.max_replicas)

    def _configure_and_split_batch(self, num_workers):
        """If sgd.utils.BATCH_SIZE is provided, split among workers."""
        if BATCH_SIZE not in self.config:
            return
        # Compute batch size per worker
        logger.debug("BATCH_SIZE parameter detected. Splitting among workers.")
        batch_size = self.config[BATCH_SIZE]
        batch_size_per_worker = batch_size // num_workers
        if batch_size % num_workers > 0:
            new_batch_size = batch_size_per_worker * num_workers
            logger.warning(
                ("Changing batch size from {old_batch_size} to "
                 "{new_batch_size} to evenly distribute batches across "
                 "{num_workers} workers.").format(
                     old_batch_size=batch_size,
                     new_batch_size=new_batch_size,
                     num_workers=num_workers))
            self.config[BATCH_SIZE] = new_batch_size
        return batch_size_per_worker

    def _start_workers(self, num_workers):
        logger.debug(f"start_workers: Setting %d workers." % num_workers)
        worker_config = self.config.copy()
        batch_size_per_worker = self._configure_and_split_batch(num_workers)
        if batch_size_per_worker:
            worker_config[BATCH_SIZE] = batch_size_per_worker
        if num_workers == 1:
            # Generate actor class
            Runner = ray.remote(
                num_cpus=1, num_gpus=int(self.use_gpu))(TorchRunner)
            # Start workers
            self.workers = [
                Runner.remote(
                    model_creator=self.model_creator,
                    data_creator=self.data_creator,
                    optimizer_creator=self.optimizer_creator,
                    loss_creator=self.loss_creator,
                    scheduler_creator=self.scheduler_creator,
                    training_operator_cls=self.training_operator_cls,
                    config=worker_config,
                    use_fp16=self.use_fp16,
                    apex_args=self.apex_args,
                    scheduler_step_freq=self.scheduler_step_freq,
                )
            ]
            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)
            # Get setup tasks in order to throw errors on failure
            ray.get(self.workers[0].setup.remote())
        else:
            # Generate actor class
            Runner = ray.remote(
                num_cpus=1, num_gpus=int(self.use_gpu))(DistributedTorchRunner)
            # Start workers
            self.workers = [
                Runner.remote(
                    model_creator=self.model_creator,
                    data_creator=self.data_creator,
                    optimizer_creator=self.optimizer_creator,
                    loss_creator=self.loss_creator,
                    scheduler_creator=self.scheduler_creator,
                    backend=self.backend,
                    training_operator_cls=self.training_operator_cls,
                    config=worker_config,
                    use_fp16=self.use_fp16,
                    apex_args=self.apex_args,
                    scheduler_step_freq=self.scheduler_step_freq)
                for i in range(num_workers)
            ]
            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)

            # Compute URL for initializing distributed PyTorch
            ip = ray.get(self.workers[0].get_node_ip.remote())
            port = ray.get(self.workers[0].find_free_port.remote())
            address = "tcp://{ip}:{port}".format(ip=ip, port=port)
            # Get setup tasks in order to throw errors on failure
            ray.get([
                worker.setup.remote(address, i, len(self.workers))
                for i, worker in enumerate(self.workers)
            ])

    def train(self,
              num_steps=None,
              profile=False,
              reduce_results=True,
              max_retries=0,
              checkpoint="auto",
              info=None):
        """Runs a training epoch.

        Calls `operator.train_epoch()` on N parallel workers simultaneously
        underneath the hood.

        Set `max_retries` to enable fault handling in case of
        instance preemption.

        Args:
            num_steps (int): Number of batches to compute update steps on.
                This corresponds also to the number of times
                ``TrainingOperator.train_batch`` is called.
            profile (bool): Returns time stats for the training procedure.
            reduce_results (bool): Whether to average all metrics across
                all workers into one dict. If a metric is a non-numerical
                value (or nested dictionaries), one value will be randomly
                selected among the workers. If False, returns a list of dicts.
            max_retries (int): Must be non-negative. If set to N, will
                kill all current workers, query the Ray global state for
                total available resources, and re-launch up to the
                available resources. Behavior is not well-defined
                in case of shared cluster usage.
            checkpoint (str): Path to checkpoint to restore from if retrying.
                If max_retries is set and ``checkpoint == "auto"``,
                TorchTrainer will save a checkpoint before starting to train.
            info (dict): Optional dictionary passed to the training
                operator for ``train_epoch`` and ``train_batch``.

        Returns:
            (dict | list) A dictionary of metrics for training.
                You can provide custom metrics by passing in a custom
                ``training_operator_cls``. If ``reduce_results=False``,
                this will return a list of metric dictionaries whose
                length will be equal to ``num_workers``.
        """
        assert max_retries >= 0, "`max_retries` must be non-negative."
        if max_retries:
            if checkpoint == "auto":
                logger.debug("Retrying detected. Automatically checkpointing.")
                checkpoint = self.save(
                    os.path.join(self.temp_dir, "tmp_checkpoint"))
            elif not checkpoint:
                raise ValueError("Cannot retry from empty checkpoint.")

        if checkpoint and self._should_resize():
            logger.info("Resize opportunity detected. Attempting to scale up.")
            self._resize_workers(checkpoint=checkpoint)

        success, worker_stats = self._train_epoch(
            num_steps=num_steps, profile=profile, info=info)
        # Fault handling
        for i in range(max_retries):
            if success:
                break
            else:
                self._num_failures += 1
            self._resize_workers(checkpoint=checkpoint)
            logger.info(
                "Retrying training step with %d workers." % len(self.workers))
            success, worker_stats = self._train_epoch(
                num_steps=num_steps, profile=profile, info=info)
        if not success:
            raise RuntimeError("Training run failed.")

        worker_stats = ray.get(worker_stats)
        if reduce_results:
            return self._process_stats(worker_stats)
        else:
            return worker_stats

    def _process_stats(self, worker_stats):
        stats = {
            NUM_SAMPLES: sum(
                stats.pop(NUM_SAMPLES, np.nan) for stats in worker_stats)
        }

        for stat_key in worker_stats[0]:
            if isinstance(worker_stats[0], numbers.Number):
                stats[stat_key] = np.nanmean(
                    [s.get(stat_key, np.nan) for s in worker_stats])
            else:
                stats[stat_key] = worker_stats[0][stat_key]
        return stats

    def _train_epoch(self, num_steps=None, profile=False, info=None):
        worker_stats = [
            w.train_epoch.remote(
                num_steps=num_steps, profile=profile, info=info)
            for w in self.workers
        ]
        success = utils.check_for_failure(worker_stats)
        return success, worker_stats

    def apply_all_workers(self, fn):
        """Run a function on all operators on the workers.

        Args:
            fn (Callable): A function that takes in no arguments.

        Returns:
            A list of objects returned by ``fn`` on each worker.

        """
        return ray.get([w.apply.remote(fn) for w in self.workers])

    def apply_all_operators(self, fn):
        """Run a function on all operators on the workers.

        Args:
            fn (Callable[TrainingOperator]): A function that takes in a
                TrainingOperator.

        Returns:
            A list of objects returned by ``fn`` on each operator.

        """
        return ray.get([w.apply_operator.remote(fn) for w in self.workers])

    def validate(self, num_steps=None, profile=False, info=None):
        """Evaluates the model on the validation data set.

        Args:
            num_steps (int): Number of batches to compute update steps on.
                This corresponds also to the number of times
                ``TrainingOperator.validate_batch`` is called.
            profile (bool): Returns time stats for the evaluation procedure.
            info (dict): Optional dictionary passed to the training
                operator for `validate` and `validate_batch`.

        Returns:
            A dictionary of metrics for validation.
                You can provide custom metrics by passing in a custom
                ``training_operator_cls``.
        """
        worker_stats = ray.get([
            w.validate.remote(num_steps=num_steps, profile=profile, info=info)
            for w in self.workers
        ])

        return self._process_stats(worker_stats)

    def update_scheduler(self, metric):
        """Calls ``scheduler.step(metric)`` on all schedulers.

        This is useful for lr_schedulers such as ``ReduceLROnPlateau``.
        """
        self.apply_all_operators(
            lambda op: [sched.step(metric) for sched in op.schedulers])

    def get_model(self):
        """Returns the learned model(s)."""
        models = self.model_creator(self.config)
        state = ray.get(self.workers[0].get_state.remote())
        if len(state["models"]) == 1:
            models.load_state_dict(state["models"][0])
        else:
            for model, state_dict in zip(models, state["models"]):
                model.load_state_dict(state_dict)
        return models

    def save(self, checkpoint):
        """Saves the model(s) to the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        Returns:
            checkpoint (str): Path to target checkpoint file.
        """
        state = ray.get(self.workers[0].get_state.remote())
        torch.save(state, checkpoint)
        return checkpoint

    def restore(self, checkpoint):
        """Restores the Trainer and all workers from the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.
        """
        state = torch.load(checkpoint)
        state_id = ray.put(state)
        ray.get([worker.set_state.remote(state_id) for worker in self.workers])

    def shutdown(self, force=False):
        """Shuts down workers and releases resources."""
        if not force:
            cleanup = [worker.shutdown.remote() for worker in self.workers]
            ray.get(cleanup)
            [worker.__ray_terminate__.remote() for worker in self.workers]
        else:
            for worker in self.workers:
                logger.warning("Killing worker {}.".format(worker))
                worker.__ray_kill__()

        self.workers = []

    def _resize_workers(self, checkpoint, max_retries=10):
        # check available resources
        self.shutdown(force=True)
        assert checkpoint, "Cannot restore without checkpoint."

        time.sleep(1)
        for i in range(max_retries):
            resources = ray.available_resources()
            new_workers = min(resources.get("CPU", 0), self.max_replicas)
            if self.use_gpu:
                new_workers = min(resources.get("GPU", 0), new_workers)
            if new_workers:
                self._last_resize = time.time()
                self._start_workers(int(new_workers))
                self.restore(checkpoint)
                return
            else:
                delay = 2**i
                logger.info("Resources: {}".format(resources))
                logger.warning(
                    "No new workers found. Retrying in %d sec." % delay)
                time.sleep(delay)
        raise RuntimeError("Exceeded max_retries for relaunching workers.")

    def _should_resize(self):
        """Returns True if past cooldown and exists resources to scale up."""
        worker_gap = self.max_replicas - len(self.workers)
        past_cooldown = (time.time() - self._last_resize) > RESIZE_COOLDOWN_S
        if past_cooldown and worker_gap:
            resources = ray.available_resources()
            potential_workers = min(resources.get("CPU", 0), self.max_replicas)
            if self.use_gpu:
                potential_workers = min(
                    resources.get("GPU", 0), potential_workers)
            return potential_workers > 0
        return False


class TorchTrainable(Trainable):
    @classmethod
    def default_resource_request(cls, config):
        return Resources(
            cpu=0,
            gpu=0,
            extra_cpu=config["num_workers"],
            extra_gpu=int(config["use_gpu"]) * config["num_workers"])

    def _setup(self, config):
        self._trainer = TorchTrainer(**config)

    def _train(self):
        train_stats = self._trainer.train()
        validation_stats = self._trainer.validate()

        train_stats.update(validation_stats)
        # output {"mean_loss": test_loss, "mean_accuracy": accuracy}
        return train_stats

    def _save(self, checkpoint_dir):
        return self._trainer.save(os.path.join(checkpoint_dir, "model.pth"))

    def _restore(self, checkpoint_path):
        return self._trainer.restore(checkpoint_path)

    def _stop(self):
        self._trainer.shutdown()
