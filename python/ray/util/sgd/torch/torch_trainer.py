import numpy as np
import os
import logging
import numbers
import tempfile
import time
import torch
import torch.distributed as dist

import ray

from ray.exceptions import RayActorError
from ray.tune import Trainable
from ray.tune.trial import Resources
from ray.util.sgd.torch.distributed_torch_runner import (
    DistributedTorchRunner)
from ray.util.sgd import utils
from ray.util.sgd.torch.torch_runner import TorchRunner
from ray.util.sgd.torch.constants import VALID_SCHEDULER_STEP
from ray.util.sgd.torch.batch_logs_reporter import BatchLogsReporter

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
        data_creator (dict -> Dataset(s)): Constructor function
            that takes in the passed config and returns one or
            two ``torch.utils.data.Dataset`` objects.
            Note that even though two Dataset objects can be returned,
            only one dataset will be used for training. RaySGD
            will automatically wrap the objects with a ``DataLoader``.
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
        scheduler_creator (optimizers, dict -> loss):
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
        dataloader_config (dict): Configuration values to be passed into
            the ``torch.utils.data.DataLoader`` object that wraps
            the dataset on each parallel worker for both training
            and validation. Note that if ``num_replicas``
            is greater than 1, ``shuffle`` and ``sampler`` will be
            automatically set. See the available arguments
            here https://pytorch.org/docs/stable/data.html.
        num_replicas (int): the number of workers used in distributed
            training.
        use_gpu (bool): Sets resource allocation for workers to 1 GPU
            if true, and automatically moves both the model and optimizer
            to the available CUDA device.
        batch_size (int): Total batch size for each minibatch. This
            value is divided among all workers and rounded.
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
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 loss_creator,
                 scheduler_creator=None,
                 training_operator_cls=None,
                 initialization_hook=None,
                 config=None,
                 dataloader_config=None,
                 num_replicas=1,
                 use_gpu=False,
                 batch_size=16,
                 backend="auto",
                 use_fp16=False,
                 apex_args=None,
                 scheduler_step_freq="batch"):
        if num_replicas > 1 and not dist.is_available():
            raise ValueError(
                ("Distributed PyTorch is not supported on macOS. "
                 "To run without distributed PyTorch, set 'num_replicas=1'. "
                 "For more information, see "
                 "https://github.com/pytorch/examples/issues/467."))

        self.model_creator = model_creator
        self.data_creator = data_creator
        self.optimizer_creator = optimizer_creator
        self.loss_creator = loss_creator
        self.scheduler_creator = scheduler_creator
        self.training_operator_cls = training_operator_cls
        self.initialization_hook = initialization_hook
        self.config = {} if config is None else config
        self.dataloader_config = dataloader_config
        self.optimizer_timer = utils.TimerStat(window_size=1)

        if backend == "auto":
            backend = "nccl" if use_gpu else "gloo"

        logger.info("Using {} as backend.".format(backend))
        self.backend = backend

        # TODO: Have an auto "use_gpu" option to detect and use GPUs.
        self.use_gpu = use_gpu
        self.batch_size = batch_size
        self.max_replicas = num_replicas

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

    def _start_workers(self, num_replicas):
        logger.info(f"start_workers: Setting %d replicas." % num_replicas)
        if num_replicas == 1:
            # Generate actor classes
            Runner = ray.remote(
                num_cpus=1, num_gpus=int(self.use_gpu))(TorchRunner)
            # Start workers
            self.workers = [
                Runner.remote(
                    self.model_creator,
                    self.data_creator,
                    self.optimizer_creator,
                    self.loss_creator,
                    self.scheduler_creator,
                    training_operator_cls=self.training_operator_cls,
                    config=self.config,
                    dataloader_config=self.dataloader_config,
                    batch_size=self.batch_size,
                    use_fp16=self.use_fp16,
                    apex_args=self.apex_args,
                    scheduler_step_freq=self.scheduler_step_freq,
                )
            ]
            self.batch_reporters = [
                BatchLogsReporter.remote(0)
            ]
            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)
            # Get setup tasks in order to throw errors on failure
            ray.get(self.workers[0].setup.remote(self.batch_reporters[0]))
        else:
            # Generate actor class
            Runner = ray.remote(
                num_cpus=1, num_gpus=int(self.use_gpu))(DistributedTorchRunner)
            # Compute batch size per replica
            batch_size_per_replica = self.batch_size // num_replicas
            if self.batch_size % num_replicas > 0:
                new_batch_size = batch_size_per_replica * num_replicas
                logger.warning(
                    ("Changing batch size from {old_batch_size} to "
                     "{new_batch_size} to evenly distribute batches across "
                     "{num_replicas} replicas.").format(
                         old_batch_size=self.batch_size,
                         new_batch_size=new_batch_size,
                         num_replicas=num_replicas))
            # Start workers
            self.workers = [
                Runner.remote(
                    self.model_creator,
                    self.data_creator,
                    self.optimizer_creator,
                    self.loss_creator,
                    self.scheduler_creator,
                    backend=self.backend,
                    training_operator_cls=self.training_operator_cls,
                    config=self.config,
                    dataloader_config=self.dataloader_config,
                    batch_size=batch_size_per_replica,
                    use_fp16=self.use_fp16,
                    apex_args=self.apex_args,
                    scheduler_step_freq=self.scheduler_step_freq)
                for i in range(num_replicas)
            ]
            self.batch_reporters = [
                BatchLogsReporter.remote(i)
                for i in range(num_replicas)
            ]
            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)

            # Compute URL for initializing distributed PyTorch
            ip = ray.get(self.workers[0].get_node_ip.remote())
            port = ray.get(self.workers[0].find_free_port.remote())
            address = "tcp://{ip}:{port}".format(ip=ip, port=port)
            # Get setup tasks in order to throw errors on failure
            ray.get([
                worker.setup.remote(address, i, len(self.workers), self.batch_reporters[i])
                for i, worker in enumerate(self.workers)
            ])

    def train(self,
              num_steps=None,
              max_retries=0,
              checkpoint="auto",
              info=None,
              batch_logs_handler=None):
        """Runs a training epoch.

        Runs an average over all values returned from workers. Set
        `max_retries` to enable fault handling in case of instance preemption.

        Args:
            num_steps (int): Number of batches to compute update steps on.
                This corresponds also to the number of times
                ``TrainingOperator.train_batch`` is called.
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
            A dictionary of metrics for training.
                You can provide custom metrics by passing in a custom
                ``training_operator_cls``.
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

        with self.optimizer_timer:
            success, worker_stats = self._train_epoch(
                num_steps=num_steps, info=info, batch_logs_handler=batch_logs_handler)
            # Fault handling
            for i in range(max_retries):
                if success:
                    break
                else:
                    self._num_failures += 1
                self._resize_workers(checkpoint=checkpoint)
                logger.info("Retrying training step with %d workers." % len(
                    self.workers))
                success, worker_stats = self._train_epoch(
                    num_steps=num_steps, info=info, batch_logs_handler=batch_logs_handler)
        if not success:
            raise RuntimeError("Training run failed.")

        worker_stats = ray.get(worker_stats)

        train_stats = {}
        for stat_key in worker_stats[0]:
            if isinstance(worker_stats[0], numbers.Number):
                train_stats[stat_key] = np.nanmean(
                    [s.get(stat_key, np.nan) for s in worker_stats])
            else:
                train_stats[stat_key] = [s[stat_key] for s in worker_stats]
        return train_stats

    def _train_epoch(self, num_steps=None, info=None, batch_logs_handler=None):
        batch_log_reads = [
            r._read.remote()
            for r in self.batch_reporters
        ]
        worker_trains = [
            w.train_epoch.remote(num_steps=num_steps, info=info)
            for w in self.workers
        ]

        unfinished_trains_n = len(worker_trains)
        unfinished = worker_trains + batch_log_reads
        try:
            while unfinished_trains_n > 0:
                finished, unfinished = ray.wait(unfinished)

                finished_trains = []
                finished_log_reads = []

                # todo: is this expensive?
                for f in finished:
                    if f in worker_trains:
                        finished_trains.append(f)
                        unfinished_trains_n -= 1
                    else:
                        finished_log_reads.append(f)

                # todo: why do we ray.get the train calls?
                finished_trains = ray.get(finished_trains)
                finished_log_reads = ray.get(finished_log_reads)

                finish_reads = []
                for log_read in finished_log_reads:
                    r = self.batch_reporters[log_read["world_rank"]]
                    finish_reads.append(r._finish_read.remote())

                    if log_read["done"]:
                        continue

                    if batch_logs_handler is not None:
                        batch_logs_handler(log_read["data"])

                    unfinished.append(r._read.remote())

                ray.get(finish_reads)

            ray.get(unfinished) # and ignore all the dones

            return True, worker_trains
        except RayActorError as exc:
            logger.exception(str(exc))
        return False, worker_trains

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

    def validate(self, num_steps=None, info=None):
        """Evaluates the model on the validation data set.

        Args:
            num_steps (int): Number of batches to compute update steps on.
                This corresponds also to the number of times
                ``TrainingOperator.validate_batch`` is called.
            info (dict): Optional dictionary passed to the training
                operator for `validate` and `validate_batch`.

        Returns:
            A dictionary of metrics for validation.
                You can provide custom metrics by passing in a custom
                ``training_operator_cls``.
        """
        worker_stats = ray.get([
            w.validate.remote(num_steps=num_steps, info=info)
            for w in self.workers
        ])

        validation_stats = {}
        for stat_key in worker_stats[0]:
            validation_stats[stat_key] = np.nanmean(
                [s.get(stat_key, np.nan) for s in worker_stats])
        return validation_stats

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
            extra_cpu=config["num_replicas"],
            extra_gpu=int(config["use_gpu"]) * config["num_replicas"])

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
