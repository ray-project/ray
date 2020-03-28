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
    DistributedTorchRunner, LocalDistributedRunner)
from ray.util.sgd.utils import check_for_failure, NUM_SAMPLES, BATCH_SIZE
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

        ray.init()

        def model_creator(config):
            return nn.Linear(1, 1)


        def optimizer_creator(model, config):
            return torch.optim.SGD(
                model.parameters(), lr=config.get("lr", 1e-4))


        def data_creator(config):
            batch_size = config["batch_size"]
            train_data, val_data = LinearDataset(2, 5), LinearDataset(2, 5)
            train_loader = DataLoader(train_data, batch_size=batch_size)
            val_loader = DataLoader(val_data, batch_size=batch_size)
            return train_loader, val_loader


        trainer = TorchTrainer(
            model_creator=model_creator,
            data_creator=data_creator,
            optimizer_creator=optimizer_creator,
            loss_creator=nn.MSELoss,
            config={"batch_size": 32},
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

    def __init__(
            self,
            *,
            model_creator,
            data_creator,
            optimizer_creator,
            loss_creator=None,
            scheduler_creator=None,
            training_operator_cls=None,
            initialization_hook=None,
            config=None,
            num_workers=1,
            use_gpu=False,
            backend="auto",
            use_fp16=False,
            use_tqdm=False,
            apex_args=None,
            scheduler_step_freq="batch",
            num_replicas=None,
            batch_size=None,
            data_loader_args=None,
    ):
        if num_workers > 1 and not dist.is_available():
            raise ValueError(
                ("Distributed PyTorch is not supported on macOS. "
                 "To run without distributed PyTorch, set 'num_workers=1'. "
                 "For more information, see "
                 "https://github.com/pytorch/examples/issues/467."))

        if not (callable(model_creator) and callable(optimizer_creator)
                and callable(data_creator)):
            raise ValueError(
                "Must provide a callable model_creator, optimizer_creator, "
                "and data_creator.")

        if num_replicas is not None:
            raise DeprecationWarning(
                "num_replicas is deprecated. Use num_workers instead.")

        if batch_size is not None:
            raise DeprecationWarning(
                "batch_size is deprecated. Use config={'batch_size': N} "
                "specify a batch size for each worker or "
                "config={ray.util.sgd.utils.BATCH_SIZE: N} to specify a "
                "batch size to be used across all workers.")

        if data_loader_args:
            raise ValueError(
                "data_loader_args is deprecated. You can return a "
                "torch.utils.data.DataLoader in data_creator. Ray will "
                "automatically set a DistributedSampler if a DataLoader is "
                "returned and num_workers > 1.")

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
        self.use_tqdm = use_tqdm

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

        self.local_worker = None
        self.remote_workers = []

        if num_workers == 1:
            # Start local worker
            self.local_worker = TorchRunner(
                model_creator=self.model_creator,
                data_creator=self.data_creator,
                optimizer_creator=self.optimizer_creator,
                loss_creator=self.loss_creator,
                scheduler_creator=self.scheduler_creator,
                training_operator_cls=self.training_operator_cls,
                config=worker_config,
                use_fp16=self.use_fp16,
                use_tqdm=self.use_tqdm,
                apex_args=self.apex_args,
                scheduler_step_freq=self.scheduler_step_freq)

            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)

            self.local_worker.setup()
        else:
            params = dict(
                model_creator=self.model_creator,
                data_creator=self.data_creator,
                optimizer_creator=self.optimizer_creator,
                loss_creator=self.loss_creator,
                scheduler_creator=self.scheduler_creator,
                backend=self.backend,
                training_operator_cls=self.training_operator_cls,
                config=worker_config,
                use_fp16=self.use_fp16,
                use_tqdm=self.use_tqdm,
                apex_args=self.apex_args,
                scheduler_step_freq=self.scheduler_step_freq)

            # Start local worker
            self.local_worker = LocalDistributedRunner(
                num_cpus=1, num_gpus=int(self.use_gpu), **params)

            # Generate actor class
            RemoteRunner = ray.remote(
                num_cpus=1, num_gpus=int(self.use_gpu))(DistributedTorchRunner)
            # Start workers
            self.remote_workers = [
                RemoteRunner.remote(**params) for i in range(num_workers - 1)
            ]
            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)

            # Compute URL for initializing distributed PyTorch
            ip = ray.services.get_node_ip_address()
            port = self.local_worker.find_free_port()

            address = "tcp://{ip}:{port}".format(ip=ip, port=port)

            remote_setups = [
                worker.setup.remote(address, i + 1, num_workers)
                for i, worker in enumerate(self.remote_workers)
            ]
            self.local_worker.setup(address, 0, num_workers)
            # Get setup tasks in order to throw errors on failure
            ray.get(remote_setups)

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
            logger.info("Retrying training step with %d workers." %
                        (len(self.remote_workers) + 1))
            success, worker_stats = self._train_epoch(
                num_steps=num_steps, profile=profile, info=info)
        if not success:
            raise RuntimeError("Training run failed.")

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
        params = dict(num_steps=num_steps, profile=profile, info=info)

        remote_worker_stats = [
            w.train_epoch.remote(**params) for w in self.remote_workers
        ]

        try:
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

    def apply_all_workers(self, fn):
        """Run a function on all operators on the workers.

        Args:
            fn (Callable): A function that takes in no arguments.

        Returns:
            A list of objects returned by ``fn`` on each worker.

        """
        remote_calls = [w.apply.remote(fn) for w in self.remote_workers]
        local_call = self.local_worker.apply(fn)
        return [local_call] + ray.get(remote_calls)

    def apply_all_operators(self, fn):
        """Run a function on all operators on the workers.

        Args:
            fn (Callable[TrainingOperator]): A function that takes in a
                TrainingOperator.

        Returns:
            A list of objects returned by ``fn`` on each operator.

        """
        remote_calls = [
            w.apply_operator.remote(fn) for w in self.remote_workers
        ]
        local_call = self.local_worker.apply_operator(fn)
        return [local_call] + ray.get(remote_calls)

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
        params = dict(num_steps=num_steps, profile=profile, info=info)

        remote_worker_stats = [
            w.validate.remote(**params) for w in self.remote_workers
        ]
        local_worker_stats = self.local_worker.validate(**params)

        return self._process_stats([local_worker_stats] +
                                   ray.get(remote_worker_stats))

    def update_scheduler(self, metric):
        """Calls ``scheduler.step(metric)`` on all schedulers.

        This is useful for lr_schedulers such as ``ReduceLROnPlateau``.
        """
        self.apply_all_operators(
            lambda op: [sched.step(metric) for sched in op.schedulers])

    def get_model(self):
        """Returns the learned model(s)."""
        models = self.model_creator(self.config)
        state = self.local_worker.get_state()
        if len(state["models"]) == 1:
            models.load_state_dict(state["models"][0])
        else:
            for model, state_dict in zip(models, state["models"]):
                model.load_state_dict(state_dict)
        return models

    def state_dict(self):
        return self.local_worker.get_state()

    def load_state_dict(self, state):
        state_id = ray.put(state)

        remote_calls = [
            worker.set_state.remote(state_id) for worker in self.remote_workers
        ]
        self.local_worker.set_state(state)
        ray.get(remote_calls)

    def save(self, checkpoint):
        """Saves the model(s) to the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        Returns:
            checkpoint (str): Path to target checkpoint file.
        """
        torch.save(self.state_dict(), checkpoint)
        return checkpoint

    def restore(self, checkpoint):
        """Restores the Trainer and all workers from the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.
        """
        state = torch.load(checkpoint)
        self.load_state_dict(state)

    def shutdown(self, force=False):
        """Shuts down workers and releases resources."""
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
                    logger.warning("Killing worker {}.".format(worker))
                    ray.kill(worker)
        else:
            self.local_worker.shutdown()
            for worker in self.remote_workers:
                logger.warning("Killing worker {}.".format(worker))
                ray.kill(worker)

        self.local_worker = None
        self.remote_workers = []

    def _reset(self):
        """Terminates models without giving up local resource reservation."""
        self.local_worker.shutdown(cleanup=False)
        for worker in self.remote_workers:
            logger.warning("Killing worker {}.".format(worker))
            ray.kill(worker)
        self.local_worker = None
        self.remote_workers = []

    def _check_potential_remote_workers_size(self):
        # ASSUME 1 GPU + 1 CPU is already reserved for the local worker
        remote_resources = ray.available_resources()
        max_remote_workers = self.max_replicas - 1
        new_remote_workers = min(
            remote_resources.get("CPU", 0), max_remote_workers)
        if self.use_gpu:
            new_remote_workers = min(
                remote_resources.get("GPU", 0), new_remote_workers)
        return new_remote_workers

    def _resize_workers(self, checkpoint, max_retries=10):
        self._reset()
        assert checkpoint, "Cannot restore without checkpoint."

        time.sleep(1)
        for i in range(max_retries):
            new_remote_workers = self._check_potential_remote_workers_size()
            if new_remote_workers:
                self._last_resize = time.time()
                self._start_workers(int(new_remote_workers) + 1)
                self.restore(checkpoint)
                return
            else:
                delay = 2**i
                logger.warning(
                    "No new workers found. Retrying in %d sec." % delay)
                time.sleep(delay)
        raise RuntimeError("Exceeded max_retries for relaunching workers.")

    def _should_resize(self):
        """Returns True if past cooldown and exists resources to scale up."""
        worker_gap = self.max_replicas - 1 - len(self.remote_workers)
        past_cooldown = (time.time() - self._last_resize) > RESIZE_COOLDOWN_S
        if past_cooldown and worker_gap:
            # Assume 1 resource is already reserved for local worker.
            potential_remote_size = self._check_potential_remote_workers_size()
            return potential_remote_size > 0
        return False


class TorchTrainable(Trainable):
    @classmethod
    def default_resource_request(cls, config):
        remote_worker_count = config["num_workers"] - 1
        return Resources(
            cpu=1,
            gpu=int(config["use_gpu"]),
            extra_cpu=int(remote_worker_count),
            extra_gpu=int(int(config["use_gpu"]) * remote_worker_count))

    def _setup(self, config):
        self._trainer = TorchTrainer(**config)

    def _train(self):
        train_stats = self._trainer.train()
        validation_stats = self._trainer.validate()

        train_stats.update(validation_stats)
        return train_stats

    def _save(self, checkpoint_dir):
        return self._trainer.save(os.path.join(checkpoint_dir, "model.pth"))

    def _restore(self, checkpoint_path):
        return self._trainer.restore(checkpoint_path)

    def _stop(self):
        self._trainer.shutdown()
