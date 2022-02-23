import inspect
import time

import numpy as np
import logging
import os
import numbers
import tempfile
import torch
import torch.distributed as dist

import ray
from ray.util import log_once
from ray.util.annotations import Deprecated
from ray.util.sgd.torch.worker_group import (
    LocalWorkerGroup,
    RemoteWorkerGroup,
    DeactivatedWorkerGroup,
)
from ray.util.sgd.utils import NUM_SAMPLES, BATCH_SIZE
from ray.util.sgd.torch.constants import VALID_SCHEDULER_STEP, NCCL_TIMEOUT_S
from ray.util.sgd.data import Dataset

try:
    from ray.tune import Trainable
    from ray.tune import PlacementGroupFactory
    from ray.tune.utils.util import merge_dicts

    TUNE_INSTALLED = True
except ImportError:
    TUNE_INSTALLED = False
    Trainable = PlacementGroupFactory = object

    def noop():
        return

    merge_dicts = noop

logger = logging.getLogger(__name__)


def _validate_scheduler_step_freq(scheduler_step_freq):
    """This validation check only happens if a scheduler is passed in."""
    if scheduler_step_freq not in VALID_SCHEDULER_STEP:
        raise ValueError(
            "Scheduler step freq must be in {}. Got {}".format(
                VALID_SCHEDULER_STEP, scheduler_step_freq
            )
        )


def _remind_gpu_usage(use_gpu):
    if not use_gpu and torch.cuda.is_available():
        logger.info(
            "GPUs detected but not using them. Set `use_gpu` to " "enable GPU usage. "
        )


@Deprecated
class TorchTrainer:
    """Train a PyTorch model using distributed PyTorch.

    Launches a set of actors which connect via distributed PyTorch and
    coordinate gradient updates to train the provided model. If Ray is not
    initialized, TorchTrainer will automatically initialize a local Ray
    cluster for you. Be sure to run `ray.init(address="auto")` to leverage
    multi-node training.

    .. code-block:: python

        class MyTrainingOperator(TrainingOperator):

            def setup(self, config):
                model = nn.Linear(1, 1)
                optimizer = torch.optim.SGD(
                    model.parameters(), lr=config.get("lr", 1e-4))
                loss = torch.nn.MSELoss()

                batch_size = config["batch_size"]
                train_data, val_data = LinearDataset(2, 5), LinearDataset(2, 5)
                train_loader = DataLoader(train_data, batch_size=batch_size)
                val_loader = DataLoader(val_data, batch_size=batch_size)

                self.model, self.optimizer = self.register(
                    models=model,
                    optimizers=optimizer,
                    criterion=loss)

                self.register_data(
                    train_loader=train_loader,
                    validation_loader=val_loader)


        trainer = TorchTrainer(
            training_operator_cls=MyTrainingOperator,
            config={"batch_size": 32},
            use_gpu=True
        )
        for i in range(4):
            trainer.train()

    Args:
        training_operator_cls (type): Custom training operator class
            that subclasses the TrainingOperator class. This class
            will be copied onto all remote workers and used to specify
            training components and custom training and validation operations.
        initialization_hook (function): A function to call on all training
            workers when they are first initialized. This could be useful to
            set environment variables for all the worker processes.
        config (dict): Custom configuration value to be passed to
            all operator constructors.
        num_workers (int): the number of workers used in distributed
            training. If 1, the worker will not be wrapped with
            DistributedDataParallel. TorchTrainer will scale down the number
            of workers if enough resources are not available, and will scale
            back up once they are. The total number of
            workers will never exceed `num_workers` amount.
        num_cpus_per_worker (int): Sets the cpu requirement for each worker.
        use_gpu (bool): Sets resource allocation for workers to 1 GPU
            if true, and automatically moves both the model and optimizer
            to the available CUDA device.
        backend (string): backend used by distributed PyTorch. Currently
            support "nccl", "gloo", and "auto". If "auto", RaySGD will
            automatically use "nccl" if `use_gpu` is True, and "gloo"
            otherwise.
        wrap_ddp (bool): Whether to automatically wrap DistributedDataParallel
            over each model. If False, you are expected to call it yourself.
        timeout_s (float): Seconds before the torch process group
            times out. Useful when machines are unreliable. If not set, default
            to 30 min, which is the same default as
            ``torch.init_process_group(...)``.
        add_dist_sampler (bool): Whether to automatically add a
            DistributedSampler to all created dataloaders. Only applicable
            if num_workers > 1.
        use_fp16 (bool|string): Enables mixed precision training.
            If set to True, will first try to use native mixed
            precision training backend, and if it's unavailable, the Apex
            backed, if installed. Apex backend must be installed separately
            and can be forced over the native backend by setting `use_fp16`
            to "apex". Torch documentation recommends the usage of the native
            backend. Mixed precision training is automatically done after
            the model and optimizers are constructed and will work for
            multi-model training. Defaults to False.
        scheduler_step_freq: "batch", "epoch", "manual", or None. This will
            determine when ``scheduler.step`` is called. If "batch",
            ``step`` will be called after every optimizer step. If "epoch",
            ``step`` will be called after one pass of the DataLoader. If
            "manual", the scheduler will not be incremented automatically -
            you are expected to call ``trainer.update_scheduler`` manually.
            If a scheduler is passed in, this value is expected to not be None.
        use_local (bool): If True, 1 worker will be a local worker running
            on the driver process, and all other workers will be remote. If
            False, all workers will be remote. Set this to True for easy
            debugging of worker on driver process, but could also
            lead to issues with Cuda devices. Defaults to False.
    """

    # TODO: Implement autoscaling. If num_workers=-1, the trainer will use as
    # many resources as available. Upon each train call, TorchTrainer will
    # query the Ray global state for total available resources and resize
    # its remote workers to consume all available resources.

    def __init__(
        self,
        *,
        training_operator_cls,
        initialization_hook=None,
        config=None,
        num_workers=1,
        num_cpus_per_worker=1,
        use_gpu="auto",
        backend="auto",
        wrap_ddp=True,
        timeout_s=1800,
        use_fp16=False,
        use_tqdm=False,
        add_dist_sampler=True,
        scheduler_step_freq=None,
        use_local=False,
        # Deprecated Args.
        num_replicas=None,
        batch_size=None,
        model_creator=None,
        data_creator=None,
        optimizer_creator=None,
        scheduler_creator=None,
        loss_creator=None,
        serialize_data_creation=None,
        data_loader_args=None,
        apex_args=None,
    ):
        if num_workers <= 0:
            raise ValueError(
                "The number of workers must be greater than 0. "
                f"Received num_workers={num_workers}"
            )

        if (
            model_creator
            or data_creator
            or optimizer_creator
            or scheduler_creator
            or loss_creator
        ):
            raise DeprecationWarning(
                "Creator functions are deprecated. You should create a "
                "custom TrainingOperator, override setup, and register all "
                "training state there. See TrainingOperator for more info. "
                "If you would still like to use creator functions, you can "
                "do CustomOperator = TrainingOperator.from_creators("
                "model_creator, ...) and pass in CustomOperator into "
                "TorchTrainer."
            )

        if use_local and ray.util.client.ray.is_connected():
            raise ValueError("use_local setting is not supported with Ray " "Client.")

        if use_local and log_once("use_local"):
            logger.warning(
                "use_local is set to True. This could lead to "
                "issues with Cuda devices. If you are seeing this "
                "issue, try setting use_local to False. For more "
                "information, see "
                "https://github.com/ray-project/ray/issues/9202."
            )

        if (
            num_workers > 1
            and not dist.is_available()
            and not ray.util.client.ray.is_connected()
        ):
            raise ValueError(
                (
                    "Distributed PyTorch is not supported on macOS. "
                    "To run without distributed PyTorch, set 'num_workers=1'. "
                    "For more information, see "
                    "https://github.com/pytorch/examples/issues/467."
                )
            )

        if num_replicas is not None:
            raise DeprecationWarning(
                "num_replicas is deprecated. Use num_workers instead."
            )

        if batch_size is not None:
            raise DeprecationWarning(
                "batch_size is deprecated. Use config={'batch_size': N} "
                "specify a batch size for each worker or "
                "config={ray.util.sgd.utils.BATCH_SIZE: N} to specify a "
                "batch size to be used across all workers."
            )

        if apex_args is not None:
            raise DeprecationWarning(
                "apex_args is deprecated. Pass in apex_args when calling "
                "`register` in the `setup` method of your `TrainingOperator` "
                "instead."
            )

        if serialize_data_creation is True:
            if log_once("serialize_data_creation"):
                logging.warning(
                    "serialize_data_creation is deprecated and will be "
                    "ignored. If you require serialized data loading you "
                    "should implement this in TrainingOperator.setup. "
                    "You may find FileLock useful here."
                )

        if data_loader_args:
            raise DeprecationWarning(
                "data_loader_args is deprecated. You can return a "
                "torch.utils.data.DataLoader in data_creator. Ray will "
                "automatically set a DistributedSampler if a DataLoader is "
                "returned and num_workers > 1."
            )

        self.training_operator_cls = training_operator_cls

        self.initialization_hook = initialization_hook
        self.config = {} if config is None else config
        if use_gpu == "auto":
            use_gpu = torch.cuda.is_available()

        _remind_gpu_usage(use_gpu)

        if backend == "auto":
            backend = "nccl" if use_gpu else "gloo"

        if backend == "nccl":
            timeout_s = NCCL_TIMEOUT_S

        logger.debug(f"Using {backend} as backend.")
        self.backend = backend
        self.num_cpus_per_worker = num_cpus_per_worker
        self.use_gpu = use_gpu
        self.max_replicas = num_workers

        self.serialize_data_creation = serialize_data_creation
        self.wrap_ddp = wrap_ddp
        self.timeout_s = timeout_s
        self.use_fp16 = use_fp16
        self.use_tqdm = use_tqdm
        self.add_dist_sampler = add_dist_sampler
        self.use_local = use_local

        self.temp_dir = tempfile.mkdtemp(prefix="raysgd")
        self._num_failures = 0
        self._last_resize = float("-inf")

        if scheduler_step_freq:
            _validate_scheduler_step_freq(scheduler_step_freq)

        self.scheduler_step_freq = scheduler_step_freq

        if not ray.is_initialized() and self.max_replicas > 1:
            logger.info(
                "Automatically initializing single-node Ray. To use "
                "multi-node training, be sure to run `ray.init("
                "address='auto')` before instantiating the Trainer."
            )
            ray.init()
        startup_success = self._start_workers(self.max_replicas)
        if not startup_success:
            raise RuntimeError(
                "Worker startup failed. "
                "Are you sure you have enough resources to "
                "start the specified number of workers?"
            )

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
                (
                    "Changing batch size from {old_batch_size} to "
                    "{new_batch_size} to evenly distribute batches across "
                    "{num_workers} workers."
                ).format(
                    old_batch_size=batch_size,
                    new_batch_size=new_batch_size,
                    num_workers=num_workers,
                )
            )
            self.config[BATCH_SIZE] = new_batch_size
        return batch_size_per_worker

    def _start_workers(self, num_workers):
        worker_config = self.config.copy()
        batch_size_per_worker = self._configure_and_split_batch(num_workers)
        if batch_size_per_worker:
            worker_config[BATCH_SIZE] = batch_size_per_worker
        params = dict(
            training_operator_cls=self.training_operator_cls,
            config=worker_config,
            serialize_data_creation=self.serialize_data_creation,
            use_fp16=self.use_fp16,
            use_gpu=self.use_gpu,
            use_tqdm=self.use_tqdm,
            scheduler_step_freq=self.scheduler_step_freq,
        )

        dist_params = dict(
            backend=self.backend,
            add_dist_sampler=self.add_dist_sampler,
            wrap_ddp=self.wrap_ddp,
        )

        worker_args = {
            "max_workers": self.max_replicas,
            "params": params,
            "dist_params": dist_params,
            "initialization_hook": self.initialization_hook,
            "num_cpus_per_worker": self.num_cpus_per_worker,
            "use_gpu": self.use_gpu,
            "timeout_s": self.timeout_s,
        }

        if self.use_local:
            self.worker_group = LocalWorkerGroup(**worker_args)
        else:
            self.worker_group = RemoteWorkerGroup(**worker_args)

        # TODO(amogkam): If not enough resources are available to create
        #  num_workers workers, this command will hang. Instead,
        #  start_workers should take into account available resources when
        #  determining how many workers to create.
        return self.worker_group.start_workers(num_workers)

    def _resize_worker_group(self, state_dict, max_retries=10):
        """Resizes the number of remote workers based on available resources.
        Total number of workers will never exceed `num_workers` amount.

        Args:
            state_dict (dict): The state dict to load to all workers.
            max_retries (int): How many times to attempt to resize workers
                before failing.
        """
        old_workers = self.worker_group.num_workers
        self.worker_group.reset()

        time.sleep(1)
        for i in range(max_retries):
            new_workers = self.worker_group.new_workers_size()
            if new_workers:
                self._last_resize = time.time()
                startup_success = self._start_workers(int(new_workers))
                if not startup_success:
                    logger.info(
                        f"Worker startup failed. Retrying "
                        f"{max_retries-i-1} more times."
                    )
                    self.worker_group.reset()
                    continue
                self.load_state_dict(state_dict, blocking=True)
                if self.use_local and new_workers == 1 and old_workers > 1:
                    # Major hack. If we go from LocalDistributedRunner to a
                    # standard TorchRunner we have to manually reset the
                    # dummy actor handle global vars.
                    # TODO(amog): Refactor LocalDistributedTorchRunner to
                    #  not use global variables for resource reservation.
                    ray.util.sgd.torch.distributed_torch_runner._dummy_cuda_actor = None
                    ray.util.sgd.torch.distributed_torch_runner._dummy_cpu_actor = None
                return
            else:
                delay = 2 ** i
                logger.warning("No new workers found. Retrying in %d sec." % delay)
                time.sleep(delay)
        raise RuntimeError("Exceeded max_retries for relaunching workers.")

    def train(
        self,
        num_steps=None,
        profile=False,
        reduce_results=True,
        max_retries=3,
        info=None,
        dataset=None,
    ):
        """Runs a training epoch.

        Calls `operator.train_epoch()` on N parallel workers simultaneously
        underneath the hood.

        Set `max_retries` to enable fault handling in case of
        instance preemption.

        Args:
            num_steps (int): Number of batches to compute update steps on
                per worker. This corresponds also to the number of times
                ``TrainingOperator.train_batch`` is called per worker.
            profile (bool): Returns time stats for the training procedure.
            reduce_results (bool): Whether to average all metrics across
                all workers into one dict. If a metric is a non-numerical
                value (or nested dictionaries), one value will be randomly
                selected among the workers. If False, returns a list of dicts.
            max_retries (int): Must be non-negative. If set to N, TorchTrainer
                will detect and recover from training failure. The recovery
                process will kill all current workers, query the Ray
                global state for total available resources, and re-launch up to
                the available resources. Behavior is not well-defined
                in case of shared cluster usage. Defaults to 3.
            info (dict): Optional dictionary passed to the training
                operator for ``train_epoch`` and ``train_batch``.
            dataset (sgd.Dataset): Optional dataset to train with. If
                specified, the dataloader passed in via data_creator will be
                ignored.

        Returns:
            (dict | list) A dictionary of metrics for training.
                You can provide custom metrics by implementing a custom
                training loop. If ``reduce_results=False``, this will return a
                list of metric dictionaries whose length will be equal to
                ``num_workers``.
        """
        assert max_retries >= 0, "`max_retries` must be non-negative."
        assert (
            isinstance(dataset, Dataset) is not None or self.data_creator
        ), "Must specify either a data creator or a dataset"
        state_dict = self.state_dict()
        if self.worker_group.should_scale_up():
            logger.info("Resize opportunity detected. Attempting to scale up.")
            self._resize_worker_group(state_dict)
        success, worker_stats = self.worker_group.train(
            num_steps=num_steps, profile=profile, info=info, dataset=dataset
        )
        # Fault handling
        for i in range(max_retries):
            if success:
                break
            else:
                self._num_failures += 1
            self._resize_worker_group(state_dict)
            logger.info(
                "Retrying training step with %d workers."
                % self.worker_group.num_workers
            )
            success, worker_stats = self.worker_group.train(
                num_steps=num_steps, profile=profile, info=info, dataset=dataset
            )
        if not success:
            raise RuntimeError("Training run failed.")

        if reduce_results:
            return self._process_stats(worker_stats)
        else:
            return worker_stats

    def _process_stats(self, worker_stats):
        stats = {
            NUM_SAMPLES: sum(stats.pop(NUM_SAMPLES, np.nan) for stats in worker_stats)
        }
        for stat_key in worker_stats[0]:
            if isinstance(worker_stats[0][stat_key], numbers.Number):
                stats[stat_key] = np.nanmean(
                    [s.get(stat_key, np.nan) for s in worker_stats]
                )
            else:
                stats[stat_key] = worker_stats[0][stat_key]
        return stats

    def apply_all_workers(self, fn):
        """Run a function on all operators on the workers.

        Args:
            fn (Callable): A function that takes in no arguments.

        Returns:
            A list of objects returned by ``fn`` on each worker.

        """
        return self.worker_group.apply_all_workers(fn)

    def apply_all_operators(self, fn):
        """Run a function on all operators on the workers.

        Args:
            fn (Callable[TrainingOperator]): A function that takes in a
                TrainingOperator.

        Returns:
            A list of objects returned by ``fn`` on each operator.

        """
        return self.worker_group.apply_all_operators(fn)

    def validate(self, num_steps=None, profile=False, reduce_results=True, info=None):
        """Evaluates the model on the validation data set.

        Args:
            num_steps (int): Number of batches to compute update steps on
                per worker. This corresponds also to the number of times
                ``TrainingOperator.validate_batch`` is called per worker.
            profile (bool): Returns time stats for the evaluation procedure.
            reduce_results (bool): Whether to average all metrics across
                all workers into one dict. If a metric is a non-numerical
                value (or nested dictionaries), one value will be randomly
                selected among the workers. If False, returns a list of dicts.
            info (dict): Optional dictionary passed to the training
                operator for `validate` and `validate_batch`.

        Returns:
            A dictionary of metrics for validation.
                You can provide custom metrics by passing in a custom
                ``training_operator_cls``.
        """
        worker_stats = self.worker_group.validate(
            num_steps=num_steps, profile=profile, info=info
        )

        if reduce_results:
            return self._process_stats(worker_stats)
        else:
            return worker_stats

    def update_scheduler(self, metric):
        """Calls ``scheduler.step(metric)`` on all registered schedulers.

        This is useful for lr_schedulers such as ``ReduceLROnPlateau``.
        """
        self.worker_group.apply_all_operators(
            lambda op: [sched.step(metric) for sched in op._schedulers]
        )

    def get_model(self, to_cpu=False):
        """Returns the learned model(s).

        Arguments:
            to_cpu (bool): Forces returned model to be on CPU. This is
                useful if workers are trained on GPU, but the TorchTrainer
                lives on a CPU-only machine.

        """
        unwrapped = []
        models = self.worker_group.get_model(to_cpu)
        for model in models:
            unwrapped += [model.module if hasattr(model, "module") else model]
        if len(unwrapped) == 1:
            return unwrapped[0]
        return unwrapped

    def get_local_operator(self):
        """Returns the local TrainingOperator object.

        Be careful not to perturb its state, or else you can cause the system
        to enter an inconsistent state.

        Returns:
            TrainingOperator: The local TrainingOperator object.
        """
        return self.worker_group.get_local_operator()

    def state_dict(self):
        return self.worker_group.state_dict()

    def load_state_dict(self, state_dict, blocking=False):
        self.worker_group.load_state_dict(state_dict, blocking=blocking)

    def save(self, checkpoint):
        """Saves the Trainer state to the provided checkpoint path.

        Args:
            checkpoint (str): Path to target checkpoint file.
        """
        torch.save(self.state_dict(), checkpoint)
        return checkpoint

    def load(self, checkpoint):
        """Loads the Trainer and all workers from the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.
        """
        state_dict = torch.load(checkpoint)
        self.load_state_dict(state_dict)

    def restore(self, *args):
        raise DeprecationWarning("Use `TorchTrainer.load()` instead.")

    def shutdown(self, force=False):
        """Shuts down workers and releases resources.

        Args:
            force (bool): If True, forcefully kill all workers. If False,
                attempt a graceful shutdown first, and then forcefully kill if
                unsuccessful.

        """
        self.worker_group.shutdown(force=force)
        self.worker_group = DeactivatedWorkerGroup()

    @classmethod
    def as_trainable(cls, *args, override_tune_step=None, **kwargs):
        """Creates a BaseTorchTrainable class compatible with Tune.

        Any configuration parameters will be overridden by the Tune
        Trial configuration. You can also pass in a custom
        ``override_tune_step`` to implement your own iterative optimization
        routine and override the default implementation.

        .. code-block:: python

            def step(trainer, info):
                # Implement custom objective function here.
                train_stats = trainer.train()
                ...
                # Return the metrics to report to tune.
                # Do not call tune.report here.
                return train_stats

            TorchTrainable = TorchTrainer.as_trainable(
                training_operator_cls=MyTrainingOperator,
                num_workers=2,
                use_gpu=True,
                override_tune_step=step
            )
            analysis = tune.run(
                TorchTrainable,
                config={"lr": tune.grid_search([0.01, 0.1])}
            )

        Args:
            override_tune_step (Callable[[TorchTrainer, Dict], Dict]): A
                function to override the default training step to be used
                for Ray Tune. It accepts two arguments: the first one is an
                instance of your TorchTrainer, and the second one is a info
                dictionary, containing information about the Trainer
                state. If None is passed in, the default step
                function will be
                used: run 1 epoch of training, 1 epoch of validation,
                and report both results to Tune. Passing in
                ``override_tune_step`` is useful to define
                custom step functions, for example if you need to
                manually update the scheduler or want to run more than 1
                training epoch for each tune iteration.

        """
        if not TUNE_INSTALLED:
            raise RuntimeError(
                "Please install `ray[tune]` to use the Tune " "integration."
            )
        if override_tune_step is not None:
            callback_args = inspect.signature(override_tune_step)
            if not len(callback_args.parameters) == 2:
                raise ValueError(
                    "override_tune_step must take in exactly 2 "
                    "arguments. The passed in function "
                    "currently takes in {} "
                    "args".format(str(len(callback_args.parameters)))
                )

        class TorchTrainable(BaseTorchTrainable):
            @classmethod
            def default_resource_request(cls, config):
                num_workers = config.get("num_workers", kwargs.get("num_workers", 1))
                num_cpus_per_worker = config.get(
                    "num_cpus_per_worker", kwargs.get("num_cpus_per_worker", 1)
                )
                use_gpu = config.get("use_gpu", kwargs.get("use_gpu"))
                use_local = config.get("use_local", kwargs.get("use_local", False))

                bundles = []

                if not use_local:
                    # We need a separate bundle for the driver
                    bundles += [{"CPU": 1}]

                bundles += [
                    # Worker bundles
                    {"CPU": num_cpus_per_worker, "GPU": int(use_gpu)}
                ] * num_workers

                return PlacementGroupFactory(bundles, strategy="PACK")

            def step(self):
                if override_tune_step is not None:
                    output = override_tune_step(
                        self._trainer, {"iteration": self.training_iteration}
                    )
                    return output
                else:
                    return super(TorchTrainable, self).step()

            def _create_trainer(self, tune_config):
                """Overrides the provided config with Tune config."""
                provided_config = kwargs.get("config", {}).copy()
                provided_config.update(tune_config)
                kwargs["config"] = provided_config
                trainer = TorchTrainer(*args, **kwargs)
                return trainer

        return TorchTrainable


class BaseTorchTrainable(Trainable):
    """Base class for converting TorchTrainer to a Trainable class.

    This class is produced when you call ``TorchTrainer.as_trainable(...)``.

    By default one step of training runs ``trainer.train()`` once and
    ``trainer.validate()`` once. You can implement custom iterative
    training procedures by passing in a ``override_tune_step`` function to
    ``as_trainable``:

    .. code-block:: python

        def custom_step(trainer, info):
            for i in range(5):
                train_stats = trainer.train()
            validation_stats = trainer.validate()
            train_stats.update(validation_stats)
            return train_stats

        # TorchTrainable is subclass of BaseTorchTrainable.
        TorchTrainable = TorchTrainer.as_trainable(
            training_operator_cls=MyTrainingOperator,
            num_workers=2,
            use_gpu=True,
            override_tune_step=custom_step
        )

        analysis = tune.run(
            TorchTrainable,
            config={"lr": tune.grid_search([0.01, 0.1])}
        )

    """

    def setup(self, config):
        """Constructs a TorchTrainer object as `self.trainer`."""
        self._trainer = self._create_trainer(config)

    def step(self):
        """Calls `self.trainer.train()` and `self.trainer.validate()` once."""
        if self._implements_method("_train"):
            raise DeprecationWarning(
                "Trainable._train is deprecated and is now removed."
                "Override Trainable.step instead."
            )

        train_stats = self.trainer.train(max_retries=0, profile=True)
        validation_stats = self.trainer.validate(profile=True)
        stats = merge_dicts(train_stats, validation_stats)
        return stats

    def save_checkpoint(self, checkpoint_dir):
        """Returns a path containing the trainer state."""
        checkpoint_path = os.path.join(checkpoint_dir, "trainer.checkpoint")
        self.trainer.save(checkpoint_path)
        return checkpoint_path

    def load_checkpoint(self, checkpoint_path):
        """Restores the trainer state.

        Override this if you have state external to the Trainer object.
        """
        return self.trainer.load(checkpoint_path)

    def cleanup(self):
        """Shuts down the trainer."""
        self.trainer.shutdown()

    def _create_trainer(self, config):
        raise NotImplementedError

    @property
    def trainer(self):
        """An instantiated TorchTrainer object.

        Use this when specifying custom training procedures for Tune.
        """
        return self._trainer
