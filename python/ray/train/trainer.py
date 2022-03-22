from datetime import datetime
import logging
import os
from pathlib import Path
from typing import Union, Callable, List, TypeVar, Optional, Any, Dict, Type

import ray
from ray.actor import ActorHandle
from ray.train.backend import (
    BackendConfig,
    BackendExecutor,
    InactiveWorkerGroupError,
    TrainBackendError,
    TrainingWorkerError,
)
from ray.train.callbacks.callback import TrainingCallback
from ray.train.session import TrainingResultType
from ray.train.utils import RayDataset, construct_train_func, ActorWrapper
from ray.train.checkpoint import (
    CheckpointStrategy,
    TuneCheckpointManager,
    CheckpointManager,
    load_checkpoint_from_path,
)
from ray.train.constants import (
    TUNE_INSTALLED,
    DEFAULT_RESULTS_DIR,
    TUNE_CHECKPOINT_FILE_NAME,
    ENABLE_DETAILED_AUTOFILLED_METRICS_ENV,
    ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
    TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV,
    TRAIN_ENABLE_WORKER_SPREAD_ENV,
)

# Ray Train should be usable even if Tune is not installed.
from ray.train.utils import construct_path
from ray.train.worker_group import WorkerGroup
from ray.util import PublicAPI
from ray.util.annotations import DeveloperAPI

if TUNE_INSTALLED:
    from ray import tune
    from ray.tune import Trainable
    from ray.tune import PlacementGroupFactory
    from ray.tune.function_runner import wrap_function
else:
    tune = PlacementGroupFactory = Trainable = object

    def noop():
        return

    wrap_function = noop

T = TypeVar("T")
S = TypeVar("S")

logger = logging.getLogger(__name__)

BACKEND_NAME_TO_CONFIG_CLS_NAME = {
    "horovod": "HorovodConfig",
    "tensorflow": "TensorflowConfig",
    "torch": "TorchConfig",
}

# The environment variables that need to be propagated from the driver to the
# `BackendExecutor` actor via runtime env.
BACKEND_ENV_VARS = {
    ENABLE_DETAILED_AUTOFILLED_METRICS_ENV,
    ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
    TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV,
    TRAIN_ENABLE_WORKER_SPREAD_ENV,
}


# Import backend configurations dynamically since not all subdependencies
# may be installed.
def get_backend_config_cls(backend_name) -> type:
    if backend_name not in BACKEND_NAME_TO_CONFIG_CLS_NAME:
        raise ValueError(
            f"Invalid backend: {backend_name}. "
            f"Supported string values are: "
            f"{BACKEND_NAME_TO_CONFIG_CLS_NAME.keys()}"
        )
    import importlib

    config_cls = getattr(
        importlib.import_module(f"ray.train" f".{backend_name}"),
        BACKEND_NAME_TO_CONFIG_CLS_NAME[backend_name],
    )
    return config_cls


@PublicAPI(stability="beta")
class Trainer:
    """A class for enabling seamless distributed deep learning.

    Directory structure:
    - A logdir is created during instantiation. This will hold all the
    results/checkpoints for the lifetime of the Trainer. By default, it will be
    of the form ``~/ray_results/train_<datestring>``.
    - A run_dir is created for each ``run`` call. This will
    hold the checkpoints and results for a single ``trainer.run()`` or
    ``trainer.run_iterator()`` call. It will be of the form ``run_<run_id>``.

    Args:
        backend (Union[str, BackendConfig]): The backend used for
            distributed communication. If configurations are needed,
            a subclass of ``BackendConfig`` can be passed in.
            Supported ``str`` values: {"torch", "tensorflow", "horovod"}.
        num_workers (int): The number of workers (Ray actors) to launch.
            Each worker will reserve 1 CPU by default. The number of CPUs
            reserved by each worker can be overridden with the
            ``resources_per_worker`` argument.
        use_gpu (bool): If True, training will be done on GPUs (1 per
            worker). Defaults to False. The number of GPUs reserved by each
            worker can be overridden with the ``resources_per_worker``
            argument.
        resources_per_worker (Optional[Dict]): If specified, the resources
            defined in this Dict will be reserved for each worker. The
            ``CPU`` and ``GPU`` keys (case-sensitive) can be defined to
            override the number of CPU/GPUs used by each worker.
        logdir (Optional[str]): Path to the file directory where logs
            should be persisted. If this is not specified, one will be
            generated.
         max_retries (int): Number of retries when Ray actors fail.
            Defaults to 3. Set to -1 for unlimited retries.
    """

    def __init__(
        self,
        backend: Union[str, BackendConfig],
        num_workers: int,
        use_gpu: bool = False,
        resources_per_worker: Optional[Dict[str, float]] = None,
        logdir: Optional[str] = None,
        max_retries: int = 3,
    ):
        if num_workers <= 0:
            raise ValueError("`num_workers` must be a positive integer.")

        if not ray.is_initialized():
            ray.init()

        if "GPU" in ray.available_resources() and not use_gpu:
            logger.info(
                "GPUs are detected in your Ray cluster, but GPU "
                "training is not enabled for Ray Train. To enable "
                "GPU training, make sure to set `use_gpu` to True "
                "when instantiating your Trainer."
            )

        self._num_workers = num_workers
        self._use_gpu = use_gpu
        self._resources_per_worker = resources_per_worker

        # Incremental unique run ID.
        self._run_id = 0
        self.logdir = self.create_logdir(logdir)

        # Setup executor.
        self._backend_config = self._get_backend_config(backend)

        num_cpus = 1
        num_gpus = int(use_gpu)

        if resources_per_worker:
            # Override CPU and GPU resources and remove from dict.
            num_cpus = resources_per_worker.pop("CPU", num_cpus)
            num_gpus = resources_per_worker.pop("GPU", num_gpus)
            if not use_gpu and num_gpus > 0:
                raise ValueError(
                    "`use_gpu` is False but `GPU` was found in "
                    "`resources_per_worker`. Either set `use_gpu` to True or "
                    "remove `GPU` from `resources_per_worker."
                )
            if use_gpu and num_gpus == 0:
                raise ValueError(
                    "`use_gpu` is True but `GPU` is set to 0 in "
                    "`resources_per_worker`. Either set `use_gpu` to False or "
                    "request a positive number of `GPU` in "
                    "`resources_per_worker."
                )

        runtime_env = {
            "env_vars": {
                var_name: os.environ[var_name]
                for var_name in BACKEND_ENV_VARS
                if var_name in os.environ
            }
        }

        remote_executor = ray.remote(num_cpus=0)(BackendExecutor)

        backend_executor_actor = remote_executor.options(
            runtime_env=runtime_env
        ).remote(
            backend_config=self._backend_config,
            num_workers=num_workers,
            num_cpus_per_worker=num_cpus,
            num_gpus_per_worker=num_gpus,
            additional_resources_per_worker=resources_per_worker,
            max_retries=max_retries,
        )

        self._backend_executor = ActorWrapper(backend_executor_actor)

        if self._is_tune_enabled():
            self.checkpoint_manager = TuneCheckpointManager()
        else:
            self.checkpoint_manager = CheckpointManager()
        self.checkpoint_manager.on_init()

    def create_logdir(self, log_dir: Optional[Union[str, Path]]) -> Path:
        """Create logdir for the Trainer."""
        # Create directory for logs.
        log_dir = Path(log_dir) if log_dir else None
        if not log_dir:
            # Initialize timestamp for identifying this Train  execution.
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            log_dir = Path(f"train_{timestr}")
        log_dir = construct_path(log_dir, DEFAULT_RESULTS_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Trainer logs will be logged in: {log_dir}")
        return log_dir

    def create_run_dir(self):
        """Create rundir for the particular training run."""
        self.latest_run_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Run results will be logged in: {self.latest_run_dir}")

    def _get_backend_config(self, backend: Union[str, BackendConfig]) -> BackendConfig:
        """Gets the ``BackendConfig`` to use for training.

        Args:
            backend (Union[str, BackendConfig]): If a ``BackendConfig`` is
                passed in, then it will also be returned. If a ``str`` is
                passed in, then the default config for that backend will be
                returned.

        Returns:
            The ``BackendConfig`` that will be used to set up the
            ``BackendExecutor``.
        """

        if isinstance(backend, BackendConfig):
            return backend
        elif isinstance(backend, str):
            return get_backend_config_cls(backend)()
        else:
            raise TypeError(f"Invalid type for backend: {type(backend)}.")

    def _is_tune_enabled(self):
        """Whether or not this Trainer is part of a Tune session."""
        return TUNE_INSTALLED and tune.is_session_enabled()

    def start(self, initialization_hook: Optional[Callable[[], None]] = None):
        """Starts the training execution service.

        Args:
            initialization_hook (Optional[Callable]): The function to call on
                each worker when it is instantiated.
        """
        self._backend_executor.start(initialization_hook)

    def run(
        self,
        train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
        config: Optional[Dict[str, Any]] = None,
        callbacks: Optional[List[TrainingCallback]] = None,
        dataset: Optional[Union[RayDataset, Dict[str, RayDataset]]] = None,
        checkpoint: Optional[Union[Dict, str, Path]] = None,
        checkpoint_strategy: Optional[CheckpointStrategy] = None,
    ) -> List[T]:
        """Runs a training function in a distributed manner.

        Args:
            train_func (Callable): The training function to execute.
                This can either take in no arguments or a ``config`` dict.
            config (Optional[Dict]): Configurations to pass into
                ``train_func``. If None then an empty Dict will be created.
            callbacks (Optional[List[TrainingCallback]]): A list of Callbacks
                which will be executed during training. If this is not set,
                currently there are NO default Callbacks.
            dataset (Optional[Union[RayDataset, Dict[str, RayDataset]]]):
                Distributed Ray :ref:`Dataset <dataset-api>` or
                :ref:`DatasetPipeline <dataset-pipeline-api>` to pass into the
                workers, which can be accessed from the training function via
                ``train.get_dataset_shard()``. Sharding will automatically be
                handled by the Trainer. Multiple Datasets can be passed in as
                a ``Dict`` that maps each name key to a Dataset value,
                and each Dataset can be accessed from the training function
                by passing in a `dataset_name` argument to
                ``train.get_dataset_shard()``.
            checkpoint (Optional[Dict|str|Path]): The checkpoint data that
                should be loaded onto each worker and accessed by the training
                function via ``train.load_checkpoint()``. If this is a ``str``
                or ``Path`` then the value is expected to be a path to a file
                that contains a serialized checkpoint dict. If this is
                ``None`` then no checkpoint will be loaded.
            checkpoint_strategy (Optional[CheckpointStrategy]): The
                configurations for saving checkpoints.

        Returns:
            A list of results from the training function. Each value in the
            list corresponds to the output of the training function from
            each worker.
        """
        # Create new log directory for this run.
        self._run_id += 1
        self.create_run_dir()

        # TODO(matt): Set default callbacks.
        callbacks = [] if callbacks is None else callbacks
        finished_with_errors = False

        for callback in callbacks:
            callback.start_training(
                logdir=str(self.latest_run_dir), config=config or {}
            )

        train_func = construct_train_func(train_func, config)

        try:
            iterator = TrainingIterator(
                backend_executor=self._backend_executor,
                backend_config=self._backend_config,
                train_func=train_func,
                dataset=dataset,
                checkpoint_manager=self.checkpoint_manager,
                checkpoint=checkpoint,
                checkpoint_strategy=checkpoint_strategy,
                run_dir=self.latest_run_dir,
            )
            for intermediate_result in iterator:
                for callback in callbacks:
                    callback.process_results(intermediate_result)

            assert iterator.is_finished()
            return iterator.get_final_results()
        finally:
            for callback in callbacks:
                callback.finish_training(error=finished_with_errors)

    def run_iterator(
        self,
        train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
        config: Optional[Dict[str, Any]] = None,
        dataset: Optional[Union[RayDataset, Dict[str, RayDataset]]] = None,
        checkpoint: Optional[Union[Dict, str, Path]] = None,
        checkpoint_strategy: Optional[CheckpointStrategy] = None,
    ) -> "TrainingIterator":
        """Same as ``run`` except returns an iterator over the results.

        This is useful if you want to have more customization of what to do
        with the intermediate results or how to use the ``Trainer`` with Ray
        Tune.

        .. code-block:: python

            def train_func(config):
                ...
                for _ in config["epochs"]:
                    metrics = train()
                    metrics = validate(...)
                    ray.train.report(**metrics)
                return model

            iterator = trainer.run_iterator(train_func, config=config)

            for result in iterator:
                do_stuff(result)
                latest_ckpt = trainer.get_latest_checkpoint()

            assert iterator.is_finished()
            model = iterator.get_fin()[0]

        Args:
            train_func (Callable): The training function to execute.
                This can either take in no arguments or a ``config`` dict.
            config (Optional[Dict]): Configurations to pass into
                ``train_func``. If None then an empty Dict will be created.
            checkpoint (Optional[Dict|Path|str]): The checkpoint data that
                should be loaded onto each worker and accessed by the
                training function via ``train.load_checkpoint()``. If this is a
                ``str`` or ``Path`` then the value is expected to be a path
                to a file that contains a serialized checkpoint dict. If this
                is ``None`` then no checkpoint will be loaded.
            checkpoint_strategy (Optional[CheckpointStrategy]): The
                configurations for saving checkpoints.

        Returns:
            An Iterator over the intermediate results from ``train.report()``.
        """
        # Create new log directory for this run.
        self._run_id += 1
        self.create_run_dir()

        train_func = construct_train_func(train_func, config)

        return TrainingIterator(
            backend_executor=self._backend_executor,
            backend_config=self._backend_config,
            train_func=train_func,
            run_dir=self.latest_run_dir,
            dataset=dataset,
            checkpoint_manager=self.checkpoint_manager,
            checkpoint=checkpoint,
            checkpoint_strategy=checkpoint_strategy,
        )

    @property
    def latest_run_dir(self) -> Optional[Path]:
        """Path to the log directory for the latest call to ``run()``.

        Returns ``None`` if ``run()`` has not been called.
        """
        if self._run_id > 0:
            run_dir = Path(f"run_{self._run_id:03d}")
            return construct_path(run_dir, self.logdir)
        else:
            return None

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        """Path to the checkpoint directory.

        Returns ``None`` if ``run()`` has not been called or if
        ``train.checkpoint()`` has not been called from ``train_func``within
        the most recent call to ``run``.
        """
        return self.checkpoint_manager.latest_checkpoint_dir

    @property
    def best_checkpoint_path(self) -> Optional[Path]:
        """Path to the best persisted checkpoint from the latest run.

        "Best" is defined by the input ``CheckpointStrategy``.
        Default behavior is to return the most recent checkpoint.

        Returns ``None`` if ``run()`` has not been called or if
        ``train.save_checkpoint()`` has not been called from ``train_func``
        within the most recent call to ``run``.
        """
        return self.checkpoint_manager.best_checkpoint_path

    @property
    def latest_checkpoint(self) -> Optional[Dict]:
        """The latest saved checkpoint.

        This checkpoint may not be saved to disk.

        Returns ``None`` if ``run()`` has not been called or if
        ``train.checkpoint()`` has not been called from ``train_func``.
        """
        return self.checkpoint_manager.latest_checkpoint

    @property
    def best_checkpoint(self) -> Optional[Dict]:
        """Best saved checkpoint from the latest run.

        "Best" is defined by the input ``CheckpointStrategy``.
        Default behavior is to return the most recent checkpoint.

        Returns ``None`` if ``run()`` has not been called or if
        ``train.save_checkpoint()`` has not been called from ``train_func``
        within the most recent call to ``run``.
        """
        best_checkpoint_path = self.best_checkpoint_path
        if best_checkpoint_path is None:
            return None
        else:
            return load_checkpoint_from_path(best_checkpoint_path)

    @staticmethod
    def load_checkpoint_from_path(checkpoint_file_path: Union[str, Path]) -> Dict:
        """Convenience method to load a checkpoint from path.

        An error will be raised if the provided path does not exist.

        Args:
            checkpoint_file_path (Union[str, Path]): The path to the checkpoint
                to load. If the checkpoint saved in this path has not been
                created by Ray Train, there is no guarantee that it can be
                loaded in successfully.
        """
        return load_checkpoint_from_path(checkpoint_file_path)

    def shutdown(self):
        """Shuts down the training execution service."""
        self._backend_executor.shutdown()

    def to_tune_trainable(
        self,
        train_func: Callable[[Dict[str, Any]], T],
        dataset: Optional[Union[RayDataset, Dict[str, RayDataset]]] = None,
    ) -> Type[Trainable]:
        """Creates a Tune ``Trainable`` from the input training function.

        Args:
            func (Callable): The function that should be executed on each
                training worker.
            dataset (Optional[Union[RayDataset, Dict[str, RayDataset]]]):
                Distributed Ray p:ref:`Dataset <dataset-api>` or
                :ref:`DatasetPipeline <dataset-pipeline-api>` to pass into the
                workers, which can be accessed from the training function via
                ``train.get_dataset_shard()``. Sharding will automatically be
                handled by the Trainer. Multiple Datasets can be passed in as
                a ``Dict`` that maps each name key to a Dataset value,
                and each Dataset can be accessed from the training function
                by passing in a `dataset_name` argument to
                ``train.get_dataset_shard()``.

        Returns:
            A Trainable that can directly be passed into ``tune.run()``.
        """
        if not TUNE_INSTALLED:
            raise ValueError(
                "Tune is not installed. Please install ray["
                "tune] to use the Tune integration."
            )

        if self._backend_executor.is_started():
            raise RuntimeError(
                "The Trainer must not be active to use "
                "`to_tune_trainable`. Either shutdown the "
                "Trainer or don't start it in the first place."
            )

        return _create_tune_trainable(
            train_func,
            dataset,
            self._backend_config,
            self._num_workers,
            self._use_gpu,
            self._resources_per_worker,
        )

    def to_worker_group(self, train_cls: Type, *args, **kwargs) -> "TrainWorkerGroup":
        """Returns Ray actors with the provided class and the backend started.

        This is useful if you want to provide your own class for training
        and have more control over execution, but still want to use Ray Train
        to setup the appropriate backend configurations (torch, tf, etc.).

        .. code-block:: python

            class Trainer:
                def __init__(self, config):
                    self.config = config

                def train_epoch(self):
                    ...
                    return 1

            config = {"lr": 0.1}
            trainer = Trainer(num_workers=2, backend="torch")
            workers = trainer.to_worker_group(train_cls=Trainer, config=config)
            futures = [w.train_epoch.remote() for w in workers]
            assert ray.get(futures) == [1, 1]
            assert ray.get(workers[0].train_epoch.remote()) == 1
            workers.shutdown()

        Args:
            train_cls (Type): The class definition to use for the Ray
                actors/workers.
            args, kwargs: Arguments to pass into the ``__init__`` of the
                provided ``train_cls``.
        """
        if self._backend_executor.is_started():
            raise RuntimeError(
                "The Trainer must not be active to use "
                "`to_worker_group`. Either shutdown the "
                "Trainer or don't start it in the first place."
            )
        self._backend_executor.start(
            train_cls=train_cls, train_cls_args=args, train_cls_kwargs=kwargs
        )
        worker_group = self._backend_executor.get_worker_group()
        return TrainWorkerGroup(worker_group)


@DeveloperAPI
class TrainWorkerGroup:
    """A container for a group of Ray actors.

    You should not instantiate this directly and only use this as the output
    of ``Trainer.to_worker_group``. You can index or iterate this object like
    you would a List.

    .. code-block:: python

        class Trainer:
            def __init__(self, config):
                self.config = config

            def train_epoch(self):
                ...
                return 1

        config = {"lr": 0.1}
        trainer = Trainer(num_workers=2, backend="torch")
        workers = trainer.to_worker_group(train_cls=Trainer, config=config)
        futures = [w.train_epoch.remote() for w in workers]
        assert ray.get(futures) == [1, 1]
        assert ray.get(workers[0].train_epoch.remote()) == 1
        workers.shutdown()`
    """

    def __init__(self, worker_group: WorkerGroup):
        self._worker_group = worker_group

    def __getitem__(self, item) -> ActorHandle:
        return self._worker_group.workers[item].actor

    def shutdown(self, patience_s: float = 5):
        """Shutdown all the workers.

        Args:
            patience_s (float): Attempt a graceful shutdown
                of the workers for this many seconds. Fallback to force kill
                if graceful shutdown is not complete after this time. If
                this is less than or equal to 0, immediately force kill all
                workers.
        """
        self._worker_group.shutdown(patience_s=patience_s)


@DeveloperAPI
class TrainingIterator:
    """An iterator over Train results. Returned by ``trainer.run_iterator``."""

    def __init__(
        self,
        backend_executor: Union[BackendExecutor, ActorWrapper],
        backend_config: BackendConfig,
        train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
        dataset: Optional[Union[RayDataset, Dict[str, RayDataset]]],
        checkpoint_manager: CheckpointManager,
        checkpoint: Optional[Union[Dict, str, Path]],
        checkpoint_strategy: Optional[CheckpointStrategy],
        run_dir: Optional[Path] = None,
    ):
        self._backend_executor = backend_executor
        self._backend = backend_config.backend_cls()
        self._train_func = train_func
        self._dataset = dataset
        self._run_dir = run_dir
        self._checkpoint_manager = checkpoint_manager
        self._checkpoint_strategy = checkpoint_strategy
        self._start_training(
            train_func=train_func,
            run_dir=run_dir,
            dataset=dataset,
            checkpoint=checkpoint,
            checkpoint_strategy=checkpoint_strategy,
        )

        self._final_results = None
        self._finished_training = False

    def __iter__(self):
        return self

    def _start_training(
        self,
        train_func,
        run_dir,
        dataset,
        checkpoint,
        checkpoint_strategy,
        latest_checkpoint_id=None,
    ):
        self._checkpoint_manager.on_start_training(
            checkpoint_strategy=checkpoint_strategy,
            run_dir=run_dir,
            latest_checkpoint_id=latest_checkpoint_id,
        )
        checkpoint_dict = self._checkpoint_manager._load_checkpoint(checkpoint)
        self._run_with_error_handling(
            lambda: self._backend_executor.start_training(
                train_func=train_func, dataset=dataset, checkpoint=checkpoint_dict
            )
        )

    def _run_with_error_handling(self, func: Callable):
        try:
            return func()
        except TrainingWorkerError:
            # Workers have already been restarted.
            self._start_training(
                self._train_func,
                self._run_dir,
                self._dataset,
                self._checkpoint_manager.latest_checkpoint,
                self._checkpoint_strategy,
                latest_checkpoint_id=self._checkpoint_manager.latest_checkpoint_id,
            )
            return self._run_with_error_handling(func)
        except InactiveWorkerGroupError:
            raise RuntimeError(
                "This Trainer is not active. It is either shutdown "
                "already or never started in the first place. "
                "Either create a new Trainer or start this one."
            ) from None
        except TrainBackendError:
            raise RuntimeError(
                "Training failed. You should not be seeing "
                "this error and this is a bug. Please create "
                "a new issue at "
                "https://github.com/ray-project/ray."
            ) from None

    def __next__(self):
        if self.is_finished():
            raise StopIteration
        next_results = self._run_with_error_handling(self._fetch_next_result)
        if next_results is None:
            try:
                self._final_results = self._run_with_error_handling(
                    self._finish_training
                )
            finally:
                self._finished_training = True
            raise StopIteration
        else:

            return next_results

    def _fetch_next_result(self) -> Optional[List[Dict]]:
        """Fetch next results produced by ``train.report()`` from each worker.

        Assumes ``start_training`` has already been called.

        Returns:
            A list of dictionaries of values passed to ``train.report()`` from
                each worker. Each item corresponds to an intermediate result
                a single worker. If there are no more items to fetch,
                returns None.
        """

        while True:
            results = self._backend_executor.get_next_results()
            if results is None:
                return None
            first_result = results[0]
            result_type = first_result.type
            if result_type is TrainingResultType.REPORT:
                result_data = [self._backend.decode_data(r.data) for r in results]
                return result_data
            elif result_type is TrainingResultType.CHECKPOINT:
                self._checkpoint_manager._process_checkpoint(
                    results, decode_checkpoint_fn=self._backend.decode_data
                )
                # Iterate until next REPORT call or training has finished.
            else:
                raise TrainBackendError(
                    f"Unexpected result type: "
                    f"{result_type}. "
                    f"Expected one of "
                    f"{[type in TrainingResultType]}"
                )

    def _finish_checkpointing(self):
        while True:
            results = self._backend_executor.get_next_results()
            if results is None:
                break
            result_type = results[0].type
            # Process checkpoints and ignore other result types.
            if result_type is TrainingResultType.CHECKPOINT:
                self._checkpoint_manager._process_checkpoint(
                    results, decode_checkpoint_fn=self._backend.decode_data
                )

    def _finish_training(self):
        """Finish training and return final results. Propagate any exceptions.

        Blocks until training is finished on all workers.

        Assumes `start_training` has already been called.

        Returns:
            A list of return values from calling ``train_func`` on each worker.
                Each item corresponds to the return value from a single worker.
        """

        self._backend_executor.pause_reporting()
        # Finish up processing checkpoints. Reporting has been disabled.
        # Results will not be processed.
        self._finish_checkpointing()
        return self._backend_executor.finish_training()

    def is_finished(self) -> bool:
        return self._finished_training

    def get_final_results(self, force: bool = False) -> List[T]:
        """Gets the training func return values from each worker.

        If ``force`` is ``True``, then immediately finish training
        and return even if all the intermediate results have not
        been processed yet. Else, intermediate results must be
        processed before obtaining the final results. Defaults to
        False.
        """
        if not self.is_finished():
            assert self._final_results is None
            if force:
                try:
                    self._final_results = self._run_with_error_handling(
                        self._finish_training
                    )
                finally:
                    self._finished_training = True
            else:
                logger.info(
                    "Please finish iterating through the "
                    "intermediate results before getting the"
                    "final returns. If you would like "
                    "training to finish immediately and get "
                    "the final returns, then set "
                    "`force=True`."
                )

        return self._final_results


def _create_tune_trainable(
    train_func, dataset, backend_config, num_workers, use_gpu, resources_per_worker
):
    """Creates a Tune Trainable class for Train training.

    This function populates class attributes and methods.
    """

    # TODO(matt): Move dataset to Ray object store, like tune.with_parameters.
    def tune_function(config, checkpoint_dir=None):
        trainer = Trainer(
            backend=backend_config,
            num_workers=num_workers,
            use_gpu=use_gpu,
            resources_per_worker=resources_per_worker,
        )

        trainer.start()

        if checkpoint_dir is not None:
            checkpoint_path = os.path.join(checkpoint_dir, TUNE_CHECKPOINT_FILE_NAME)
        else:
            checkpoint_path = None

        iterator = trainer.run_iterator(
            train_func, config, dataset=dataset, checkpoint=checkpoint_path
        )

        for results in iterator:
            first_worker_results = results[0]

            tune.report(**first_worker_results)

        trainer.shutdown()

    trainable_cls = wrap_function(tune_function)

    class TrainTrainable(trainable_cls):
        """Add default resources to the Trainable."""

        @classmethod
        def default_resource_request(cls, config: Dict) -> PlacementGroupFactory:
            trainer_bundle = [{"CPU": 1}]
            worker_resources = {"CPU": 1, "GPU": int(use_gpu)}
            worker_resources_extra = (
                {} if resources_per_worker is None else resources_per_worker
            )
            worker_bundles = [
                {**worker_resources, **worker_resources_extra}
                for _ in range(num_workers)
            ]
            bundles = trainer_bundle + worker_bundles
            return PlacementGroupFactory(bundles, strategy="PACK")

    return TrainTrainable
