import inspect
import logging
from pathlib import Path
from typing import Union, Callable, List, TypeVar, Optional, Any, Dict, \
    Type, Iterator

from ray.util.sgd.v2.backends.backend import BackendConfig, BackendExecutor, \
    InactiveWorkerGroupError, SGDBackendError
from ray.util.sgd.v2.backends.tensorflow import TensorflowConfig
from ray.util.sgd.v2.backends.torch import TorchConfig
from ray.util.sgd.v2.callbacks.callback import SGDCallback
from ray.util.sgd.v2.checkpoint import CheckpointStrategy

# Ray SGD should be usable even if Tune is not installed.
try:
    TUNE_INSTALLED = True
    from ray import tune
    from ray.tune import Trainable
    from ray.tune import PlacementGroupFactory
    from ray.tune.function_runner import wrap_function
except ImportError:
    TUNE_INSTALLED = False
    tune = PlacementGroupFactory = Trainable = object

    def noop():
        return

    wrap_function = noop

T = TypeVar("T")
S = TypeVar("S")

logger = logging.getLogger(__name__)

BACKEND_NAME_TO_CONFIG_CLS = {
    "tensorflow": TensorflowConfig,
    "torch": TorchConfig
}


class Trainer:
    """A class for enabling seamless distributed deep learning.

    Args:
        backend (Union[str, BackendConfig]): The backend used for
            distributed communication. If configurations are needed,
            a subclass of ``BackendConfig`` can be passed in.
            Supported ``str`` values: {"torch"}.
        num_workers (int): The number of workers (Ray actors) to launch.
            Defaults to 1. Each worker will reserve 1 CPU by default.
        use_gpu (bool): If True, training will be done on GPUs (1 per
            worker). Defaults to False.
        resources_per_worker (Optional[Dict]): If specified, the resources
            defined in this Dict will be reserved for each worker.
        logdir (Optional[str]): Path to the file directory where logs
            should be persisted. If this is not specified, one will be
            generated.
    """

    def __init__(self,
                 backend: Union[str, BackendConfig],
                 num_workers: int = 1,
                 use_gpu: bool = False,
                 resources_per_worker: Optional[Dict[str, float]] = None,
                 logdir: Optional[str] = None):

        self._backend = backend
        self._num_workers = num_workers
        self._use_gpu = use_gpu
        self._resources_per_worker = resources_per_worker

        # Setup executor.
        backend_config = self._get_backend_config(backend)

        if resources_per_worker:
            raise NotImplementedError("`resources_per_worker` argument is not "
                                      "supported yet.")

        self._executor = BackendExecutor(backend_config, num_workers, 1,
                                         int(use_gpu), logdir)

    def _get_backend_config(
            self, backend: Union[str, BackendConfig]) -> BackendConfig:
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
            try:
                return BACKEND_NAME_TO_CONFIG_CLS[backend]()
            except KeyError:
                raise ValueError(f"Invalid backend: {backend}. "
                                 f"Supported string values are: "
                                 f"{BACKEND_NAME_TO_CONFIG_CLS.keys()}")
        else:
            raise TypeError(f"Invalid type for backend: {type(backend)}.")

    def start(self,
              initialization_hook: Optional[Callable[[], None]] = None,
              train_cls: Optional[S] = None,
              *args,
              **kwargs):
        """Starts the training execution service.

        Args:
            initialization_hook (Optional[Callable]): The function to call on
                each worker when it is instantiated.
            train_cls (Optional[cls]): The training class that each worker
                should be instantiated as.
            args, kwargs: The arguments to pass into ``train_cls.__init__``.
        """
        self._executor.start(initialization_hook)

    def run(self,
            train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
            config: Optional[Dict[str, Any]] = None,
            callbacks: Optional[List[SGDCallback]] = None,
            checkpoint: Optional[Union[Dict, str, Path]] = None,
            checkpoint_strategy: Optional[CheckpointStrategy] = None
            ) -> List[T]:
        """Runs a training function in a distributed manner.

        Args:
            train_func (Callable): The training function to execute.
                This can either take in no arguments or a ``config`` dict.
            config (Optional[Dict]): Configurations to pass into
                ``train_func``. If None then an empty Dict will be created.
            callbacks (Optional[List[SGDCallback]]): A list of Callbacks which
                will be executed during training. If this is not set,
                currently there are NO default Callbacks.
            checkpoint (Optional[Dict|str|Path]): The checkpoint data that
                should be loaded onto each worker and accessed by the training
                function via ``sgd.load_checkpoint()``. If this is a ``str`` or
                ``Path`` then the value is expected to be a path to a file
                that contains a serialized checkpoint dict. If this is
                ``None`` then no checkpoint will be loaded.
            checkpoint_strategy (Optional[CheckpointStrategy]): The
                configurations for saving checkpoints.

        Returns:
            A list of results from the training function. Each value in the
            list corresponds to the output of the training function from
            each worker.
        """
        # TODO(matt): Set default callbacks.
        callbacks = [] if callbacks is None else callbacks
        finished_with_errors = False

        for callback in callbacks:
            callback.start_training()

        try:
            iterator = self.run_iterator(
                train_func=train_func,
                config=config,
                checkpoint=checkpoint,
                checkpoint_strategy=checkpoint_strategy)
            for intermediate_result in iterator:
                for callback in callbacks:
                    callback.handle_result(intermediate_result)

            assert iterator.is_finished()
            return iterator.get_final_results()
        finally:
            for callback in callbacks:
                callback.finish_training(error=finished_with_errors)

    def run_iterator(
            self,
            train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
            config: Optional[Dict[str, Any]] = None,
            checkpoint: Optional[Dict] = None,
            checkpoint_strategy: Optional[CheckpointStrategy] = None
    ) -> Iterator[List[Dict]]:
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
                    ray.sgd.report(**metrics)
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
            checkpoint (Optional[Dict]): The checkpoint data that should be
                loaded onto each worker and accessed by the training function
                via ``sgd.load_checkpoint()``.
            checkpoint_strategy (Optional[CheckpointStrategy]): The
                configurations for saving checkpoints.

        Returns:
            An Iterator over the intermediate results from ``sgd.report()``.
        """
        train_func = self._get_train_func(train_func, config)

        return SGDIterator(
            backend_executor=self._executor,
            train_func=train_func,
            checkpoint=checkpoint,
            checkpoint_strategy=checkpoint_strategy)

    def _get_train_func(
            self,
            train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
            config: Optional[Dict[str, Any]]) -> Callable[[], T]:
        """Validates and constructs the training function to execute.

        Args:
            train_func (Callable): The training function to execute.
                This can either take in no arguments or a ``config`` dict.
            config (Optional[Dict]): Configurations to pass into
                ``train_func``. If None then an empty Dict will be created.

        Returns:
            A valid training function.

        Raises:
            ValueError: if the input ``train_func`` is invalid.
        """
        signature = inspect.signature(train_func)
        num_params = len(signature.parameters)
        if num_params > 1:
            raise ValueError("train_func should take in a 0 or 1 arguments.")
        elif num_params == 1:
            config = {} if config is None else config
            return lambda: train_func(config)
        else:  # num_params == 0
            return train_func

    def execute(self, func: Callable[..., T], *args, **kwargs) -> List[T]:
        """Executes a function for all instances of ``self.train_cls``.

        Args:
            func (Callable): The function that should be executed.
                The first argument should be an instance of
                ``self.train_cls``.
            args, kwargs: The arguments to pass into ``func``.

        Returns:
            A list of results from ``func``. Each value in the
            list corresponds to the output of ``func`` from
            each worker.
        """
        raise NotImplementedError

    def execute_single(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Executes a function on a single instance of ``self.train_cls``.

        Args:
            func (Callable): The function that should be executed.
                The first argument should be an instance of
                ``self.train_cls``.
            args, kwargs: The arguments to pass into ``func``.

        Returns:
            The output of ``func`` from a single worker.
        """
        raise NotImplementedError

    @property
    def logdir(self) -> Path:
        """Path to this Trainer's log directory."""
        return self._executor.logdir

    @property
    def latest_run_dir(self) -> Optional[Path]:
        """Path to the log directory for the latest call to ``run()``.

        Returns ``None`` if ``run()`` has not been called.
        """
        return self._executor.latest_run_dir

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        """Path to the checkpoint directory.

        Returns ``None`` if ``run()`` has not been called or if
        ``sgd.checkpoint()`` has not been called from ``train_func``within
        the most recent call to ``run``.
        """
        return self._executor.latest_checkpoint_dir

    @property
    def latest_checkpoint_path(self) -> Optional[Path]:
        """Path to the latest persisted checkpoint from the latest run.

        Returns ``None`` if ``run()`` has not been called or if
        ``sgd.checkpoint()`` has not been called from ``train_func`` within
        the most recent call to ``run``.
        """
        return self._executor.latest_checkpoint_path

    @property
    def latest_checkpoint(self) -> Optional[Dict]:
        """The latest saved checkpoint.

        This checkpoint may not be saved to disk.

        Returns ``None`` if ``run()`` has not been called or if
        ``sgd.checkpoint()`` has not been called from ``train_func``.
        """
        return self._executor.latest_checkpoint

    def shutdown(self):
        """Shuts down the training execution service."""
        self._executor.shutdown()

    def to_tune_trainable(self, train_func: Callable[[Dict[str, Any]], T]
                          ) -> Type[Trainable]:
        """Creates a Tune ``Trainable`` from the input training function.

        Args:
            func (Callable): The function that should be executed on each
                training worker.

        Returns:
            A Trainable that can directly be passed into ``tune.run()``.
        """
        if not TUNE_INSTALLED:
            raise ValueError("Tune is not installed. Please install ray["
                             "tune] to use the Tune integration.")

        if self._executor.is_started:
            raise RuntimeError("The Trainer must not be active to use "
                               "`to_tune_trainable`. Either shutdown the "
                               "Trainer or don't start it in the first place.")

        return _create_tune_trainable(train_func, self._backend,
                                      self._num_workers, self._use_gpu,
                                      self._resources_per_worker)


class SGDIterator:
    """An iterator over SGD results. Returned by ``trainer.run_iterator``."""

    def __init__(
            self, backend_executor: BackendExecutor,
            train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
            checkpoint: Optional[Dict],
            checkpoint_strategy: Optional[CheckpointStrategy]):
        self._executor = backend_executor
        self._run_with_error_handling(
            lambda: self._executor.start_training(
                train_func=train_func,
                checkpoint=checkpoint,
                checkpoint_strategy=checkpoint_strategy)
        )

        self._final_results = None
        self._finished_training = False

    def __iter__(self):
        return self

    def _run_with_error_handling(self, func: Callable):
        try:
            return func()
        except InactiveWorkerGroupError:
            raise RuntimeError(
                "This Trainer is not active. It is either shutdown "
                "already or never started in the first place. "
                "Either create a new Trainer or start this one.") \
                from None
        except SGDBackendError:
            raise RuntimeError("Training failed. You should not be seeing "
                               "this error and this is a bug. Please create "
                               "a new issue at "
                               "https://github.com/ray-project/ray.") from None

    def __next__(self):
        if self.is_finished():
            raise StopIteration
        next_results = self._run_with_error_handling(
            self._executor.fetch_next_result)
        if next_results is None:
            try:
                self._final_results = \
                    self._run_with_error_handling(
                        self._executor.finish_training)
            finally:
                self._finished_training = True
            raise StopIteration
        else:
            return next_results

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
                    self._final_results = \
                        self._run_with_error_handling(
                            self._executor.finish_training)
                finally:
                    self._finished_training = True
            else:
                logger.info("Please finish iterating through the "
                            "intermediate results before getting the"
                            "final returns. If you would like "
                            "training to finish immediately and get "
                            "the final returns, then set "
                            "`force=True`.")

        return self._final_results


def _create_tune_trainable(train_func, backend, num_workers, use_gpu,
                           resources_per_worker):
    """Creates a Tune Trainable class for SGD training.

    This function populates class attributes and methods.
    """

    # TODO(amog): Implement checkpointing for Tune.
    def tune_function(config, checkpoint_dir=None):
        trainer = Trainer(
            backend=backend,
            num_workers=num_workers,
            use_gpu=use_gpu,
            resources_per_worker=resources_per_worker)

        trainer.start()

        iterator = trainer.run_iterator(train_func, config)

        for results in iterator:
            first_worker_results = results[0]

            tune.report(**first_worker_results)

        trainer.shutdown()

    trainable_cls = wrap_function(tune_function)

    class SgdTrainable(trainable_cls):
        """Add default resources to the Trainable."""

        @classmethod
        def default_resource_request(cls,
                                     config: Dict) -> PlacementGroupFactory:
            head_bundle = [{"CPU": 1}]  # driver
            worker_resources = {"CPU": 1, "GPU": int(use_gpu)}
            worker_resources_extra = {} if resources_per_worker is None else\
                resources_per_worker
            worker_bundles = [{
                **worker_resources,
                **worker_resources_extra
            } for _ in range(num_workers)]
            bundles = head_bundle + worker_bundles
            return PlacementGroupFactory(bundles, strategy="PACK")

    return SgdTrainable
