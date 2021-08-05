import inspect
import logging
from typing import Union, Callable, List, TypeVar, Optional, Any, Dict

from ray.tune import Trainable
from ray.util.sgd.v2.backends.backend import BackendConfig, BackendExecutor, \
    InactiveWorkerGroupError, SGDBackendError
from ray.util.sgd.v2.backends.tensorflow import TensorflowConfig
from ray.util.sgd.v2.backends.torch import TorchConfig
from ray.util.sgd.v2.callbacks.callback import SGDCallback

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
    """

    def __init__(self,
                 backend: Union[str, BackendConfig],
                 num_workers: int = 1,
                 use_gpu: bool = False,
                 resources_per_worker: Optional[Dict[str, float]] = None):
        """A class for distributed training.

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
        """
        # Setup executor.
        backend_config = self._get_backend_config(backend)

        if resources_per_worker:
            raise NotImplementedError("`resources_per_worker` argument is not "
                                      "supported yet.")

        self._executor = BackendExecutor(backend_config, num_workers, 1,
                                         int(use_gpu))

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
            callbacks: Optional[List[SGDCallback]] = None) -> List[T]:
        """Runs a training function in a distributed manner.

        Args:
            train_func (Callable): The training function to execute.
                This can either take in no arguments or a ``config`` dict.
            config (Optional[Dict]): Configurations to pass into
                ``train_func``. If None then an empty Dict will be created.
            callbacks (Optional[List[SGDCallback]]): A list of Callbacks which
                will be executed during training. If this is not set,
                currently there are NO default Callbacks.

        Returns:
            A list of results from the training function. Each value in the
            list corresponds to the output of the training function from
            each worker.
        """
        train_func = self._get_train_func(train_func, config)
        # TODO(matt): Set default callbacks.
        callbacks = [] if callbacks is None else callbacks

        try:
            self._executor.start_training(train_func)
            while True:
                intermediate_results = self._executor.fetch_next_result()
                if intermediate_results is None:
                    break
                else:
                    for callback in callbacks:
                        callback.handle_result(intermediate_results)
            return self._executor.finish_training()
        except InactiveWorkerGroupError:
            raise RuntimeError(
                "This Trainer is not active. It is either shutdown already or "
                "never started in the first place. Either create a new "
                "Trainer or start this one.") from None
        except SGDBackendError:
            raise RuntimeError("Training failed. You should not be seeing "
                               "this error and this is a bug. Please create "
                               "a new issue at "
                               "https://github.com/ray-project/ray.") from None

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

    def shutdown(self):
        """Shuts down the training execution service."""
        self._executor.shutdown()

    def to_tune_trainable(
            self, train_func: Callable[[Dict[str, Any]], T]) -> Trainable:
        """Creates a Tune ``Trainable`` from the input training function.

        Args:
            func (Callable): The function that should be executed on each
                training worker.

        Returns:
            A Trainable that can directly be passed into ``tune.run()``.
        """

        def trainable_func(config: Dict[str, Any]) -> T:
            pass

        raise NotImplementedError
