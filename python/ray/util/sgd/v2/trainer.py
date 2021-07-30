import logging
from typing import Union, Callable, List, TypeVar, Optional, Any, Dict

from ray.util.sgd.v2.backends.backend import BackendConfig, BackendExecutor
from ray.util.sgd.v2.callbacks.callback import Callback
from ray.util.sgd.v2.constants import BACKEND_NAME_TO_CONFIG_CLS

T = TypeVar("T")
S = TypeVar("S")

logger = logging.getLogger(__name__)


class Trainer:
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
        """
        Gets the ``BackendConfig`` to use for training.

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
            train_func: Callable[[Dict[str, Any]], T],
            config: Optional[Dict[str, Any]] = None,
            callbacks: Optional[List[Callback]] = None) -> List[T]:
        """Runs a training function in a distributed manner.

        Args:
            train_func (Callable): The training function to execute.
            config (Optional[Dict]): Configurations to pass into
                ``train_func``. If None then an empty Dict will be created.
            callbacks (Optional[List[Callback]]): A list of Callbacks which
                will be executed during training. If this is not set,
                currently there are NO default Callbacks.

        Returns:
            A list of results from the training function. Each value in the
            list corresponds to the output of the training function from
            each worker.
        """
        config = {} if config is None else config
        # TODO(matt): Set default callbacks.
        callbacks = [] if callbacks is None else callbacks
        return self._executor.run(train_func, config)

    def execute(self, func: Callable[..., T], *args, **kwargs) -> List[T]:
        """Executes a function for all instances of ``self.train_cls``.

        Args:
            func (Callable): The function that should be executed.
                The first argument should be an instance of
                ``self.train_cls``.
            args, kwargs: The arguments to pass into `func`.

        Returns:
            A list of results from ``func``. Each value in the
            list corresponds to the output of ``func`` from
            each worker.
        """
        pass

    def execute_single(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Executes a function on a single instance of ``self.train_cls``.

        Args:
            func (Callable): The function that should be executed.
                The first argument should be an instance of
                ``self.train_cls``.
            args, kwargs: The arguments to pass into `func`.

        Returns:
            The output of ``func`` from a single worker.
        """
        pass

    def shutdown(self):
        """Shuts down the training execution service."""
        self._executor.shutdown()

    def to_tune_trainable(self, train_func: Callable[[Dict[str, Any]], T]
                          ) -> Callable[[Dict[str, Any]], List[T]]:
        """Creates a Tune trainable function."""

        def trainable(config: Dict[str, Any]) -> List[T]:
            pass

        return trainable
