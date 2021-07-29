from typing import Union, Callable, List, TypeVar, Optional, Any, Dict

from ray.util.sgd.v2.backends.backend import BackendConfig, BackendExecutor
from ray.util.sgd.v2.callbacks.callback import Callback
from ray.util.sgd.v2.constants import BACKEND_NAME_TO_CONFIG_CLS

T = TypeVar("T")


class Trainer:
    def __init__(self,
                 backend: Union[str, BackendConfig],
                 num_workers: int = 1,
                 num_cpus_per_worker: float = 1,
                 num_gpus_per_worker: float = 0,
                 callbacks: Optional[List[Callback]] = None):
        """
        A class for distributed training.

        Args:
            backend (Union[str, BackendConfig]): The backend used for
                distributed communication. If configurations are needed,
                a subclass of ``BackendConfig`` can be passed in.
                Supported ``str`` values: {"torch"}.
            num_workers (int): The number of workers (Ray actors) to launch.
                Defaults to 1.
            num_cpus_per_worker (float): The number of CPUs to reserve for each
                worker. Fractional values are allowed. Defaults to 1.
            num_gpus_per_worker (float): The number of GPUs to reserve for each
                worker. Fractional values are allowed. Defaults to 0.
            callbacks (Optional[List[Callback]]): A list of Callbacks which
                will be executed during training. If this is not set,
                currently there are NO default Callbacks.
        """
        # TODO(matt): Set default callbacks.
        self._callbacks = [] if callbacks is None else callbacks

        # Setup executor.
        backend_config = self._get_backend_config(backend)
        self._executor = BackendExecutor(backend_config, num_workers,
                                         num_cpus_per_worker,
                                         num_gpus_per_worker)

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

    def start(self):
        """
        Starts the training execution service.
        """
        self._executor.start()

    def run(self,
            train_func: Callable[[Dict[str, Any]], T],
            config: Optional[Dict[str, Any]] = None) -> List[T]:
        """
        Runs a training function in a distributed manner.

        Args:
            train_func (Callable): The training function to execute.
            config (Optional[Dict]): Configurations to pass into
                ``train_func``. If None then an empty Dict will be created.

        Returns:
            A list of results from the training function. Each value in the
            list corresponds to the value returned by one call of the training
            function.
        """
        config = {} if config is None else config
        return self._executor.run(train_func, config)

    def shutdown(self):
        """
        Shuts down the training execution service.
        """
        self._executor.shutdown()
