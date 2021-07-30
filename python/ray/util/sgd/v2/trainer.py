from typing import Union, Callable, List, TypeVar, Optional, Any, Dict

from ray.util.sgd.v2.backends.backend import BackendConfig
from ray.util.sgd.v2.callbacks.callback import Callback

T = TypeVar("T")
S = TypeVar("S")


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
        pass

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
        pass

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
        pass

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
        pass

    def to_tune_trainable(self, train_func: Callable[[Dict[str, Any]], T]
                          ) -> Callable[[Dict[str, Any]], List[T]]:
        """Creates a Tune trainable function."""

        def trainable(config: Dict[str, Any]) -> List[T]:
            pass

        return trainable
