from typing import Union, Callable, List, TypeVar, Optional, Any, Dict

from ray.util.sgd.v2.backends.backend import BackendConfig
from ray.util.sgd.v2.callbacks.callback import SGDCallback

T = TypeVar("T")
S = TypeVar("S")


class Trainer:
    """A class for enabling seamless distributed deep learning.

    Args:
        backend (Union[str, BackendConfig]): The backend used for
            distributed communication. If configurations are needed,
            a subclass of BackendConfig can be passed in.
            Supported ``str`` values: {"torch"}.
        num_workers (int): The number of workers (Ray actors) to launch.
            Defaults to 1.
        num_cpus_per_worker (float): The number of CPUs to reserve for each
            worker. Fractional values are allowed. Defaults to 1.
        num_gpus_per_worker (float): The number of GPUs to reserve for each
            worker. Fractional values are allowed. Defaults to 0.
        train_cls (Optional[Type]): The training class that each worker
            should be instantiated as.
        callbacks (Optional[List[SGDCallback]]): A list of Callbacks which
            will be executed during training. If this is not set,
            currently there are NO default Callbacks.
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
                Defaults to 1.
            use_gpu (bool): If True, training will be done on GPUs.
                Defaults to False.
            resources_per_worker (Optional[Dict]): If specified, the resources
                defined in the Dict will be reserved for each worker.
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
                each worker when it instantiated.
            train_cls (Optional[cls]): The training class that each worker
                should be instantiated as.
            args, kwargs: The arguments to pass into ``train_cls.__init__``.
        """
        pass

    def run(self,
            train_func: Callable[[Dict[str, Any]], T],
            config: Optional[Dict[str, Any]] = None,
            callbacks: Optional[List[SGDCallback]] = None) -> List[T]:
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
            list corresponds to the value returned by one call of the training
            function.
        """
        pass

    def execute(self, func: Callable[..., T], *args, **kwargs) -> List[T]:
        """Executes a function for all instances of ``self.train_cls``.

        Args:
            func (Callable): The function that should be executed.
                ``self.train_cls`` should be the first argument.
            args, kwargs: The arguments to pass into `func`.
        """
        pass

    def execute_single(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Executes a function on a single instance of ``self.train_cls``.

        Args:
            func (Callable): The function that should be executed.
                ``self.train_cls`` should be the first argument.
            args, kwargs: The arguments to pass into `func`.
        """
        pass

    def shutdown(self):
        """Shuts down the training execution service.

        This will delete any live actors and return any used resources
        back to Ray.
        """
        pass

    def to_tune_trainable(self, train_func: Callable[[Dict[str, Any]], T]):
        """Creates a Tune trainable object.

        Args:
            func (Callable): The function that should be executed on each
                training worker.

        Returns:
            :py:class:`ray.tune.Trainable`"""

        def trainable(config: Dict[str, Any]) -> List[T]:
            pass

        return trainable
