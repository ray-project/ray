"""Abstract base class for WorkerGroup implementations.

This module defines the common base class that both V1 and V2 WorkerGroup
implementations should inherit from to ensure backend compatibility.
"""

import abc
from typing import Callable, List, TypeVar

from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI

T = TypeVar("T")


@DeveloperAPI
class BaseWorkerGroup(abc.ABC):
    """Abstract base class for WorkerGroup implementations.

    This base class defines the minimal set of methods that backend classes
    expect from WorkerGroup implementations. Both V1 and V2 WorkerGroup
    classes should inherit from this base class to ensure compatibility with
    all backend configurations.

    The interface focuses on the core operations that backends need:
    - Executing functions on workers
    - Getting worker count and resource allocation
    """

    @abc.abstractmethod
    def execute(self, func: Callable[..., T], *args, **kwargs) -> List[T]:
        """Execute a function on all workers synchronously.

        Args:
            func: The function to execute on each worker.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            A list of results from each worker, in worker rank order.
        """
        pass

    @abc.abstractmethod
    def execute_async(self, func: Callable[..., T], *args, **kwargs) -> List[ObjectRef]:
        """Execute a function on all workers asynchronously.

        Args:
            func: The function to execute on each worker.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            A list of ObjectRef results from each worker, in worker rank order.
        """
        pass

    @abc.abstractmethod
    def execute_single(
        self, worker_index: int, func: Callable[..., T], *args, **kwargs
    ) -> T:
        """Execute a function on a single worker synchronously.

        Args:
            worker_index: The index of the worker to execute on.
            func: The function to execute.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            The result from the specified worker.
        """
        pass

    @abc.abstractmethod
    def execute_single_async(
        self, worker_index: int, func: Callable[..., T], *args, **kwargs
    ) -> ObjectRef:
        """Execute a function on a single worker asynchronously.

        Args:
            worker_index: The index of the worker to execute on.
            func: The function to execute.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            An ObjectRef to the result from the specified worker.
        """
        pass

    @abc.abstractmethod
    def __len__(self) -> int:
        """Return the number of workers in the group."""
        pass

    @abc.abstractmethod
    def get_resources_per_worker(self) -> dict:
        """Get the resources allocated per worker.

        Returns:
            A dictionary mapping resource names to quantities per worker.
            Common keys include "CPU", "GPU", "memory".
        """
        pass
