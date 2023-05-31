import abc
import platform
from typing import Any, Dict, Optional, Union

import ray
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.annotations import ExperimentalAPI


@ExperimentalAPI
class EnvRunner(FaultAwareApply, metaclass=abc.ABCMeta):
    """Base class for distributed RL-style data collection from an environment.

    The EnvRunner API's core functionalities can be summarized as:
    - Gets configured via passing a AlgorithmConfig object to the constructor.
    Normally, subclasses of EnvRunner then construct their own environment (possibly
    vectorized) copies and RLModules/Policies and use the latter to step through the
    environment in order to collect training data.
    - Clients of EnvRunner can use the `sample()` method to collect data for training
    from the environment(s).
    - EnvRunner offers parallelism via creating n remote Ray Actors based on this class.
    Use the `as_remote()` method to create the corresponding Ray remote class. Then
    instantiate n Actors using the Ray `[ctor].remote(...)` syntax.
    - EnvRunner clients can get information about the server/node on which the
    individual Actors are running.
    """

    @classmethod
    def as_remote(
        cls,
        num_cpus: Optional[int] = None,
        num_gpus: Optional[Union[int, float]] = None,
        memory: Optional[int] = None,
        resources: Optional[dict] = None,
        max_num_worker_restarts: int = 0,
    ) -> type:
        """Returns the EnvRunner class as a `@ray.remote` using given options.

        The returned class can then be used to instantiate ray actors.

        Examples:
            >>> remote_cls = EnvRunner.as_remote(num_cpus=1, num_gpus=0)
            >>> array = [remote_cls.remote({"key": "value"}) for _ in range(10)]
            >>> results = [env_runner.sample.remote() for env_runner in array]

        Args:
            num_cpus: The number of CPUs to allocate for the remote actor.
            num_gpus: The number of GPUs to allocate for the remote actor.
                This could be a fraction as well.
            memory: The heap memory request in bytes for this task/actor,
                rounded down to the nearest integer.
            resources: The default custom resources to allocate for the remote
                actor.
            max_num_worker_restarts: The maximum number of restarts a failed EnvRunner
                will undergo. Note that this setting is only effective if the EnvRunner
                is a) a Ray Actor and b) managed by a fault-tolerant
                ray.rllib.utils.actor_manager::FaultTolerantActorManager.

        Returns:
            The `@ray.remote` decorated EnvRunner class.
        """
        return ray.remote(
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            resources=resources,
            # Automatically restart failed EnvRunners.
            max_restarts=max_num_worker_restarts,
        )(cls)

    def __init__(self, *, config, **kwargs):
        """Initializes an EnvRunner instance.

        Args:
            config: The config to use to setup this EnvRunner.
            **kwargs: Forward compatibility kwargs.
        """
        self.config = config
        super().__init__(**kwargs)

    @abc.abstractmethod
    def assert_healthy(self):
        """Checks that self.__init__() has been completed properly.

        Useful in case an `EnvRunner` is run as @ray.remote (Actor) and the owner
        would like to make sure the Ray Actor has been properly initialized.

        Raises:
            AssertionError: If the EnvRunner Actor has NOT been properly initialized.
        """

    @abc.abstractmethod
    def sample(self, **kwargs) -> Any:
        """Returns experiences (of any form) sampled from this EnvRunner.

        The exact nature and size of collected data are defined via the EnvRunner's
        config and may be overridden by the given arguments.

        Args:
            **kwargs: Forward compatibility kwargs.

        Returns:
            The collected experience in any form.
        """

    def get_state(self) -> Dict[str, Any]:
        """Returns this EnvRunner's (possibly serialized) current state as a dict.

        Returns:
            The current state of this EnvRunner.
        """
        return {}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores this EnvRunner's state from the given state dict.

        Args:
            state: The state dict to restore the state from.

        Examples:
            >>> from ray.rllib.evaluation.rollout_worker import RolloutWorker
            >>> # Create a RolloutWorker.
            >>> worker = ... # doctest: +SKIP
            >>> state = worker.get_state() # doctest: +SKIP
            >>> new_worker = RolloutWorker(...) # doctest: +SKIP
            >>> new_worker.set_state(state) # doctest: +SKIP
        """
        pass

    def stop(self) -> None:
        """Releases all resources used by this EnvRunner."""
        pass

    def __del__(self) -> None:
        """If this Actor is deleted, clears all resources used by it."""
        pass
