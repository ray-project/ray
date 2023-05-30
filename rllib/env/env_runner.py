import abc
import platform
from typing import Any, Optional, Union

import ray
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
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
    def sample(
        self,
        *,
        num_timesteps: Optional[int] = None,
        num_episodes: Optional[int] = None,
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
        **kwargs,
    ) -> Any:
        """Returns a batch of experience sampled from this EnvRunner.

        The exact nature and size of collected batches are defined via the EnvRunner's
        config and the given arguments.

        Args:
            num_timesteps: If provided, will step exactly this number of timesteps
                through the environment. Note that only one or none of `num_timesteps`
                and `num_episodes` may be provided, but never both. If both
                `num_timesteps` and `num_episodes` are None, will determine how to
                sample via `self.config`.
            num_episodes: If provided, will step through the env(s) until exactly this
                many episodes have been completed. Note that only one or none of
                `num_timesteps` and `num_episodes` may be provided, but never both.
                If both `num_timesteps` and `num_episodes` are None, will determine how
                to sample via `self.config`.
            explore: Whether to use some exploration strategy to determine the actions
                to send to the env. Only has an effect if `random_actions` is False.
            random_actions: Whether to act completely randomly in the env. If True, the
                value of `explore` has no effect.
            force_reset: If True, will force-reset the env at the very beginning and
                thus begin sampling from freshly started episodes.

        Returns:
            A batch of data of any form.
        """

    def stop(self) -> None:
        """Releases all resources used by this EnvRunner."""
        pass

    def __del__(self) -> None:
        """If this Actor is deleted, clears all resources used by it."""
        self.stop()

    def get_host(self) -> str:
        """Returns the hostname of the process running this Actor."""
        return platform.node()

    def get_node_ip(self) -> str:
        """Returns the IP address of the node that this Actor runs on."""
        return ray.util.get_node_ip_address()

    def find_free_port(self) -> int:
        """Finds a free port on the node that this Actor runs on."""
        from ray.air._internal.util import find_free_port

        return find_free_port()
