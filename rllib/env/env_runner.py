import abc
from typing import Any, Dict, TYPE_CHECKING

from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.framework import try_import_tf

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

tf1, _, _ = try_import_tf()


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
    Use `ray.remote([resources])(EnvRunner)` method to create the corresponding Ray
    remote class. Then instantiate n Actors using the Ray `[ctor].remote(...)` syntax.
    - EnvRunner clients can get information about the server/node on which the
    individual Actors are running.
    """

    def __init__(self, *, config: "AlgorithmConfig", **kwargs):
        """Initializes an EnvRunner instance.

        Args:
            config: The AlgorithmConfig to use to setup this EnvRunner.
            **kwargs: Forward compatibility kwargs.
        """
        self.config = config
        super().__init__(**kwargs)

        # This eager check is necessary for certain all-framework tests
        # that use tf's eager_mode() context generator.
        if (
            tf1
            and (self.config.framework_str == "tf2" or config.enable_tf1_exec_eagerly)
            and not tf1.executing_eagerly()
        ):
            tf1.enable_eager_execution()

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

        .. testcode::
            :skipif: True

            from ray.rllib.env.env_runner import EnvRunner
            env_runner = ...
            state = env_runner.get_state()
            new_runner = EnvRunner(...)
            new_runner.set_state(state)
        """
        pass

    def stop(self) -> None:
        """Releases all resources used by this EnvRunner."""
        pass

    def __del__(self) -> None:
        """If this Actor is deleted, clears all resources used by it."""
        pass
