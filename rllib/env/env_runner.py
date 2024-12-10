import abc
import logging
from typing import Any, Dict, Tuple, TYPE_CHECKING

import gymnasium as gym
import tree  # pip install dm_tree

from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import TensorType
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

logger = logging.getLogger("ray.rllib")

tf1, tf, _ = try_import_tf()

ENV_RESET_FAILURE = "env_reset_failure"
ENV_STEP_FAILURE = "env_step_failure"


# TODO (sven): As soon as RolloutWorker is no longer supported, make this base class
#  a Checkpointable. Currently, only some of its subclasses are Checkpointables.
@PublicAPI(stability="alpha")
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
        self.config = config.copy(copy_frozen=False)
        self.env = None

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

    # TODO: Make this an abstract method that must be implemented.
    def make_env(self):
        """Creates the RL environment for this EnvRunner and assigns it to `self.env`.

        Note that users should be able to change the EnvRunner's config (e.g. change
        `self.config.env_config`) and then call this method to create new environments
        with the updated configuration.
        It should also be called after a failure of an earlier env in order to clean up
        the existing env (for example `close()` it), re-create a new one, and then
        continue sampling with that new env.
        """
        pass

    # TODO: Make this an abstract method that must be implemented.
    def make_module(self):
        """Creates the RLModule for this EnvRunner and assigns it to `self.module`.

        Note that users should be able to change the EnvRunner's config (e.g. change
        `self.config.rl_module_spec`) and then call this method to create a new RLModule
        with the updated configuration.
        """
        pass

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

    # TODO (sven): Make this an abstract method that must be overridden.
    def get_metrics(self) -> Any:
        """Returns metrics (in any form) of the thus far collected, completed episodes.

        Returns:
            Metrics of any form.
        """
        pass

    @abc.abstractmethod
    def get_spaces(self) -> Dict[str, Tuple[gym.Space, gym.Space]]:
        """Returns a dict mapping ModuleIDs to 2-tuples of obs- and action space."""

    def stop(self) -> None:
        """Releases all resources used by this EnvRunner.

        For example, when using a gym.Env in this EnvRunner, you should make sure
        that its `close()` method is called.
        """
        pass

    def __del__(self) -> None:
        """If this Actor is deleted, clears all resources used by it."""
        pass

    def _try_env_reset(self):
        """Tries resetting the env and - if an error orrurs - handles it gracefully."""
        # Try to reset.
        try:
            obs, infos = self.env.reset()
            # Everything ok -> return.
            return obs, infos
        # Error.
        except Exception as e:
            # If user wants to simply restart the env -> recreate env and try again
            # (calling this method recursively until success).
            if self.config.restart_failed_sub_environments:
                logger.exception(
                    "Resetting the env resulted in an error! The original error "
                    f"is: {e.args[0]}"
                )
                # Recreate the env and simply try again.
                self.make_env()
                return self._try_env_reset()
            else:
                raise e

    def _try_env_step(self, actions):
        """Tries stepping the env and - if an error orrurs - handles it gracefully."""
        try:
            results = self.env.step(actions)
            return results
        except Exception as e:
            if self.config.restart_failed_sub_environments:
                logger.exception(
                    "Stepping the env resulted in an error! The original error "
                    f"is: {e.args[0]}"
                )
                # Recreate the env.
                self.make_env()
                # And return that the stepping failed. The caller will then handle
                # specific cleanup operations (for example discarding thus-far collected
                # data and repeating the step attempt).
                return ENV_STEP_FAILURE
            else:
                raise e

    def _convert_to_tensor(self, struct) -> TensorType:
        """Converts structs to a framework-specific tensor."""

        if self.config.framework_str == "torch":
            return convert_to_torch_tensor(struct)
        else:
            return tree.map_structure(tf.convert_to_tensor, struct)
