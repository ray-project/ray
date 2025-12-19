import abc
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

import gymnasium as gym
import tree  # pip install dm_tree

import ray
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.env.env_errors import StepFailedRecreateEnvError
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.debug import update_global_seed_if_necessary
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import ENV_RESET_TIMER, ENV_STEP_TIMER
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import StateDict, TensorType
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.metrics import Counter

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

logger = logging.getLogger("ray.rllib")

tf1, tf, _ = try_import_tf()

ENV_RESET_FAILURE = "env_reset_failure"
ENV_STEP_FAILURE = "env_step_failure"
NUM_ENV_STEP_FAILURES_LIFETIME = "num_env_step_failures"


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
        self.config: AlgorithmConfig = config.copy(copy_frozen=False)

        self.num_env_steps_sampled_lifetime = 0

        # Get the worker index on which this instance is running.

        # TODO (sven): We should make these c'tor named args.
        self.worker_index: int = kwargs.get("worker_index")
        self.num_workers: int = kwargs.get("num_workers", self.config.num_env_runners)

        self.env = None
        # Create a MetricsLogger object for logging custom stats.
        self.metrics: MetricsLogger = MetricsLogger(
            stats_cls_lookup=config.stats_cls_lookup,
            root=False,
        )

        super().__init__()

        # This eager check is necessary for certain all-framework tests
        # that use tf's eager_mode() context generator.
        if (
            tf1
            and (self.config.framework_str == "tf2" or config.enable_tf1_exec_eagerly)
            and not tf1.executing_eagerly()
        ):
            tf1.enable_eager_execution()

        # Determine actual seed for this particular worker based on worker index AND
        # whether it's an eval worker.
        self._seed: Optional[int] = None
        if self.config.seed is not None:
            self._seed = int(
                self.config.seed
                + (self.worker_index or 0)
                # Eval workers get a +1M seed.
                + (1e6 * self.config.in_evaluation)
            )
        # Seed everything (random, numpy, torch, tf), if `seed` is provided.
        update_global_seed_if_necessary(
            framework=self.config.framework_str,
            seed=self._seed,
        )

        # Ray metrics
        self._metrics_num_try_env_step = Counter(
            name="rllib_env_runner_num_try_env_step_counter",
            description="Number of env.step() calls attempted in this Env Runner.",
            tag_keys=("rllib",),
        )
        self._metrics_num_try_env_step.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_num_env_steps_sampled = Counter(
            name="rllib_env_runner_num_env_steps_sampled_counter",
            description="Number of env steps sampled in this Env Runner.",
            tag_keys=("rllib",),
        )
        self._metrics_num_env_steps_sampled.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

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

    @DeveloperAPI
    def sample_get_state_and_metrics(
        self,
    ) -> Tuple[ray.ObjectRef, StateDict, StateDict]:
        """Convenience method for fast, async algorithms.

        Use this in Algorithms that need to sample Episode lists as ray.ObjectRef, but
        also require (in the same remote call) the metrics and the EnvRunner states,
        except for the module weights.
        """
        _episodes = self.sample()
        # Get the EnvRunner's connector states.
        _connector_states = self.get_state(not_components=COMPONENT_RL_MODULE)
        _metrics = self.get_metrics()
        # Return episode lists by reference so we don't have to send them to the
        # main algo process, but to the Aggregator- or Learner actors directly.
        return ray.put(_episodes), _connector_states, _metrics

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

    def _try_env_reset(
        self,
        *,
        seed: Optional[int] = None,
        options: Optional[dict] = None,
    ) -> Tuple[Any, Any]:
        """Tries resetting the env and - if an error occurs - handles it gracefully.

        Args:
            seed: An optional seed (int) to be passed to the Env.reset() call.
            options: An optional options-dict to be passed to the Env.reset() call.

        Returns:
            The results of calling `Env.reset()`, which is a tuple of observations and
            info dicts.

        Raises:
            Exception: In case `config.restart_failed_sub_environments` is False and
                `Env.reset()` resulted in an error.
        """
        # Try to reset.
        try:
            with self.metrics.log_time(ENV_RESET_TIMER):
                obs, infos = self.env.reset(seed=seed, options=options)
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
                return self._try_env_reset(seed=seed, options=options)
            else:
                raise e

    def _try_env_step(self, actions):
        """Tries stepping the env and - if an error occurs - handles it gracefully."""
        try:
            with self.metrics.log_time(ENV_STEP_TIMER):
                results = self.env.step(actions)
            self._log_env_steps(metric=self._metrics_num_try_env_step, num_steps=1)

            return results
        except Exception as e:
            self.metrics.log_value(
                NUM_ENV_STEP_FAILURES_LIFETIME, 1, reduce="lifetime_sum"
            )

            if self.config.restart_failed_sub_environments:
                if not isinstance(e, StepFailedRecreateEnvError):
                    logger.exception(
                        f"RLlib {self.__class__.__name__}: Environment step failed. Will force reset env(s) in this EnvRunner. The original error is: {e}"
                    )
                # Recreate the env.
                self.make_env()
                # And return that the stepping failed. The caller will then handle
                # specific cleanup operations (for example discarding thus-far collected
                # data and repeating the step attempt).
                return ENV_STEP_FAILURE
            else:
                logger.exception(
                    f"RLlib {self.__class__.__name__}: Environment step failed and "
                    "'config.restart_failed_sub_environments' is False. "
                    "This env will not be recreated. "
                    "Consider setting 'fault_tolerance(restart_failed_sub_environments=True)' in your AlgorithmConfig "
                    "in order to automatically re-create and force-reset an env."
                    f"The original error type: {type(e)}. "
                    f"{e}"
                )
                raise RuntimeError from e

    def _convert_to_tensor(self, struct) -> TensorType:
        """Converts structs to a framework-specific tensor."""

        if self.config.framework_str == "torch":
            return convert_to_torch_tensor(struct)
        else:
            return tree.map_structure(tf.convert_to_tensor, struct)

    def _log_env_steps(self, metric: Counter, num_steps: int) -> None:
        if num_steps > 0:
            metric.inc(value=num_steps)
        else:
            logger.warning(
                f"RLlib {self.__class__.__name__}: Skipping Prometheus logging for metric '{metric.info['name']}'. "
                f"Received num_steps={num_steps}, but the number of steps must be greater than 0."
            )
