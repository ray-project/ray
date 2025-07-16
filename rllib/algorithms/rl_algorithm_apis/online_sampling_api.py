import abc
import functools
import gymnasium as gym
import ray

from typing import Any, Dict, List, Optional, Set, Tuple, Union

from ray import ObjectRef
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import (
    COMPONENT_ENV_TO_MODULE_CONNECTOR,
    COMPONENT_MODULE_TO_ENV_CONNECTOR,
)

# TODO (simon, sven): Deprecate
from ray.rllib.connectors.module_to_env.module_to_env_pipeline import (
    ModuleToEnvPipeline,
)
from ray.rllib.connectors.env_to_module.env_to_module_pipeline import (
    EnvToModulePipeline,
)
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.env_runner_group import EnvRunnerGroup
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS
from ray.rllib.utils.typing import EnvCreator, EnvType

# TODO (simon): Maybe rename after old stack deprecation to `MetricsDict`.
from ray.rllib.utils.typing import ResultDict
from ray.tune.registry import ENV_CREATOR, _global_registry


class OnlineSamplingAPI(abc.ABC):
    """Base API for rollout functionalities.

    This base api defines an `EnvRunnerGroup` and multiple related
    attributes and methods to rollout an `RLModule` and collect experiences
    in environments.

    Next to the `EnvRunnerGroup` it contains a local `EnvRunner` if
    the config requests it, the `EnvToModule` and the `ModuleToEnv`
    pipelines, and the corresponding spaces used in the environment
    the pipelines and the `RLModule`.

    Note, only concrete apis can be inherited from. Furthermore,
    the "apis-to-the-left"-rule must be applied, i.e. any api
    needs to be left of `RLAlgorithm`. Finally, any derived method
    should always call `super()`.
    """

    # The sampling metrics.
    _env_runner_metrics: Dict[str, Any] = {}
    # The `EnvRunnerGroup` could be customized.
    _env_runner_group: EnvRunnerGroup = None
    # Also enable a local `EnvRunner`, if needed
    _local_env_runner: Union[MultiAgentEnvRunner, SingleAgentEnvRunner] = None

    # The spaces for environment, pipelines, and `RLModule`.
    _spaces: Dict[str, Any] = None
    # The pipelines for state synching.
    _env_to_module: EnvToModulePipeline = None
    _module_to_env: ModuleToEnvPipeline = None

    def __init__(self, config: AlgorithmConfig, **kwargs):
        """Initializes an `OnlineSamplingAPI`."""
        # Call the super's method.
        super().__init__(config=config, **kwargs)

    abc.abstractmethod

    def _setup(self, config: AlgorithmConfig):
        """Abstract method to setup the specific `EnvRunner`s."""
        super()._setup(config=config)

    abc.abstractmethod

    def get_metrics(self, metrics: ResultDict, **kwargs):
        super().get_metrics(metrics=metrics, **kwargs)

    abc.abstractmethod

    def sample(
        self,
        env_steps: Optional[int] = None,
        agent_steps: Optional[int] = None,
        **kwargs: Dict[str, Any],
    ) -> Tuple[List[Union[MultiAgentEpisode, SingleAgentEpisode]], ResultDict]:
        """Samples experiences from `EnvRunner`s."""
        pass

    abc.abstractmethod

    def sync(self, state, **kwargs):
        """Syncs states between `EnvRunner`s and `RLAgorithm`."""
        # TODO (sven, simon): This is bad style to make a mixin
        #   dependent on another one. Better give it a state.
        # NOTE (sven, simon): We could set this method to an abstract one, but then
        # users would always need to implement this for their custom classes (instead
        # of simply overriding the inner `EnvRunnerGroup` class).
        self._env_runner_group.sync_weights(
            state, from_worker_or_learner_group=kwargs.get("learner_group")
        )
        self._env_runner_group.sync_env_runner_states(
            config=self.config,
            env_to_module=self._env_to_module,
            module_to_env=self._module_to_env,
        )

    abc.abstractmethod

    def cleanup(self):
        """Pulls down all `EnvRunner`s."""
        # NOTE (simon, sven): Same here. Abstract would mean any custom class not
        # directly inherited from one of our provided one (SyncExperienceSampling or
        # `AsyncExperienceSampling`) needs to provide the logic for this.
        self._env_runner_group.stop()
        self._env_runner_group = None
        self._local_env_runner = None
        self._spaces = None
        self._env_to_module = None
        self._module_to_env = None
        super().cleanup()

    # TODO (simon, sven): If this is a static method we could also move this
    #   for better readability into a non-class function and import.
    #   Maybe even better: Move it into `EnvRunnerGroup` the only class that
    #   needs to use it.
    @staticmethod
    def _get_env_id_and_creator(
        env_specifier: Union[str, EnvType, None], config: AlgorithmConfig
    ) -> Tuple[Optional[str], EnvCreator]:
        """Returns env_id and creator callable given original env id from config.

        Args:
            env_specifier: An env class, an already tune registered env ID, a known
                gym env name, or None (if no env is used).
            config: The AlgorithmConfig object.

        Returns:
            Tuple consisting of a) env ID string and b) env creator callable.
        """
        # Environment is specified via a string.
        if isinstance(env_specifier, str):
            # An already registered env.
            if _global_registry.contains(ENV_CREATOR, env_specifier):
                return env_specifier, _global_registry.get(ENV_CREATOR, env_specifier)

            # A class path specifier.
            elif "." in env_specifier:

                def env_creator_from_classpath(env_context):
                    try:
                        env_obj = from_config(env_specifier, env_context)
                    except ValueError:
                        raise EnvError(
                            ERR_MSG_INVALID_ENV_DESCRIPTOR.format(env_specifier)
                        )
                    return env_obj

                return env_specifier, env_creator_from_classpath
            # Try gym/PyBullet.
            else:
                return env_specifier, functools.partial(
                    _gym_env_creator, env_descriptor=env_specifier
                )

        elif isinstance(env_specifier, type):
            env_id = env_specifier  # .__name__

            if config["remote_worker_envs"]:
                # Check gym version (0.22 or higher?).
                # If > 0.21, can't perform auto-wrapping of the given class as this
                # would lead to a pickle error.
                gym_version = importlib.metadata.version("gym")
                if version.parse(gym_version) >= version.parse("0.22"):
                    raise ValueError(
                        "Cannot specify a gym.Env class via `config.env` while setting "
                        "`config.remote_worker_env=True` AND your gym version is >= "
                        "0.22! Try installing an older version of gym or set `config."
                        "remote_worker_env=False`."
                    )

                @ray.remote(num_cpus=1)
                class _wrapper(env_specifier):
                    # Add convenience `_get_spaces` and `_is_multi_agent`
                    # methods:
                    def _get_spaces(self):
                        return self.observation_space, self.action_space

                    def _is_multi_agent(self):
                        from ray.rllib.env.multi_agent_env import MultiAgentEnv

                        return isinstance(self, MultiAgentEnv)

                return env_id, lambda cfg: _wrapper.remote(cfg)
            # gym.Env-subclass: Also go through our RLlib gym-creator.
            elif issubclass(env_specifier, gym.Env):
                return env_id, functools.partial(
                    _gym_env_creator,
                    env_descriptor=env_specifier,
                )
            # All other env classes: Call c'tor directly.
            else:
                return env_id, lambda cfg: env_specifier(cfg)

        # No env -> Env creator always returns None.
        elif env_specifier is None:
            return None, lambda env_config: None

        else:
            raise ValueError(
                "{} is an invalid env specifier. ".format(env_specifier)
                + "You can specify a custom env as either a class "
                '(e.g., YourEnvCls) or a registered env id (e.g., "your_env").'
            )

    @staticmethod
    def validate_env(env: EnvType, env_context: EnvContext) -> None:
        """Env validator function for this Algorithm class.

        Override this in child classes to define custom validation
        behavior.

        Args:
            env: The (sub-)environment to validate. This is normally a
                single sub-environment (e.g. a gym.Env) within a vectorized
                setup.
            env_context: The EnvContext to configure the environment.

        Raises:
            Exception: in case something is wrong with the given environment.
        """
        pass


class SyncOnlineSamplingAPI(OnlineSamplingAPI):
    """A concrete `OnlineSamplingMixin` to sample sychronously."""

    class SyncEnvRunnerGroup(EnvRunnerGroup):
        def __init__(
            self,
            config: AlgorithmConfig,
        ):
            # TODO (simon, sven): As described above, this goes at best into
            # the EnvRunnerGroup itself.
            _, env_creator = SyncOnlineSamplingAPI._get_env_id_and_creator(
                env_specifier=config.env,
                config=config,
            )

            # Initialiaze the `EnvRunnerGroup`.
            super().__init__(
                env_creator=env_creator,
                validate_env=SyncOnlineSamplingAPI.validate_env,
                # TODO (simon, sven): Will be deprecated with the old stack.
                default_policy_class=None,
                config=config,
                local_env_runner=config.create_env_on_local_worker,
            )

        def sample(
            self,
            env_steps: Optional[int] = None,
            agent_steps: Optional[int] = None,
            **kwargs: Dict[str, Any],
        ) -> Tuple[List[Union[MultiAgentEpisode, SingleAgentEpisode]], ResultDict]:
            """Samples synchronously from `EnvRunner`s."""
            # TODO (simon): Move logic from function to here.
            return synchronous_parallel_sample(
                worker_set=self,
                max_agent_steps=agent_steps,
                max_env_steps=env_steps,
                sample_timeout_s=self._local_config.sample_timeout_s,
                # TODO (should be kwargs).
                concat=False,
                _uses_new_env_runners=True,
                **kwargs,
                # TODO (simon): Maybe in kwargs. If a MetricsMixin is there
                # it should set this in the algorithm to always False and
                # collect metrics extra. Otherwise collecting metrics via
                # Actors would not work.
                #   Maybe we set a config parameter `config._return_metrics`.
                # _return_metrics=True,
            )

    def __init__(self, config: AlgorithmConfig, **kwargs):
        """Initializes a `SyncOnlineSamplingAPI`."""
        # Call `super`'s method.
        super().__init__(config=config, **kwargs)

    @override(OnlineSamplingAPI)
    def _setup(self, config: AlgorithmConfig):
        """Sets up a `SyncOnlineSamplingAPI`."""
        # Note, the logger is defined in the `RLAlgorithm`.
        self.logger.info(f"Setup SyncOnlineSamplingAPI ... ")
        # Setup here the customized `SyncEnvRunnerGroup`.
        self._env_runner_group = self.SyncEnvRunnerGroup(config)

        # Define the spaces.
        if self._env_runner_group.local_env_runner:
            self._local_env_runner = self._env_runner_group.local_env_runner
            # TODO (sven): Get these spaces from a central location (RLAlgorithm)
            #   single source of truth.
            self._spaces = self._local_env_runner.get_spaces()
        else:
            self._spaces = self._env_runner_group.get_spaces()
        # Setup the connectors (needed for synching states).
        if self._local_env_runner is None and self._spaces is not None:
            self._env_to_module = self.config.build_env_to_module_connector(
                spaces=self._spaces
            )
            self._module_to_env = self.config.build_module_to_env_connector(
                spaces=self._spaces
            )
        # Setup the `super`.
        super()._setup(config=config)
        # ...

    @override(OnlineSamplingAPI)
    def sample(
        self,
        env_steps: Optional[int] = None,
        agent_steps: Optional[int] = None,
        **kwargs: Dict[str, Any],
    ) -> Tuple[List[Union[MultiAgentEpisode, SingleAgentEpisode]], ResultDict]:
        # TODO (sven, simon): Maybe turn to class attribute set by config.
        _return_metrics = kwargs.get("_return_metrics", True)
        # TODO (sven, simon): Either
        #   (1) we pull the metrics here already and just store them locally (will
        #       be more correpsonding to the sample).
        #   (2) Or, we pull the metrics extra when the MetricsAPI calls `get_metrics`.
        #   Here I implement the first (might be more performant).
        episodes, self._env_runner_metrics = self._env_runner_group.sample(
            env_steps,
            agent_steps,
            **kwargs,
        )

        if _return_metrics:
            return episodes, self._env_runner_metrics
        else:
            return episodes

    @override(OnlineSamplingAPI)
    def get_metrics(self, metrics: ResultDict, **kwargs):
        metrics.update({ENV_RUNNER_RESULTS: self._env_runner_metrics})


class AsyncOnlineSamplingAPI(OnlineSamplingAPI):
    class AsyncEnvRunnerGroup(EnvRunnerGroup):
        def sample(
            self,
            env_steps: int,
            agent_steps: int,
            **kwargs: Dict[str, Any],
        ) -> Tuple[List[ObjectRef], Dict[str, Any], Dict[str, Any], Set[int]]:

            env_runner_indices_to_update = set()
            num_healthy_remote_workers = (
                self._env_runner_group.num_healthy_remote_workers()
            )
            if num_healthy_remote_workers > 0:
                async_results: List[
                    Tuple[int, ObjectRef]
                ] = self.env_runner_group.fetch_ready_async_reqs(
                    timeout_seconds=self.config.timeout_s_sampler_manager,
                    return_obj_refs=False,
                )
                self.env_runner_group.foreach_env_runner_async(
                    "sample_get_state_and_metrics"
                )

                # Get results from the n different async calls and store those EnvRunner
                # indices we should update.
                results = []
                for r in async_results:
                    env_runner_indices_to_update.add(r[0])
                    results.append(r[1])

                for (episodes, states, metrics) in results:
                    episode_refs.append(episodes)
                    connector_states.append(states)
                    env_runner_metrics.append(metrics)
            # Sample from the local EnvRunner.
            else:
                episodes = self._local_env_runner.sample()
                env_runner_metrics = [self._local_env_runner.get_metrics()]
                episode_refs = [ray.put(episodes)]
                connector_states = [
                    self.env_runner.get_state(
                        components=[
                            COMPONENT_ENV_TO_MODULE_CONNECTOR,
                            COMPONENT_MODULE_TO_ENV_CONNECTOR,
                        ]
                    )
                ]

            return (
                episode_refs,
                connector_states,
                env_runner_metrics,
                env_runner_indices_to_update,
            )

    @override(OnlineSamplingAPI)
    def _setup(self, config: AlgorithmConfig):
        self._env_runner_group = self.AsyncEnvRunnerGroup(config)
        # To cover for failed remote workers this mixin always provides
        # the local `EnvRunner`.
        self._local_env_runner = (
            MultiAgentEnvRunner(config)
            if config.is_multi_agent
            else SingleAgentEnvRunner(config)
        )
        # ...

    @override(OnlineSamplingAPI)
    def sample(
        self, env_steps: int, agent_steps: int, **kwargs: Dict[str, Any]
    ) -> List[SingleAgentEpisode]:
        return self._env_runner_group.sample(env_steps, agent_steps)
