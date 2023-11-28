import gymnasium as gym

from collections import defaultdict
from functools import partial
from typing import Dict, List, Optional, TYPE_CHECKING, Union

from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.utils.annotations import override
from ray.tune.registry import ENV_CREATOR, _global_registry

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

    # TODO (sven): This gives a tricky circular import that goes
    #  deep into the library. We have to see, where to dissolve it.
    from ray.rllib.env.multi_agent_episode import MultiAgentEpisode


class MultiAgentEnvRunner(EnvRunner):
    """The genetic environment runner for the multi agent case."""

    @override(EnvRunner)
    def __init__(self, config: "AlgorithmConfig", **kwargs):
        super().__init__(config=config)

        # Get the worker index on which this instance is running.
        self.worker_index: int = kwargs.get("worker_index")

        # Register env for the local context.
        # Note, `gym.register` has to be called on each worker.
        if isinstance(self.config.env, str) and _global_registry.contains(
            ENV_CREATOR, self.config.env
        ):
            entry_point = partial(
                _global_registry.get(ENV_CREATOR, self.config.env),
                self.config.env_config,
            )

        else:
            entry_point = partial(
                _gym_env_creator,
                env_context=self.config.env_config,
                env_descriptor=self.config.env,
            )
        gym.register("rllib-multi-agent-env-runner-v0", entry_point=entry_point)

        # Wrap into `VectorListInfo`` wrapper to get infos as lists.
        self.env: gym.Wrapper = gym.wrappers.VectorListInfo(
            gym.vector.make(
                "rllib-multi-agent-env-runner-v0",
                num_envs=self.config.num_envs_per_worker,
                asynchronous=self.config.remote_worker_envs,
            )
        )

        # Create the vectorized gymnasium env.
        assert isinstance(self.env.envs[0].unwrapped, MultiAgentEnv), (
            "ERROR: When using the `MultiAgentEnvRunner` the environment needs "
            "to inherit from `ray.rllib.env.multi_agent_env.MultiAgentEnv`."
        )

        self.num_envs: int = self.env.num_envs
        self.agent_ids: List[Union[str, int]] = self.env.envs[0].get_agent_ids()
        assert self.num_envs == self.config.num_envs_per_worker

        # Create our own instance of the (single-agent) `RLModule` (which
        # the needs to be weight-synched) each iteration.
        # TODO (sven, simon): We have to rebuild the `AlgorithmConfig` to work on
        # `RLModule`s and not `Policy`s. Like here `policies`->`modules`
        module_spec: MultiAgentRLModuleSpec = self.config.get_marl_module_spec(
            policy_dict=config.policies
        )

        # TODO (simon): The `gym.Wrapper` for `gym.vector.VectorEnv` should
        #  actually hold the spaces for a single env, but for boxes the
        #  shape is (1, 1) which brings a problem with the action dists.
        #  shape=(1,) is expected.
        module_spec.action_space = self.env.envs[0].action_space
        module_spec.observation_space = self.env.envs[0].observation_space
        # Set action and observation spaces for all module specs.
        for agent_module_spec in module_spec.module_specs.values():
            agent_module_spec.observation_space = self.env.envs[0].observation_space
            agent_module_spec.action_space = self.env.envs[0].action_space
        # Build the module from its spec.
        self.module: MultiAgentRLModule = module_spec.build()

        # This should be the default.
        self._needs_initial_reset: bool = True
        self._episodes: List[Optional["MultiAgentEpisode"]] = [
            None for _ in range(self.num_envs)
        ]

        self._done_episodes_for_metrics: List["MultiAgentEpisode"] = []
        self._ongoing_episodes_for_metrics: Dict[List] = defaultdict(list)
        self._ts_since_last_metrics: int = 0
        self._weights_seq_no: int = 0

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List["MultiAgentEpisode"]:
        """Runs and returns a sample (n timesteps or m episodes) on the env(s)."""

        # If npt execution details are provided, use the configf.
        if num_timesteps is None and num_episodes is None:
            if self.config.batch_mode == "truncate_episodes":
                num_timesteps = (
                    self.config.get_rollout_fragment_length(
                        worker_index=self.worker_index,
                    )
                    * self.num_envs
                )
            else:
                num_episodes = self.num_envs

        # Sample n timesteps
        if num_timesteps is not None:
            return self._sample_timesteps(
                num_timesteps=num_timesteps,
                explore=explore,
                random_actions=random_actions,
                force_reset=False,
            )
        # Sample m episodes.
        else:
            return self._sample_episodes(
                num_episodes=num_episodes,
                explore=explore,
                random_actions=random_actions,
                with_render_data=with_render_data,
            )

    def _sample_timesteps(
        self,
        num_timesteps: int,
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List["MultiAgentEpisode"]:
        """Helper method to sample n timesteps."""

        # TODO (sven): This gives a tricky circular import that goes
        # deep into the library. We have to see, where to dissolve it.
        # from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
        pass

    def _sample_episodes(
        self,
        num_episodes: int,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List["MultiAgentEpisode"]:
        """Helper method to run n episodes.

        See docstring of `self.sample()` for more details.
        """

        # TODO (sven): This gives a tricky circular import that goes
        # deep into the library. We have to see, where to dissolve it.
        # from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
        pass

    @override(EnvRunner)
    def assert_healthy(self):
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and self.module
