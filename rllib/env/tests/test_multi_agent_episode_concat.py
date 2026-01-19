from typing import Any, Callable, Dict, Optional, Tuple

import gymnasium as gym

from ray.rllib import MultiAgentEnv
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import MultiAgentDict
from ray.tune import register_env


class MultiAgentCountingEnv(MultiAgentEnv):
    def __init__(
        self, agent_fns: dict[str, Callable[[int], bool]], max_episode_length: int = 100
    ):
        super().__init__()

        self.agents = list(agent_fns.keys())
        self.possible_agents = list(agent_fns.keys())
        self.agent_fns = agent_fns

        self.observation_space = gym.spaces.Dict(
            {
                agent: gym.spaces.Discrete(max_episode_length)
                for agent in self.possible_agents
            }
        )
        self.action_space = gym.spaces.Dict(
            {
                agent: gym.spaces.Discrete(max_episode_length)
                for agent in self.possible_agents
            }
        )

        self.agent_timestep = {}
        self.env_timestep = 0
        self.max_episode_length = max_episode_length

    def reset(
        self,
        *,
        seed: Optional[int] = None,
        options: Optional[dict] = None,
    ) -> Tuple[MultiAgentDict, MultiAgentDict]:
        self.env_timestep = 0
        self.agent_timestep = {agent: 0 for agent in self.possible_agents}

        obs = self.get_obs()
        return obs, {agent_id: {"env_timestep": self.env_timestep} for agent_id in obs}

    def step(
        self, action_dict: MultiAgentDict
    ) -> Tuple[
        MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict
    ]:
        self.env_timestep += 1

        if self.env_timestep >= self.max_episode_length:
            obs = self.agent_timestep
        else:
            obs = self.get_obs()

        # replace 1 with self.env_timestep for debugging to see what time that the observation is from
        rewards = {agent: 1 for agent in obs.keys()}
        terminated = {"__all__": self.env_timestep == self.max_episode_length}
        truncated = {}
        info = {agent: {"env_timestep": self.env_timestep} for agent in obs.keys()}

        return obs, rewards, terminated, truncated, info

    def get_obs(self) -> dict[str, int]:
        obs = {}
        for agent, fn in self.agent_fns.items():
            if fn(self.env_timestep):
                obs[agent] = self.agent_timestep[agent]
                self.agent_timestep[agent] += 1

        assert obs, f"{obs=}, {self.env_timestep=}, {self.agent_timestep=}"
        return obs


class EchoRLModule(RLModule):
    """An RLModule that returns the observation as the action (for testing)."""

    framework = "torch"

    @override(RLModule)
    def _forward(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Return the observation as the action."""
        obs = batch[Columns.OBS]
        # For Discrete observation space, obs is already an integer/array of integers
        return {Columns.ACTIONS: obs}

    @override(RLModule)
    def _forward_inference(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return self._forward(batch, **kwargs)

    @override(RLModule)
    def _forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return self._forward(batch, **kwargs)

    @override(RLModule)
    def _forward_train(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        raise NotImplementedError("EchoRLModule is not trainable!")


class ObservationIncrementValidator(ConnectorV2):
    """Connector that validates observations increment by 1 for each agent."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Track last observation per agent (agent_id -> last_obs)
        self._last_obs_per_agent = {}

    def __call__(
        self,
        *,
        rl_module,
        batch,
        episodes,
        explore=None,
        shared_data=None,
        **kwargs,
    ):
        # Iterate over all single-agent episodes (handles multi-agent automatically)
        for sa_episode, agent_id, module_id in self.single_agent_episode_iterator(
            episodes, agents_that_stepped_only=True
        ):
            # Get the current observation
            current_obs = sa_episode.get_observations(-1)

            if agent_id in self._last_obs_per_agent:
                expected_obs = self._last_obs_per_agent[agent_id] + 1
                if current_obs != expected_obs:
                    raise AssertionError(
                        f"Agent {agent_id}: Expected obs {expected_obs}, got {current_obs}"
                    )

            # Update last observed
            self._last_obs_per_agent[agent_id] = current_obs

        return batch


AGENT_FNS = {
    "p_true": lambda x: True,
    "p_mod_2": lambda x: x % 2 == 0,
    "p_mod_3+": lambda x: x % 3 == 0 and x > 0,
    "p_in": lambda x: x in [2, 12, 18, 20],
}
MAX_EPISODE_LENGTH = 20

# Sample 8 timesteps
# Env Time: 0 1 2 3 4 5 6 7 8 | 9 10 11 12 13 14 15 16 | 17 18 19 20 | 0 1 2 3
#  Agents  -------------------|------------------------|-------------|--------
# p_true  : 0 1 2 3 4 5 6 7 8 | 9 10 11 12 13 14 15 16 | 17 18 19 20 | 0 1 2 3
# p_mod_2 : 0 - 1 - 2 - 3 - 4 | -  5  -  6  -  7  -  8 |  -  9  - 10 | 0 - 1 -
# p_mod_5+: - - - 0 - - 1 - - | 2  -  -  3  -  -  4  - |  -  5  -  - | - - - 0
# p_in    : - - 0 - - - - - - | -  -  -  1  -  -  -  - |  -  2  -  3 | - - 0 -


def create_env(config):
    return MultiAgentCountingEnv(AGENT_FNS, max_episode_length=MAX_EPISODE_LENGTH)


register_env("env", create_env)


CONFIG = (
    PPOConfig()
    .environment("env")
    .env_runners(
        num_envs_per_env_runner=1,
        num_env_runners=0,
        # env_to_module_connector=lambda env: ObservationIncrementValidator(),
    )
    .rl_module(rl_module_spec=RLModuleSpec(module_class=EchoRLModule))
    .multi_agent(
        policies={"p0"},
        policy_mapping_fn=lambda aid, eps, **kw: "p0",
        policies_to_train=[],
    )
)


def test_multi_agent_episode_concat(num_timesteps=8):
    env_runner = MultiAgentEnvRunner(CONFIG)

    episodes = []
    for repeat in range(10):
        print("SAMPLE")
        new_episodes = env_runner.sample(
            num_timesteps=num_timesteps, random_actions=False
        )
        episodes += new_episodes

        # ADD TESTING FOR INDIVIDUAL EPS CHUNK THAT THE DATA IS CORRECT
        for ep in new_episodes:
            print(
                f"{ep.id_}\n"
                f"\t{ep.env_t_started=}, {ep.env_t=}\n"
                f"\t{ep._len_lookback_buffers=}\n"
                f"\t{ep.agent_t_started=}"
            )
            for agent_id, sa_episode in ep.agent_episodes.items():
                obs = sa_episode.get_observations()
                actions = sa_episode.get_actions()
                rewards = sa_episode.get_rewards()
                infos = sa_episode.get_infos()

                # print(f"\n\t{agent_id=}")
                # print(f"\t\t{sa_episode.t_started=}, {sa_episode.t=}")
                # print(f'\t\t{obs=}')
                # print(f'\t\t{actions=}')
                # print(f'\t\t{rewards=}')
                # print(f'\t\t{infos=}')

                # Check that the obs, actions and rewards are the expected values
                assert list(obs) == list(
                    range(sa_episode.t_started, sa_episode.t + 1)
                ), f"{obs=}, expected-obs={list(range(sa_episode.t_started, sa_episode.t + 1))}, {sa_episode.observations=}"
                assert list(actions) == list(range(sa_episode.t_started, sa_episode.t))
                assert list(rewards) == [1] * (sa_episode.t - sa_episode.t_started)

                # Check that the info is the same length as the observations
                assert len(list(infos)) == len(list(obs))

                env_t_to_agent_t = ep.env_t_to_agent_t[agent_id].get()
                # print(
                #     f"\t\t{env_t_to_agent_t=}, lookback={ep.env_t_to_agent_t[agent_id].lookback}"
                # )
                assert (
                    len(env_t_to_agent_t) == ep.env_t + 1 - ep.env_t_started
                ), f"AssertationError for length of env_t_to_agent_t: {len(env_t_to_agent_t)} ({env_t_to_agent_t}) != {ep.env_t + 1 - ep.env_t_started}"

                # Calculate the agent_t for the first observation in this chunk
                # by counting how many times the agent was active before env_t_started
                agent_t_at_chunk_start = sum(
                    1 for t in range(ep.env_t_started) if AGENT_FNS[agent_id](t)
                )
                agent_t = agent_t_at_chunk_start
                expected_env_t_to_agent_t = []
                for env_t in range(ep.env_t_started, ep.env_t + 1):
                    if AGENT_FNS[agent_id](env_t):
                        expected_env_t_to_agent_t.append(agent_t)
                        agent_t += 1
                    else:
                        expected_env_t_to_agent_t.append(
                            MultiAgentEpisode.SKIP_ENV_TS_TAG
                        )
                # print(f'\t\t{expected_env_t_to_agent_t=}')
                assert (
                    list(env_t_to_agent_t) == expected_env_t_to_agent_t
                ), f"AssertationError for env_t_to_agent_t data: {list(env_t_to_agent_t)} != {expected_env_t_to_agent_t}"

                # You should have the same number of info as obs
                # The info timesteps should equal to the non-skip timesteps
                # Calculate lookback env_ts (env_t values where agent was active before this chunk)
                all_lookback_env_ts = [
                    env_t
                    for env_t in range(ep.env_t_started)
                    if AGENT_FNS[agent_id](env_t)
                ]
                # Calculate how many observations are in lookback vs current chunk
                total_obs = sa_episode.t - sa_episode.t_started + 1
                current_chunk_obs = sum(
                    1
                    for t in env_t_to_agent_t
                    if t != MultiAgentEpisode.SKIP_ENV_TS_TAG
                )
                lookback_obs_count = total_obs - current_chunk_obs
                # Take only the last lookback_obs_count env_ts from lookback
                lookback_env_ts = (
                    all_lookback_env_ts[-lookback_obs_count:]
                    if lookback_obs_count > 0
                    else []
                )
                # Current chunk's non-skip env_t
                current_chunk_env_ts = [
                    ep.env_t_started + idx
                    for idx, agent_t in enumerate(env_t_to_agent_t)
                    if agent_t != MultiAgentEpisode.SKIP_ENV_TS_TAG
                ]
                non_skip_env_t = lookback_env_ts + current_chunk_env_ts
                info_timesteps = [info["env_timestep"] for info in infos]
                assert (
                    non_skip_env_t == info_timesteps
                ), f"AssertationError for env_t to info timesteps: {non_skip_env_t} != {info_timesteps}"

    # CONCATENATE CHUNKS THEN TEST THAT THE CONCATENATED DATA IS CORRECT
    print("\nCONCATENATE EPISODES\n")
    unique_episode_ids = {eps.id_ for eps in episodes}
    for ep_id in unique_episode_ids:
        print(f"{ep_id=}")
        eps_chunks = [ep for ep in episodes if ep.id_ == ep_id]
        combined = eps_chunks[0]
        for chunk in eps_chunks[1:]:
            combined.concat_episode(chunk)

        # print(f'  Combined episode: env_t_started={combined.env_t_started}, '
        #       f'env_t={combined.env_t}, is_done={combined.is_done}')

        # Check the episode contents for each agent
        for agent_id, sa_episode in combined.agent_episodes.items():
            obs = sa_episode.get_observations()
            actions = sa_episode.get_actions()
            rewards = sa_episode.get_rewards()
            infos = sa_episode.get_infos()
            env_t_to_agent_t = combined.env_t_to_agent_t[agent_id].get()

            print(
                f"  Agent {agent_id}: len={len(sa_episode)}, "
                f"obs={list(obs)}, env_t_to_agent_t={list(env_t_to_agent_t)}"
            )

            # Observations should be sequential: 0, 1, 2, 3, ...
            expected_obs = list(range(len(obs)))
            assert (
                list(obs) == expected_obs
            ), f"Agent {agent_id}: expected obs {expected_obs}, got {list(obs)}"

            # Actions should equal observations (EchoRLModule)
            assert list(actions) == list(
                obs[:-1]
            ), f"Agent {agent_id}: expected actions {list(obs[:-1])}, got {list(actions)}"

            # Rewards should equal 1 for every timestep
            assert list(rewards) == [1] * len(actions)

            # You should have the same number of info as obs
            assert len(list(infos)) == len(list(obs))

            expected_env_t_to_agent_t = []
            agent_t = 0
            for env_t in range(combined.env_t + 1):
                if AGENT_FNS[agent_id](env_t):
                    expected_env_t_to_agent_t.append(agent_t)
                    agent_t += 1
                else:
                    expected_env_t_to_agent_t.append(MultiAgentEpisode.SKIP_ENV_TS_TAG)

            assert (
                list(env_t_to_agent_t) == expected_env_t_to_agent_t
            ), f"\tAssertationError for env_t_to_agent_t: {list(env_t_to_agent_t)} != {expected_env_t_to_agent_t}"

            # The info timesteps should equal to the non-skip timesteps
            non_skip_agent_t = [
                env_t
                for env_t, agent_t in enumerate(env_t_to_agent_t)
                if agent_t != MultiAgentEpisode.SKIP_ENV_TS_TAG
            ]
            info_timesteps = [info["env_timestep"] for info in infos]
            assert (
                non_skip_agent_t == info_timesteps
            ), f"\tAssertationError non-skip-agent-t: {non_skip_agent_t=}, {info_timesteps=}"
