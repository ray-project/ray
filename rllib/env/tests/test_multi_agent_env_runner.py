import unittest

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    EPISODE_AGENT_RETURN_MEAN,
    EPISODE_MODULE_RETURN_MEAN,
)
from ray.rllib.utils.test_utils import check


class AgentsComeAndGoEnv(MultiAgentEnv):
    """A multi-agent env with a changing set of agents.

    Every step the lowest-id agent terminates (leaves) and a brand-new agent id
    joins, so the set of active agents keeps shifting. Used to reproduce the
    connector `KeyError` from issue #61602, where a module still emits an action
    for an agent whose `SingleAgentEpisode` has already been removed.
    """

    MAX_AGENTS = 300

    def __init__(self, config=None):
        super().__init__()
        self._obs_space = gym.spaces.Box(-1.0, 1.0, (3,), np.float32)
        self._act_space = gym.spaces.Discrete(2)
        self.possible_agents = [str(i) for i in range(self.MAX_AGENTS)]
        self.observation_spaces = dict.fromkeys(self.possible_agents, self._obs_space)
        self.action_spaces = dict.fromkeys(self.possible_agents, self._act_space)

    def reset(self, *, seed=None, options=None):
        self._next_id = 3
        self._t = 0
        self._active = ["0", "1", "2"]
        self.agents = list(self._active)
        obs = {a: self._obs_space.sample() for a in self._active}
        return obs, {a: {} for a in self._active}

    def step(self, action_dict):
        self._t += 1
        leaving, survivors = self._active[0], self._active[1:]
        newcomer = str(self._next_id)
        self._next_id += 1
        self._active = survivors + [newcomer]

        obs = {a: self._obs_space.sample() for a in self._active}
        rewards = dict.fromkeys(self._active, 1.0)
        rewards[leaving] = 0.0
        terminateds = dict.fromkeys(self._active, False)
        terminateds[leaving] = True
        truncateds = dict.fromkeys(self._active, False)
        truncateds[leaving] = False

        all_done = self._t >= 25 or self._next_id >= self.MAX_AGENTS - 4
        terminateds["__all__"] = all_done
        truncateds["__all__"] = False
        # `agents` must cover every id referenced this step (incl. the leaver).
        self.agents = [
            a for a in set(obs) | set(rewards) | set(terminateds) if a != "__all__"
        ]
        return obs, rewards, terminateds, truncateds, {a: {} for a in obs}


class TestMultiAgentEnvRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(self) -> None:
        ray.shutdown()

    def test_sample_with_changing_agents(self):
        """Sampling an env with a changing agent set must not raise KeyError.

        Regression test for #61602: with `batch_mode="truncate_episodes"` and a
        set `rollout_fragment_length`, the module-to-env connector used to raise
        `KeyError` when an action was produced for an agent whose
        `SingleAgentEpisode` had already been removed.
        """
        config = (
            PPOConfig()
            .environment(AgentsComeAndGoEnv)
            .multi_agent(
                policies={"p"},
                policy_mapping_fn=lambda aid, *args, **kwargs: "p",
            )
            .env_runners(
                batch_mode="truncate_episodes",
                rollout_fragment_length=32,
            )
        )
        env_runner = MultiAgentEnvRunner(config=config)
        # Several sampling rounds so the changing-agent connector path is hit.
        for _ in range(6):
            env_runner.sample(num_timesteps=64)

    def test_sample_timesteps(self):
        # Build a multi agent config.
        config = self._build_config()
        # Create a `MultiAgentEnvRunner` instance.
        env_runner = MultiAgentEnvRunner(config=config)

        # Now sample 10 timesteps.
        episodes = env_runner.sample(num_timesteps=10)
        # Assert that we have 10 timesteps sampled.
        check(sum(len(episode) for episode in episodes), 10)

        # Now sample 200 timesteps.
        episodes = env_runner.sample(num_timesteps=200)
        # Ensure that two episodes are returned.
        # Note, after 200 timesteps the test environment truncates.
        self.assertGreaterEqual(len(episodes), 2)
        # Also ensure that the first episode was truncated.
        check(episodes[0].is_terminated, True)
        # Assert that indeed 200 timesteps were sampled.
        check(sum(len(e) for e in episodes), 200)
        # Assert that the timesteps however in the episodes are 210.
        # Note, the first episode started at `t_started=10`.
        check(sum(e.env_t for e in episodes), 210)
        # Assert that all agents extra model outputs are recorded.
        for agent_eps in episodes[0].agent_episodes.values():
            check("action_logp" in agent_eps.extra_model_outputs, True)
            check(
                len(agent_eps.actions),
                len(agent_eps.extra_model_outputs["action_logp"]),
            )
            check(
                len(agent_eps.actions),
                len(agent_eps.extra_model_outputs["action_dist_inputs"]),
            )

    def test_sample_episodes(self):
        # Build a multi agent config.
        config = self._build_config()
        # Create a `MultiAgentEnvRunner` instance.
        env_runner = MultiAgentEnvRunner(config=config)

        # Now sample 5 episodes.
        episodes = env_runner.sample(num_episodes=5)
        # Assert that we have 5 episodes sampled.
        check(len(episodes), 5)
        # Also assert that the episodes are indeed truncated.
        check(all(eps.is_terminated for eps in episodes), True)
        # Assert that all agents have the extra model outputs.
        for eps in episodes:
            for agent_eps in eps.agent_episodes.values():
                check("action_logp" in agent_eps.extra_model_outputs, True)
                check(
                    len(agent_eps.actions),
                    len(agent_eps.extra_model_outputs["action_logp"]),
                )
                check(
                    len(agent_eps.actions),
                    len(agent_eps.extra_model_outputs["action_dist_inputs"]),
                )

        # Now sample 10 timesteps and then 1 episode.
        episodes = env_runner.sample(num_timesteps=10)
        episodes += env_runner.sample(num_episodes=1)
        # Ensure that the episodes both start at zero.
        for eps in episodes:
            check(eps.env_t_started, 0)

        # Now sample 1 episode and then 10 timesteps.
        episodes = env_runner.sample(num_episodes=1)
        episodes += env_runner.sample(num_timesteps=10)
        # Assert that in both cases we start at zero.
        for eps in episodes:
            check(eps.env_t_started, 0)

    def test_counting_by_agent_steps(self):
        """Tests whether counting by agent_steps works."""
        # Build a multi agent config.
        config = self._build_config(num_agents=4, num_policies=1)
        config.multi_agent(count_steps_by="agent_steps")
        config.env_runners(
            rollout_fragment_length=20,
            num_envs_per_env_runner=4,
        )

        # Create a `MultiAgentEnvRunner` instance.
        env_runner = MultiAgentEnvRunner(config=config)
        episodes = env_runner.sample()
        assert len(episodes) == 4
        assert all(e.agent_steps() == 20 for e in episodes)

    def _build_config(self, num_agents=2, num_policies=2):
        # Build the configuration and use `PPO`.
        assert num_policies == 1 or num_agents == num_policies

        config = (
            PPOConfig()
            .environment(
                MultiAgentCartPole,
                env_config={"num_agents": num_agents},
            )
            .multi_agent(
                policies={f"p{i}" for i in range(num_policies)},
                policy_mapping_fn=(
                    lambda aid, *args, **kwargs: (
                        f"p{aid}" if num_agents == num_policies else "p0"
                    )
                ),
            )
        )

        return config

    def test_module_metrics_returns_equal_sum_of_agent_returns(self):
        """Check if module metrics returns equals sum of returns of agents assigned to that module.

        Related to https://github.com/ray-project/ray/issues/59860
        """
        # Build a multi agent config.
        config = self._build_config(num_agents=4, num_policies=1)
        # Create a `MultiAgentEnvRunner` instance.
        env_runner = MultiAgentEnvRunner(config=config)
        # Now run one episode
        env_runner.sample(num_episodes=1)
        # Collect metrics from that episode
        metrics = env_runner.get_metrics()
        # Expected singular policy name when setting num_agents != num_policies and num_policies = 1
        assert "p0" in metrics[EPISODE_MODULE_RETURN_MEAN].keys()
        # Collect episode return, module return, and sum of agent returns
        episode_return_mean = metrics["episode_return_mean"].reduce()
        module_episode_returns_mean = metrics[EPISODE_MODULE_RETURN_MEAN]["p0"].reduce()
        sum_agent_episode_returns_mean = sum(
            value.reduce() for value in metrics[EPISODE_AGENT_RETURN_MEAN].values()
        )
        # Expect episode_return_mean == module_return_mean == sum_agent_returns_mean
        assert (
            episode_return_mean
            == module_episode_returns_mean
            == sum_agent_episode_returns_mean
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
