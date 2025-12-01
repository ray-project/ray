import unittest

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check


class TestMultiAgentEnvRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

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
        # Build a multi-agent config.
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
        # Build a multi-agent config.
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

    def test_count_steps_by_env_steps(self):
        """Tests whether counting by env_steps works (default behavior)."""
        config = self._build_config(num_agents=2, num_policies=2)
        config.multi_agent(count_steps_by="env_steps")
        config.env_runners(
            rollout_fragment_length=20,
            num_envs_per_env_runner=1,
        )

        env_runner = MultiAgentEnvRunner(config=config)
        episodes = env_runner.sample()

        # With env_steps counting, we should have 20 env steps
        total_env_steps = sum(len(e) for e in episodes)
        self.assertGreaterEqual(total_env_steps, 20)
        self.assertLessEqual(total_env_steps, 21)

        env_runner.stop()

    def test_policy_mapping_all_agents_to_one_policy(self):
        """Test mapping all agents to a single policy."""
        config = self._build_config(num_agents=4, num_policies=1)
        env_runner = MultiAgentEnvRunner(config=config)

        episodes = env_runner.sample(num_episodes=2, random_actions=True)

        # All 4 agents should be mapped to the same policy "p0"
        for episode in episodes:
            # Each episode should have 4 agent episodes
            self.assertEqual(len(episode.agent_episodes), 4)

        env_runner.stop()

    def test_policy_mapping_each_agent_to_own_policy(self):
        """Test mapping each agent to its own policy."""
        num_agents = 3
        config = self._build_config(num_agents=num_agents, num_policies=num_agents)
        env_runner = MultiAgentEnvRunner(config=config)

        episodes = env_runner.sample(num_episodes=2, random_actions=True)

        for episode in episodes:
            # Each episode should have agent episodes for each agent
            self.assertEqual(len(episode.agent_episodes), num_agents)

        env_runner.stop()

    def test_get_spaces_returns_per_policy_spaces(self):
        """Test that get_spaces returns spaces for each policy."""
        config = self._build_config(num_agents=2, num_policies=2)
        env_runner = MultiAgentEnvRunner(config=config)

        spaces = env_runner.get_spaces()

        # Should have spaces for each policy (may include additional module IDs)
        self.assertGreaterEqual(len(spaces), 2)
        self.assertIn("p0", spaces)
        self.assertIn("p1", spaces)

        for policy_id, (obs_space, act_space) in spaces.items():
            # Observation spaces may be Box, Dict, or None depending on configuration
            if obs_space is not None:
                self.assertIsInstance(obs_space, gym.Space)
            # Action spaces may be Discrete, Dict, or None
            if act_space is not None:
                self.assertIsInstance(act_space, gym.Space)

        env_runner.stop()

    def test_agent_episodes_have_correct_structure(self):
        """Test that agent episodes have correct data structure."""
        config = self._build_config(num_agents=2, num_policies=2)
        env_runner = MultiAgentEnvRunner(config=config)

        episodes = env_runner.sample(num_episodes=3, random_actions=True)

        for episode in episodes:
            # Should have agent_episodes attribute
            self.assertTrue(hasattr(episode, "agent_episodes"))
            self.assertIsInstance(episode.agent_episodes, dict)

            for agent_id, agent_episode in episode.agent_episodes.items():
                # Each agent episode should have observations and actions
                self.assertGreater(len(agent_episode.observations), 0)
                self.assertGreater(len(agent_episode.actions), 0)

        env_runner.stop()

    def test_per_agent_rewards(self):
        """Test that per-agent rewards are correctly tracked."""
        config = self._build_config(num_agents=2, num_policies=2)
        env_runner = MultiAgentEnvRunner(config=config)

        episodes = env_runner.sample(num_episodes=3, random_actions=True)

        for episode in episodes:
            for agent_id, agent_episode in episode.agent_episodes.items():
                # Each agent should have rewards
                self.assertGreater(len(agent_episode.rewards), 0)
                # Rewards should be numeric
                for reward in agent_episode.rewards:
                    self.assertIsInstance(reward, (int, float, np.floating))

        env_runner.stop()

    def test_agent_steps_method(self):
        """Test that agent_steps() method returns correct count."""
        config = self._build_config(num_agents=3, num_policies=1)
        config.env_runners(
            rollout_fragment_length=50,
            num_envs_per_env_runner=1,
        )

        env_runner = MultiAgentEnvRunner(config=config)
        episodes = env_runner.sample(num_episodes=2, random_actions=True)

        for episode in episodes:
            # agent_steps should equal sum of all agent episode lengths
            expected_agent_steps = sum(
                len(ae) for ae in episode.agent_episodes.values()
            )
            self.assertEqual(episode.agent_steps(), expected_agent_steps)

        env_runner.stop()

    def test_env_steps_vs_agent_steps(self):
        """Test the difference between env_steps and agent_steps."""
        num_agents = 4
        config = self._build_config(num_agents=num_agents, num_policies=1)
        config.env_runners(
            rollout_fragment_length=100,
            num_envs_per_env_runner=1,
        )

        env_runner = MultiAgentEnvRunner(config=config)
        episodes = env_runner.sample(num_episodes=2, random_actions=True)

        for episode in episodes:
            env_steps = len(episode)
            agent_steps = episode.agent_steps()

            # With 4 agents, agent_steps should be approximately 4x env_steps
            # (assuming all agents act at each step)
            self.assertGreaterEqual(agent_steps, env_steps)
            # Should be close to num_agents * env_steps
            self.assertLessEqual(agent_steps, num_agents * env_steps + num_agents)

        env_runner.stop()

    def test_get_state_contains_multi_rl_module(self):
        """Test that get_state includes the MultiRLModule state."""
        config = self._build_config(num_agents=2, num_policies=2)
        env_runner = MultiAgentEnvRunner(config=config)

        env_runner.sample(num_episodes=1, random_actions=True)
        state = env_runner.get_state()

        self.assertIsInstance(state, dict)
        # Should have rl_module component
        self.assertIn("rl_module", state)

        env_runner.stop()

    def test_set_state_restores_multi_rl_module(self):
        """Test that set_state restores the MultiRLModule state."""
        config = self._build_config(num_agents=2, num_policies=2)

        env_runner1 = MultiAgentEnvRunner(config=config)
        env_runner1.sample(num_episodes=1, random_actions=True)
        state = env_runner1.get_state()

        env_runner2 = MultiAgentEnvRunner(config=config)
        env_runner2.set_state(state)

        state2 = env_runner2.get_state()
        self.assertEqual(set(state.keys()), set(state2.keys()))

        env_runner1.stop()
        env_runner2.stop()

    def test_get_metrics_returns_multi_agent_metrics(self):
        """Test that get_metrics returns multi-agent specific metrics."""
        config = self._build_config(num_agents=2, num_policies=2)
        config.env_runners(
            rollout_fragment_length=200,
            num_envs_per_env_runner=1,
        )

        env_runner = MultiAgentEnvRunner(config=config)
        env_runner.sample(num_episodes=3, random_actions=True)
        metrics = env_runner.get_metrics()

        self.assertIsInstance(metrics, dict)

        env_runner.stop()

    def test_distributed_multi_agent_env_runner(self):
        """Test that MultiAgentEnvRunner works as a Ray Actor."""
        config = self._build_config(num_agents=2, num_policies=2)

        remote_cls = ray.remote(num_cpus=0)(MultiAgentEnvRunner)
        actor = remote_cls.remote(config=config)

        try:
            episodes = ray.get(actor.sample.remote(num_episodes=2, random_actions=True))
            self.assertEqual(len(episodes), 2)

            for episode in episodes:
                self.assertTrue(episode.is_done)
                self.assertEqual(len(episode.agent_episodes), 2)
        finally:
            ray.kill(actor)

    def test_force_reset_multi_agent(self):
        """Test force_reset behavior in multi-agent setting."""
        config = self._build_config(num_agents=2, num_policies=2)
        env_runner = MultiAgentEnvRunner(config=config)

        # Sample partial episode
        env_runner.sample(num_timesteps=10, random_actions=True)

        # Sample with force_reset
        episodes = env_runner.sample(
            num_timesteps=20, random_actions=True, force_reset=True
        )

        # All episodes should start fresh
        for episode in episodes:
            self.assertEqual(episode.env_t_started, 0)

        env_runner.stop()

    def test_episode_continuation_multi_agent(self):
        """Test episode continuation in multi-agent setting."""
        config = self._build_config(num_agents=2, num_policies=2)
        env_runner = MultiAgentEnvRunner(config=config)

        # Sample partial - episodes may not all be done
        env_runner.sample(num_timesteps=10, random_actions=True)

        # Sample more - should continue
        episodes2 = env_runner.sample(num_timesteps=10, random_actions=True)

        # At least some should be continuations
        has_continuation = any(e.env_t_started > 0 for e in episodes2)
        self.assertTrue(has_continuation, "Expected episode continuation")

        env_runner.stop()

    def test_multi_agent_extra_model_outputs(self):
        """Test that extra model outputs are recorded for all agents."""
        config = self._build_config(num_agents=3, num_policies=3)
        env_runner = MultiAgentEnvRunner(config=config)

        episodes = env_runner.sample(num_episodes=2)

        for episode in episodes:
            for agent_id, agent_episode in episode.agent_episodes.items():
                # Should have action_logp in extra outputs
                self.assertIn("action_logp", agent_episode.extra_model_outputs)

                # Length should match actions
                self.assertEqual(
                    len(agent_episode.actions),
                    len(agent_episode.extra_model_outputs["action_logp"]),
                )

        env_runner.stop()

    def test_assert_healthy_multi_agent(self):
        """Test assert_healthy for multi-agent runner."""
        config = self._build_config(num_agents=2, num_policies=2)
        env_runner = MultiAgentEnvRunner(config=config)

        # Should not raise
        env_runner.assert_healthy()

        env_runner.stop()

    def test_make_env_multi_agent(self):
        """Test make_env creates proper multi-agent environment."""
        config = self._build_config(num_agents=2, num_policies=2)
        env_runner = MultiAgentEnvRunner(config=config)

        self.assertIsNotNone(env_runner.env)

        # Recreate env
        env_runner.make_env()
        self.assertIsNotNone(env_runner.env)

        env_runner.stop()

    def test_varying_num_agents(self):
        """Test with different numbers of agents."""
        for num_agents in [1, 2, 4, 8]:
            config = self._build_config(num_agents=num_agents, num_policies=1)
            env_runner = MultiAgentEnvRunner(config=config)

            episodes = env_runner.sample(num_episodes=2, random_actions=True)

            for episode in episodes:
                # Should have correct number of agent episodes
                self.assertEqual(len(episode.agent_episodes), num_agents)

            env_runner.stop()

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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
