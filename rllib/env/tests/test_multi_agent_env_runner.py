import unittest

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
    def tearDownClass(self) -> None:
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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
