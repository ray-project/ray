import math
import sys

import gymnasium as gym
import numpy as np
import pytest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.env_runner_group import EnvRunnerGroup
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.utils.metrics import NUM_AGENT_STEPS_SAMPLED


class _ChangingAgentsEnv(MultiAgentEnv):
    """First episode has two agents; later episodes only have agent_0."""

    def __init__(self, config=None):
        super().__init__()
        self.possible_agents = ["agent_0", "agent_1"]
        self.observation_spaces = {
            aid: gym.spaces.Box(0.0, 1.0, (1,), np.float32)
            for aid in self.possible_agents
        }
        self.action_spaces = {
            aid: gym.spaces.Discrete(2) for aid in self.possible_agents
        }
        self.agents = []
        self._episode_index = -1
        self._t = 0

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed, options=options)
        self._episode_index += 1
        self._t = 0
        self.agents = (
            list(self.possible_agents)
            if self._episode_index == 0
            else ["agent_0"]
        )
        return {
            aid: np.array([0.0], dtype=np.float32) for aid in self.agents
        }, {}

    def step(self, action_dict):
        self._t += 1
        terminated = self._t >= 2
        obs = {
            aid: np.array([0.0], dtype=np.float32) for aid in self.agents
        }
        rewards = {aid: 0.0 for aid in self.agents}
        terminateds = {aid: terminated for aid in self.agents}
        terminateds["__all__"] = terminated
        truncateds = {aid: False for aid in self.agents}
        truncateds["__all__"] = False
        infos = {aid: {} for aid in self.agents}
        return obs, rewards, terminateds, truncateds, infos


def test_synchronous_parallel_sample_ignores_nan_agent_step_metrics():
    ray.init(num_cpus=1)
    workers = None
    try:
        config = (
            PPOConfig()
            .environment(_ChangingAgentsEnv, disable_env_checking=True)
            .env_runners(
                num_env_runners=0,
                batch_mode="complete_episodes",
                rollout_fragment_length=2,
            )
            .multi_agent(
                policies={"p0"},
                policy_mapping_fn=lambda aid, *args, **kwargs: "p0",
                count_steps_by="agent_steps",
            )
        )
        workers = EnvRunnerGroup(config=config)
        env_runner = workers.local_env_runner

        # Prime the real MetricsLogger with both AgentIDs, then reduce it. The
        # Stats keys remain registered while their current windows are cleared.
        env_runner.sample(num_episodes=1)
        first_metrics = env_runner.get_metrics()
        assert int(first_metrics[NUM_AGENT_STEPS_SAMPLED]["agent_0"]) > 0
        assert int(first_metrics[NUM_AGENT_STEPS_SAMPLED]["agent_1"]) > 0

        # The next real episode contains only agent_0. agent_1's existing
        # SumStats key therefore reduces to NaN. Before the fix, converting that
        # value to int inside synchronous_parallel_sample raises ValueError.
        samples, metrics = synchronous_parallel_sample(
            worker_set=workers,
            max_agent_steps=2,
            concat=False,
            _uses_new_env_runners=True,
            _return_metrics=True,
        )

        assert samples
        assert len(metrics) == 1
        agent_steps = metrics[0][NUM_AGENT_STEPS_SAMPLED]
        assert int(agent_steps["agent_0"]) == 2
        assert math.isnan(float(agent_steps["agent_1"]))
    finally:
        if workers is not None:
            workers.stop()
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
