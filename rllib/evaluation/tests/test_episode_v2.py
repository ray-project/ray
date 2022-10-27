import unittest

import ray
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.examples.env.mock_env import MockEnv3
from ray.rllib.policy import Policy
from ray.rllib.utils import override

NUM_STEPS = 25
NUM_AGENTS = 4


class EchoPolicy(Policy):
    @override(Policy)
    def compute_actions(
        self,
        obs_batch,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
        episodes=None,
        explore=None,
        timestep=None,
        **kwargs
    ):
        return obs_batch.argmax(axis=1), [], {}


class EpisodeEnv(MultiAgentEnv):
    def __init__(self, episode_length, num):
        super().__init__()
        self._skip_env_checking = True
        self.agents = [MockEnv3(episode_length) for _ in range(num)]
        self.dones = set()
        self.observation_space = self.agents[0].observation_space
        self.action_space = self.agents[0].action_space

    def reset(self):
        self.dones = set()
        return {i: a.reset() for i, a in enumerate(self.agents)}

    def step(self, action_dict):
        obs, rew, done, info = {}, {}, {}, {}
        print("ACTIONDICT IN ENV\n", action_dict)
        for i, action in action_dict.items():
            obs[i], rew[i], done[i], info[i] = self.agents[i].step(action)
            obs[i] = obs[i] + i
            rew[i] = rew[i] + i
            info[i]["timestep"] = info[i]["timestep"] + i
            if done[i]:
                self.dones.add(i)
        done["__all__"] = len(self.dones) == len(self.agents)
        return obs, rew, done, info


class TestEpisodeV2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=1)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_singleagent_env(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv3(NUM_STEPS),
            policy_spec=EchoPolicy,
        )
        sample_batch = ev.sample()
        self.assertEqual(sample_batch.count, 100)
        # A batch of 100. 4 episodes, each 25.
        self.assertEqual(len(set(sample_batch["eps_id"])), 4)

    def test_multiagent_env(self):
        temp_env = EpisodeEnv(NUM_STEPS, NUM_AGENTS)
        ev = RolloutWorker(
            env_creator=lambda _: temp_env,
            policy_spec={
                str(agent_id): (
                    EchoPolicy,
                    temp_env.observation_space,
                    temp_env.action_space,
                    {},
                )
                for agent_id in range(NUM_AGENTS)
            },
            policy_mapping_fn=lambda aid, eps, **kwargs: str(aid),
        )
        sample_batches = ev.sample()
        self.assertEqual(len(sample_batches.policy_batches), 4)
        for agent_id, sample_batch in sample_batches.policy_batches.items():
            self.assertEqual(sample_batch.count, 100)
            # A batch of 100. 4 episodes, each 25.
            self.assertEqual(len(set(sample_batch["eps_id"])), 4)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
