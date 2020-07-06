import unittest
from copy import deepcopy

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env
from ray.rllib.env import PettingZooEnv
from ray.rllib.agents.registry import get_agent_class
from ray.rllib.utils.test_utils import framework_iterator

from pettingzoo.mpe import simple_spread_v0


class TestPettingZooEnv(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_pettingzoo_env(self):
        register_env('prison', lambda _: PettingZooEnv(simple_spread_v0.env()))

        agent_class = get_agent_class('PPO')

        config = deepcopy(agent_class._default_config)

        test_env = PettingZooEnv(simple_spread_v0.env())
        obs_space = test_env.observation_space
        act_space = test_env.action_space
        test_env.close()

        config['multiagent'] = {
                'policies': {
                    # the first tuple value is None -> uses default policy
                    'av': (None, obs_space, act_space, {}),
                },
                'policy_mapping_fn': lambda agent_id: 'av'
                }

        config['log_level'] = 'DEBUG'
        config['num_workers'] = 0
        config['sample_batch_size'] = 30     # Fragment length, collected at once from each worker and for each agent!
        config['train_batch_size'] = 200     # Training batch size -> Fragments are concatenated up to this point.
        config['horizon'] = 200              # After n steps, force reset simulation
        config['no_done_at_end'] = False
        ray.init(num_cpus=1)
        agent = agent_class(env='prison', config=config)
        agent.train()


if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main(['-v', __file__]))
