from gym.spaces import Discrete
from tensorflow.python.eager.context import eager_mode
import unittest

from ray.rllib.utils.explorations import Exploration
from ray.rllib.utils import check, try_import_tf
from ray.rllib.utils.from_config import from_config

tf = try_import_tf()


class TestExplorations(unittest.TestCase):
    """
    Tests all Exploration classes.
    """

    def test_epsilon_greedy_exploration(self):
        value = 2.3
        ts = [100, 0, 10, 2, 3, 4, 99, 56, 10000, 23, 234, 56]

        # Simple config where 1 ts leads from random-only to non-random-only.
        config = {"action_space": Discrete(4), "initial_epsilon": 1.0,
                  "final_epsilon": 0.0, "epsilon_time_steps": 1}

        model = Model()
        epsilon_greedy = from_config(Exploration, config=config,
                                     framework="tf")

        
        # Test eager as well.
        with eager_mode():
            constant = from_config(ConstantSchedule, config, framework="tf")
            for t in ts:
                out = constant(t)
                check(out, value)
