from gym.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import os
import unittest

import ray
from ray.rllib.agents.pg import PGTrainer, DEFAULT_CONFIG
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.offline.json_reader import JsonReader
from ray.rllib.utils.test_utils import framework_iterator


SPACES = {
    "dict": Dict({
        "a": Dict({
            "aa": Box(-1.0, 1.0, shape=(3, )),
            "ab": MultiDiscrete([4, 3]),
        }),
        "b": Discrete(3),
        "c": Tuple([Box(0, 100, (), dtype=np.int32), Discrete(2)]),
    }),
    "tuple": Tuple([
        Tuple([
            Box(-1.0, 1.0, shape=(3, )),
            Discrete(3),
        ]),
        MultiDiscrete([4, 3]),
        Dict({
            "a": Box(0, 100, (), dtype=np.int32),
            "b": Discrete(2),
        }),
    ]),
    "multidiscrete": MultiDiscrete([2, 3, 4]),
    # "intbox": Box(0, 100, (2, ), dtype=np.int32),
}


class NestedActionSpacesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=5)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_nested_action_spaces(self):
        config = DEFAULT_CONFIG.copy()
        config["env"] = RandomEnv
        # Write output to check, whether actions are written correctly.
        tmp_dir = os.popen("mktemp -d").read()[:-1]
        if not os.path.exists(tmp_dir):
            # Last resort: Resolve via underlying tempdir (and cut tmp_.
            tmp_dir = ray._private.utils.tempfile.gettempdir() + tmp_dir[4:]
            assert os.path.exists(tmp_dir), f"'{tmp_dir}' not found!"
        config["output"] = tmp_dir + "/out.json"

        for _ in framework_iterator(config):
            for name, action_space in SPACES.items():
                config["env_config"] = {
                    "config": {"action_space": action_space}
                }
                for flatten in [True, False]:
                    print(f"A={action_space} flatten={flatten}")
                    config["_disable_action_flattening"] = not flatten
                    trainer = PGTrainer(config)
                    trainer.train()
                    trainer.stop()

                    # Check actions in output file (whether properly flattened
                    # or not).
                    reader = JsonReader(inputs=config["output"])
                    sample_batch = reader.next()
                    print(sample_batch["actions"])
