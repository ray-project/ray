from gym.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import os
import shutil
import tree  # pip install dm_tree
import unittest

import ray
from ray.rllib.algorithms.bc import BC
from ray.rllib.algorithms.pg import PG, DEFAULT_CONFIG
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.offline.json_reader import JsonReader
from ray.rllib.utils.test_utils import framework_iterator

SPACES = {
    "dict": Dict(
        {
            "a": Dict(
                {
                    "aa": Box(-1.0, 1.0, shape=(3,)),
                    "ab": MultiDiscrete([4, 3]),
                }
            ),
            "b": Discrete(3),
            "c": Tuple([Box(0, 10, (2,), dtype=np.int32), Discrete(2)]),
            "d": Box(0, 3, (), dtype=np.int64),
        }
    ),
    "tuple": Tuple(
        [
            Tuple(
                [
                    Box(-1.0, 1.0, shape=(2,)),
                    Discrete(3),
                ]
            ),
            MultiDiscrete([4, 3]),
            Dict(
                {
                    "a": Box(0, 100, (), dtype=np.int32),
                    "b": Discrete(2),
                }
            ),
        ]
    ),
    "multidiscrete": MultiDiscrete([2, 3, 4]),
    "intbox": Box(0, 100, (2,), dtype=np.int32),
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
        config["output"] = tmp_dir
        # Switch off OPE as we don't write action-probs.
        # TODO: We should probably always write those if `output` is given.
        config["off_policy_estimation_methods"] = {}

        # Pretend actions in offline files are already normalized.
        config["actions_in_input_normalized"] = True

        for _ in framework_iterator(config):
            for name, action_space in SPACES.items():
                config["env_config"] = {
                    "action_space": action_space,
                }
                for flatten in [True, False]:
                    print(f"A={action_space} flatten={flatten}")
                    shutil.rmtree(config["output"])
                    config["_disable_action_flattening"] = not flatten
                    pg = PG(config)
                    pg.train()
                    pg.stop()

                    # Check actions in output file (whether properly flattened
                    # or not).
                    reader = JsonReader(
                        inputs=config["output"],
                        ioctx=pg.workers.local_worker().io_context,
                    )
                    sample_batch = reader.next()
                    if flatten:
                        assert isinstance(sample_batch["actions"], np.ndarray)
                        assert len(sample_batch["actions"].shape) == 2
                        assert sample_batch["actions"].shape[0] == len(sample_batch)
                    else:
                        tree.assert_same_structure(
                            pg.get_policy().action_space_struct,
                            sample_batch["actions"],
                        )

                    # Test, whether offline data can be properly read by
                    # BC, configured accordingly.

                    # doing this for backwards compatibility until we move to parquet
                    # as default output
                    config["input"] = lambda ioctx: JsonReader(
                        ioctx.config["input_config"]["paths"], ioctx
                    )
                    config["input_config"] = {"paths": config["output"]}
                    del config["output"]
                    bc = BC(config=config)
                    bc.train()
                    bc.stop()
                    config["output"] = tmp_dir
                    config["input"] = "sampler"


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
