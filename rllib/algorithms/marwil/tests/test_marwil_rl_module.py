import itertools
import unittest
from pathlib import Path

import ray


class TestMARWIL(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(self) -> None:
        ray.shutdown()

    def test_rollouts(self):
        frameworks = ["torch"]
        envs = ["CartPole-v1"]
        fwd_fns = ["forward_exploration", "forward_inference"]
        config_combinations = [frameworks, envs, fwd_fns]
        rllib_dir = Path(__file__).parents[3]
        print(f"rllib_dir={rllib_dir.as_posix()}")
        data_file = rllib_dir.joinpath("tests/data/cartpole/large.json")
        print(f"data_file={data_file.as_posix()}")

        for config in itertools.product(*config_combinations):
            fw, env, fwd_fn = config

            print(f"[Fw={fw}] | [Env={env}] | [FWD={fwd_fn}]")


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
