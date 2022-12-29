import gymnasium as gym
from gymnasium.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import unittest

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.preprocessors import (
    DictFlatteningPreprocessor,
    get_preprocessor,
    NoPreprocessor,
    TupleFlatteningPreprocessor,
    OneHotPreprocessor,
    AtariRamPreprocessor,
    GenericPixelPreprocessor,
)
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestPreprocessors(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_preprocessing_disabled(self):
        config = (
            ppo.PPOConfig()
            .environment(
                "ray.rllib.examples.env.random_env.RandomEnv",
                env_config={
                    "config": {
                        "observation_space": Dict(
                            {
                                "a": Discrete(5),
                                "b": Dict(
                                    {
                                        "ba": Discrete(4),
                                        "bb": Box(-1.0, 1.0, (2, 3), dtype=np.float32),
                                    }
                                ),
                                "c": Tuple((MultiDiscrete([2, 3]), Discrete(1))),
                                "d": Box(-1.0, 1.0, (1,), dtype=np.int32),
                            }
                        ),
                    },
                },
            )
            # Speed things up a little.
            .rollouts(rollout_fragment_length=5)
            .training(train_batch_size=100, sgd_minibatch_size=10, num_sgd_iter=1)
            .debugging(seed=42)
            # Set this to True to enforce no preprocessors being used.
            # Complex observations now arrive directly in the model as
            # structures of batches, e.g. {"a": tensor, "b": [tensor, tensor]}
            # for obs-space=Dict(a=..., b=Tuple(..., ...)).
            .experimental(_disable_preprocessor_api=True)
        )

        num_iterations = 1
        # Only supported for tf so far.
        for _ in framework_iterator(config):
            algo = config.build()
            for i in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                print(results)
            check_compute_single_action(algo)
            algo.stop()

    def test_gym_preprocessors(self):
        p1 = ModelCatalog.get_preprocessor(gym.make("CartPole-v1"))
        self.assertEqual(type(p1), NoPreprocessor)

        p2 = ModelCatalog.get_preprocessor(gym.make("FrozenLake-v1"))
        self.assertEqual(type(p2), OneHotPreprocessor)

        p3 = ModelCatalog.get_preprocessor(
            gym.make("GymV26Environment-v0", env_id="ALE/MsPacman-ram-v5")
        )
        self.assertEqual(type(p3), AtariRamPreprocessor)

        p4 = ModelCatalog.get_preprocessor(
            gym.make(
                "GymV26Environment-v0",
                env_id="ALE/MsPacman-v5",
                make_kwargs={"frameskip": 1},
            )
        )
        self.assertEqual(type(p4), GenericPixelPreprocessor)

    def test_tuple_preprocessor(self):
        class TupleEnv:
            def __init__(self):
                self.observation_space = Tuple(
                    [Discrete(5), Box(0, 5, shape=(3,), dtype=np.float32)]
                )

        pp = ModelCatalog.get_preprocessor(TupleEnv())
        self.assertTrue(isinstance(pp, TupleFlatteningPreprocessor))
        self.assertEqual(pp.shape, (8,))
        self.assertEqual(
            list(pp.transform((0, np.array([1, 2, 3], np.float32)))),
            [float(x) for x in [1, 0, 0, 0, 0, 1, 2, 3]],
        )

    def test_dict_flattening_preprocessor(self):
        space = Dict(
            {
                "a": Discrete(2),
                "b": Tuple([Discrete(3), Box(-1.0, 1.0, (4,))]),
            }
        )
        pp = get_preprocessor(space)(space)
        self.assertTrue(isinstance(pp, DictFlatteningPreprocessor))
        self.assertEqual(pp.shape, (9,))
        check(
            pp.transform(
                {"a": 1, "b": (1, np.array([0.0, -0.5, 0.1, 0.6], np.float32))}
            ),
            [0.0, 1.0, 0.0, 1.0, 0.0, 0.0, -0.5, 0.1, 0.6],
        )

    def test_one_hot_preprocessor(self):
        space = Discrete(5)
        pp = get_preprocessor(space)(space)
        self.assertTrue(isinstance(pp, OneHotPreprocessor))
        self.assertTrue(pp.shape == (5,))
        check(pp.transform(3), [0.0, 0.0, 0.0, 1.0, 0.0])
        check(pp.transform(0), [1.0, 0.0, 0.0, 0.0, 0.0])

        space = MultiDiscrete([2, 3, 4])
        pp = get_preprocessor(space)(space)
        self.assertTrue(isinstance(pp, OneHotPreprocessor))
        self.assertTrue(pp.shape == (9,))
        check(
            pp.transform(np.array([1, 2, 0])),
            [0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0],
        )
        check(
            pp.transform(np.array([0, 1, 3])),
            [1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0],
        )

    def test_nested_multidiscrete_one_hot_preprocessor(self):
        space = Tuple((MultiDiscrete([2, 3, 4]),))
        pp = get_preprocessor(space)(space)
        self.assertTrue(pp.shape == (9,))
        check(
            pp.transform((np.array([1, 2, 0]),)),
            [0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0],
        )
        check(
            pp.transform((np.array([0, 1, 3]),)),
            [1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0],
        )

    def test_multidimensional_multidiscrete_one_hot_preprocessor(self):
        space2d = MultiDiscrete([[2, 2], [3, 3]])
        space3d = MultiDiscrete([[[2, 2], [3, 4]], [[5, 6], [7, 8]]])
        pp2d = get_preprocessor(space2d)(space2d)
        pp3d = get_preprocessor(space3d)(space3d)
        self.assertTrue(isinstance(pp2d, OneHotPreprocessor))
        self.assertTrue(isinstance(pp3d, OneHotPreprocessor))
        self.assertTrue(pp2d.shape == (10,))
        self.assertTrue(pp3d.shape == (37,))
        check(
            pp2d.transform(np.array([[1, 0], [2, 1]])),
            [0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0],
        )
        check(
            pp3d.transform(np.array([[[0, 1], [2, 3]], [[4, 5], [6, 7]]])),
            [
                1.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                1.0,
            ],
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
