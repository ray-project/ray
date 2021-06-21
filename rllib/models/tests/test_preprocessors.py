import gym
from gym.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import unittest

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.preprocessors import DictFlatteningPreprocessor, \
    get_preprocessor, NoPreprocessor, TupleFlatteningPreprocessor, \
    OneHotPreprocessor, AtariRamPreprocessor, GenericPixelPreprocessor
from ray.rllib.utils.test_utils import check


class TestPreprocessors(unittest.TestCase):
    def test_gym_preprocessors(self):
        p1 = ModelCatalog.get_preprocessor(gym.make("CartPole-v0"))
        self.assertEqual(type(p1), NoPreprocessor)

        p2 = ModelCatalog.get_preprocessor(gym.make("FrozenLake-v0"))
        self.assertEqual(type(p2), OneHotPreprocessor)

        p3 = ModelCatalog.get_preprocessor(gym.make("MsPacman-ram-v0"))
        self.assertEqual(type(p3), AtariRamPreprocessor)

        p4 = ModelCatalog.get_preprocessor(gym.make("MsPacmanNoFrameskip-v4"))
        self.assertEqual(type(p4), GenericPixelPreprocessor)

    def test_tuple_preprocessor(self):
        class TupleEnv:
            def __init__(self):
                self.observation_space = Tuple(
                    [Discrete(5),
                     Box(0, 5, shape=(3, ), dtype=np.float32)])

        pp = ModelCatalog.get_preprocessor(TupleEnv())
        self.assertTrue(isinstance(pp, TupleFlatteningPreprocessor))
        self.assertEqual(pp.shape, (8, ))
        self.assertEqual(
            list(pp.transform((0, np.array([1, 2, 3])))),
            [float(x) for x in [1, 0, 0, 0, 0, 1, 2, 3]])

    def test_dict_flattening_preprocessor(self):
        space = Dict({
            "a": Discrete(2),
            "b": Tuple([Discrete(3), Box(-1.0, 1.0, (4, ))]),
        })
        pp = get_preprocessor(space)(space)
        self.assertTrue(isinstance(pp, DictFlatteningPreprocessor))
        self.assertEqual(pp.shape, (9, ))
        check(
            pp.transform({
                "a": 1,
                "b": (1, np.array([0.0, -0.5, 0.1, 0.6]))
            }), [0.0, 1.0, 0.0, 1.0, 0.0, 0.0, -0.5, 0.1, 0.6])

    def test_one_hot_preprocessor(self):
        space = Discrete(5)
        pp = get_preprocessor(space)(space)
        self.assertTrue(isinstance(pp, OneHotPreprocessor))
        self.assertTrue(pp.shape == (5, ))
        check(pp.transform(3), [0.0, 0.0, 0.0, 1.0, 0.0])
        check(pp.transform(0), [1.0, 0.0, 0.0, 0.0, 0.0])

        space = MultiDiscrete([2, 3, 4])
        pp = get_preprocessor(space)(space)
        self.assertTrue(isinstance(pp, OneHotPreprocessor))
        self.assertTrue(pp.shape == (9, ))
        check(
            pp.transform(np.array([1, 2, 0])),
            [0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0])
        check(
            pp.transform(np.array([0, 1, 3])),
            [1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0])

    def test_nested_multidiscrete_one_hot_preprocessor(self):
        space = Tuple((MultiDiscrete([2, 3, 4]), ))
        pp = get_preprocessor(space)(space)
        self.assertTrue(pp.shape == (9, ))
        check(
            pp.transform((np.array([1, 2, 0]), )),
            [0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0])
        check(
            pp.transform((np.array([0, 1, 3]), )),
            [1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
