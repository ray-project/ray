import unittest
import numpy as np
from numpy.testing import assert_allclose

from ray.rllib.agents.ppo.utils import flatten, concatenate
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


# TODO(sven): Move to utils/tests/.
class UtilsTest(unittest.TestCase):
    def testFlatten(self):
        d = {
            "s": np.array([[[1, -1], [2, -2]], [[3, -3], [4, -4]]]),
            "a": np.array([[[5], [-5]], [[6], [-6]]])
        }
        flat = flatten(d.copy(), start=0, stop=2)
        assert_allclose(d["s"][0][0][:], flat["s"][0][:])
        assert_allclose(d["s"][0][1][:], flat["s"][1][:])
        assert_allclose(d["s"][1][0][:], flat["s"][2][:])
        assert_allclose(d["s"][1][1][:], flat["s"][3][:])
        assert_allclose(d["a"][0][0], flat["a"][0])
        assert_allclose(d["a"][0][1], flat["a"][1])
        assert_allclose(d["a"][1][0], flat["a"][2])
        assert_allclose(d["a"][1][1], flat["a"][3])

    def testConcatenate(self):
        d1 = {"s": np.array([0, 1]), "a": np.array([2, 3])}
        d2 = {"s": np.array([4, 5]), "a": np.array([6, 7])}
        d = concatenate([d1, d2])
        assert_allclose(d["s"], np.array([0, 1, 4, 5]))
        assert_allclose(d["a"], np.array([2, 3, 6, 7]))

        D = concatenate([d])
        assert_allclose(D["s"], np.array([0, 1, 4, 5]))
        assert_allclose(D["a"], np.array([2, 3, 6, 7]))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
