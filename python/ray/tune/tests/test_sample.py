import numpy as np
import unittest

from ray import tune


class SearchSpaceTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testBoundedFloat(self):
        bounded = tune.sample.Float(-4.2, 8.3)

        # Don't allow to specify more than one sampler
        with self.assertRaises(ValueError):
            bounded.normal().uniform()

        # Normal
        samples = bounded.normal(-4, 2).sample(size=1000)
        self.assertTrue(any(-4.2 < s < 8.3 for s in samples))
        self.assertTrue(np.mean(samples) < -2)

        # Uniform
        samples = bounded.uniform().sample(size=1000)
        self.assertTrue(any(-4.2 < s < 8.3 for s in samples))
        self.assertFalse(np.mean(samples) < -2)

        # Loguniform
        with self.assertRaises(ValueError):
            bounded.loguniform().sample(size=1000)

        bounded_positive = tune.sample.Float(1e-4, 1e-1)
        samples = bounded_positive.loguniform().sample(size=1000)
        self.assertTrue(any(1e-4 < s < 1e-1 for s in samples))

    def testUnboundedFloat(self):
        unbounded = tune.sample.Float()

        # Require min and max bounds for loguniform
        with self.assertRaises(ValueError):
            unbounded.loguniform()

    def testBoundedInt(self):
        bounded = tune.sample.Integer(-3, 12)

        samples = bounded.uniform().sample(size=1000)
        self.assertTrue(any(-3 <= s < 12 for s in samples))
        self.assertFalse(np.mean(samples) < 2)

    def testCategorical(self):
        categories = [-2, -1, 0, 1, 2]
        cat = tune.sample.Categorical(categories)

        samples = cat.uniform().sample(size=1000)
        self.assertTrue(any([-2 <= s <= 2 for s in samples]))
        self.assertTrue(all([c in samples for c in categories]))

    def testIterative(self):
        categories = [-2, -1, 0, 1, 2]

        def test_iter():
            for i in categories:
                yield i

        itr = tune.sample.Iterative(test_iter())
        samples = itr.uniform().sample(size=5)
        self.assertTrue(any([-2 <= s <= 2 for s in samples]))
        self.assertTrue(all([c in samples for c in categories]))

        itr = tune.sample.Iterative(iter(categories))
        samples = itr.uniform().sample(size=5)
        self.assertTrue(any([-2 <= s <= 2 for s in samples]))
        self.assertTrue(all([c in samples for c in categories]))

    def testFunction(self):
        def sample(spec):
            return np.random.uniform(-4, 4)

        fnc = tune.sample.Function(sample)

        samples = fnc.uniform().sample(size=1000)
        self.assertTrue(any([-4 < s < 4 for s in samples]))
        self.assertTrue(-2 < np.mean(samples) < 2)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
