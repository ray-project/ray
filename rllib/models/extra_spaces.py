import numpy as np
import gym


class Simplex(gym.Space):
    """Represents a d - 1 dimensional Simplex in R^d.

    That is, all coordinates are in [0, 1] and sum to 1.
    The dimension d of the simplex is assumed to be shape[-1].

    Additionally one can specify the underlying distribution of
    the simplex as a Dirichlet distribution by providing concentration
    parameters. By default, sampling is uniform, i.e. concentration is
    all 1s.

    Example usage:
    self.action_space = spaces.Simplex(shape=(3, 4))
        --> 3 independent 4d Dirichlet with uniform concentration
    """

    def __init__(self, shape, concentration=None, dtype=np.float32):
        assert type(shape) in [tuple, list]
        self.shape = shape
        self.dtype = dtype
        self.dim = shape[-1]

        if concentration is not None:
            assert concentration.shape == shape[:-1]
        else:
            self.concentration = [1] * self.dim

        super().__init__(shape, dtype)
        self.np_random = np.random.RandomState()

    def seed(self, seed):
        self.np_random.seed(seed)

    def sample(self):
        return np.random.dirichlet(
            self.concentration, size=self.shape[:-1]).astype(self.dtype)

    def contains(self, x):
        return x.shape == self.shape and np.allclose(
            np.sum(x, axis=-1), np.ones_like(x[..., 0]))

    def to_jsonable(self, sample_n):
        return np.array(sample_n).tolist()

    def from_jsonable(self, sample_n):
        return [np.asarray(sample) for sample in sample_n]

    def __repr__(self):
        return "Simplex({}; {})".format(self.shape, self.concentration)

    def __eq__(self, other):
        return np.allclose(self.concentration,
                           other.concentration) and self.shape == other.shape
