class Space(object):
    """Defines the observation and action spaces, so you can write generic
    code that applies to any Env. For example, you can choose a random
    action.
    """
    def __init__(self, shape=None, dtype=None):
        import numpy as np  # takes about 300-400ms to import, so we load lazily
        self.shape = None if shape is None else tuple(shape)
        self.dtype = None if dtype is None else np.dtype(dtype)
        self.np_random = np.random.RandomState()

    def sample(self):
        """Uniformly randomly sample a random element of this space. """
        raise NotImplementedError

    def seed(self, seed):
        """Seed the PRNG of this space. """
        self.np_random.seed(seed)

    def contains(self, x):
        """
        Return boolean specifying if x is a valid
        member of this space
        """
        raise NotImplementedError

    def __contains__(self, x):
        return self.contains(x)

    def to_jsonable(self, sample_n):
        """Convert a batch of samples from this space to a JSONable data type."""
        # By default, assume identity is JSONable
        return sample_n

    def from_jsonable(self, sample_n):
        """Convert a JSONable data type to a batch of samples from this space."""
        # By default, assume identity is JSONable
        return sample_n
