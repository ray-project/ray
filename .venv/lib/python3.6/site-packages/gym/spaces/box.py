import numpy as np

from .space import Space


class Box(Space):
    """A box in R^n, i.e.each coordinate is bounded.
    
    There are two common use cases:
    
    * Identical bound for each dimension::
        >>> Box(low=-1.0, high=2.0, shape=(3, 4), dtype=np.float32)
        Box(3, 4)
        
    * Independent bound for each dimension::
        >>> Box(low=np.array([-1.0, -2.0]), high=np.array([2.0, 4.0]), dtype=np.float32)
        Box(2,)

    """
    def __init__(self, low, high, shape=None, dtype=np.float32):
        assert dtype is not None, 'dtype must be explicitly provided. '
        self.dtype = np.dtype(dtype)

        if shape is None:
            assert low.shape == high.shape
            self.shape = low.shape
            self.low = low
            self.high = high
        else:
            assert np.isscalar(low) and np.isscalar(high)
            self.shape = tuple(shape)
            self.low = np.full(self.shape, low)
            self.high = np.full(self.shape, high)
            low = low + np.zeros(shape)
            high = high + np.zeros(shape)
        self.low = self.low.astype(self.dtype)
        self.high = self.high.astype(self.dtype)
        super(Box, self).__init__(self.shape, self.dtype)

    def sample(self):
        high = self.high if self.dtype.kind == 'f' else self.high.astype('int64') + 1
        return self.np_random.uniform(low=self.low, high=high, size=self.shape).astype(self.dtype)

    def contains(self, x):
        return x.shape == self.shape and np.all(x >= self.low) and np.all(x <= self.high)

    def to_jsonable(self, sample_n):
        return np.array(sample_n).tolist()

    def from_jsonable(self, sample_n):
        return [np.asarray(sample) for sample in sample_n]

    def __repr__(self):
        return "Box" + str(self.shape)

    def __eq__(self, other):
        return isinstance(other, Box) and np.allclose(self.low, other.low) and np.allclose(self.high, other.high)
