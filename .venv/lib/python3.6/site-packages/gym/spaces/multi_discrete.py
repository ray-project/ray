import numpy as np
from .space import Space


class MultiDiscrete(Space):
    """
    - The multi-discrete action space consists of a series of discrete action spaces with different number of actions in eachs
    - It is useful to represent game controllers or keyboards where each key can be represented as a discrete action space
    - It is parametrized by passing an array of positive integers specifying number of actions for each discrete action space

    Note: A value of 0 always need to represent the NOOP action.

    e.g. Nintendo Game Controller
    - Can be conceptualized as 3 discrete action spaces:

        1) Arrow Keys: Discrete 5  - NOOP[0], UP[1], RIGHT[2], DOWN[3], LEFT[4]  - params: min: 0, max: 4
        2) Button A:   Discrete 2  - NOOP[0], Pressed[1] - params: min: 0, max: 1
        3) Button B:   Discrete 2  - NOOP[0], Pressed[1] - params: min: 0, max: 1

    - Can be initialized as

        MultiDiscrete([ 5, 2, 2 ])

    """
    def __init__(self, nvec):
    
        """
        nvec: vector of counts of each categorical variable
        """
        assert (np.array(nvec) > 0).all(), 'nvec (counts) have to be positive'
        self.nvec = np.asarray(nvec, dtype=np.int64)

        super(MultiDiscrete, self).__init__(self.nvec.shape, np.int64)

    def sample(self):
        return (self.np_random.random_sample(self.nvec.shape)*self.nvec).astype(self.dtype)

    def contains(self, x):
        # if nvec is uint32 and space dtype is uint32, then 0 <= x < self.nvec guarantees that x
        # is within correct bounds for space dtype (even though x does not have to be unsigned)
        return (0 <= x).all() and (x < self.nvec).all()

    def to_jsonable(self, sample_n):
        return [sample.tolist() for sample in sample_n]

    def from_jsonable(self, sample_n):
        return np.array(sample_n)

    def __repr__(self):
        return "MultiDiscrete({})".format(self.nvec)

    def __eq__(self, other):
        return isinstance(other, MultiDiscrete) and np.all(self.nvec == other.nvec)
