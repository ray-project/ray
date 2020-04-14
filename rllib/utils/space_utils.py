from gym.spaces import Tuple, Dict
from collections import namedtuple


class TupleActions(namedtuple("TupleActions", ["batches"])):
    """Used to return tuple actions as a list of batches per tuple element."""

    def __new__(cls, batches):
        return super(TupleActions, cls).__new__(cls, batches)

    def numpy(self):
        return TupleActions([b.numpy() for b in self.batches])


def flatten_space(space):

    def _helper_flatten(space_, l_):
        if isinstance(space_, Tuple):
            for s in space_:
                _helper_flatten(s, l_)
        elif isinstance(space, Dict):
            for k in sorted(space_.keys()):
                _helper_flatten(space_[k], l_)
        else:
            l_.append(space_)

    l = []
    _helper_flatten(space, l)
    return l
