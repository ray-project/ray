from gym.spaces import Tuple, Dict
from collections import namedtuple


class TupleActions(namedtuple("TupleActions", ["batches"])):
    """Used to return tuple actions as a list of batches per tuple element."""

    def __new__(cls, batches):
        return super(TupleActions, cls).__new__(cls, batches)

    def numpy(self):
        return TupleActions([b.numpy() for b in self.batches])


def flatten_space(space):
    """Flattens a gym.Space into its primitive components.

    Primitive components are any non Tuple/Dict spaces.

    Args:
        space(gym.Space): The gym.Space to flatten. This may be any
            supported type (including nested Tuples and Dicts).

    Returns:
        List[gym.Space]: The flattened list of primitive Spaces. This list
            does not contain Tuples or Dicts anymore.
    """

    def _helper_flatten(space_, l):
        if isinstance(space_, Tuple):
            for s in space_:
                _helper_flatten(s, l)
        elif isinstance(space_, Dict):
            for k in space_.spaces:
                _helper_flatten(space_[k], l)
        else:
            l.append(space_)

    ret = []
    _helper_flatten(space, ret)
    return ret


def get_base_struct_from_space(space):
    def _helper_struct(space_):
        if isinstance(space_, Tuple):
            return [_helper_struct(s) for s in space_]
        elif isinstance(space_, Dict):
            return {k: _helper_struct(space_[k]) for k in space_.spaces}
        else:
            return space_

    return _helper_struct(space)
