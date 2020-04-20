from gym.spaces import Tuple, Dict


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
    """
    Returns a Tuple/Dict Space as native (and equally structured) tuple/dict.

    Args:
        space (gym.Space): The Space to get the python struct for.

    Returns:
        Union[dict,tuple,gym.Space]: The struct equivalent to the given Space.
            Note that the returned struct still contains all original
            "primitive" Spaces (e.g. Box, Discrete).
    """
    def _helper_struct(space_):
        if isinstance(space_, Tuple):
            return tuple(_helper_struct(s) for s in space_)
        elif isinstance(space_, Dict):
            return {k: _helper_struct(space_[k]) for k in space_.spaces}
        else:
            return space_

    return _helper_struct(space)
