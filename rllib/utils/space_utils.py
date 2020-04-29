from gym.spaces import Tuple, Dict
import numpy as np

from ray.rllib.utils import try_import_tree

tree = try_import_tree()


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
    """Returns a Tuple/Dict Space as native (equally structured) py tuple/dict.

    Args:
        space (gym.Space): The Space to get the python struct for.

    Returns:
        Union[dict,tuple,gym.Space]: The struct equivalent to the given Space.
            Note that the returned struct still contains all original
            "primitive" Spaces (e.g. Box, Discrete).

    Examples:
        >>> get_base_struct_from_space(Dict({
        >>>     "a": Box(),
        >>>     "b": Tuple([Discrete(2), Discrete(3)])
        >>> }))
        >>> # Will return: dict(a=Box(), b=tuple(Discrete(2), Discrete(3)))
    """

    def _helper_struct(space_):
        if isinstance(space_, Tuple):
            return tuple(_helper_struct(s) for s in space_)
        elif isinstance(space_, Dict):
            return {k: _helper_struct(space_[k]) for k in space_.spaces}
        else:
            return space_

    return _helper_struct(space)


def flatten_to_single_ndarray(input_):
    """Returns a single np.ndarray given a list/tuple of np.ndarrays.

    Args:
        input_ (Union[List[np.ndarray],np.ndarray]): The list of ndarrays or
            a single ndarray.

    Returns:
        np.ndarray: The result after concatenating all single arrays in input_.

    Examples:
        >>> flatten_to_single_ndarray([
        >>>     np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]),
        >>>     np.array([7, 8, 9]),
        >>> ])
        >>> # Will return:
        >>> # np.array([
        >>> #     1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0
        >>> # ])
    """
    # Concatenate complex inputs.
    if isinstance(input_, (list, tuple, dict)):
        expanded = []
        for in_ in tree.flatten(input_):
            expanded.append(np.reshape(in_, [-1]))
        input_ = np.concatenate(expanded, axis=0).flatten()
    return input_
