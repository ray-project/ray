from gym.spaces import Tuple, Dict
import numpy as np
import tree


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
        from ray.rllib.utils.spaces.flexdict import FlexDict
        if isinstance(space_, Tuple):
            for s in space_:
                _helper_flatten(s, l)
        elif isinstance(space_, (Dict, FlexDict)):
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
        input_ (Union[List[np.ndarray], np.ndarray]): The list of ndarrays or
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


def unbatch(batches_struct):
    """Converts input from (nested) struct of batches to batch of structs.

    Input: Struct of different batches (each batch has size=3):
        {"a": [1, 2, 3], "b": ([4, 5, 6], [7.0, 8.0, 9.0])}
    Output: Batch (list) of structs (each of these structs representing a
        single action):
        [
            {"a": 1, "b": (4, 7.0)},  <- action 1
            {"a": 2, "b": (5, 8.0)},  <- action 2
            {"a": 3, "b": (6, 9.0)},  <- action 3
        ]

    Args:
        batches_struct (any): The struct of component batches. Each leaf item
            in this struct represents the batch for a single component
            (in case struct is tuple/dict).
            Alternatively, `batches_struct` may also simply be a batch of
            primitives (non tuple/dict).

    Returns:
        List[struct[components]]: The list of rows. Each item
            in the returned list represents a single (maybe complex) struct.
    """
    flat_batches = tree.flatten(batches_struct)

    out = []
    for batch_pos in range(len(flat_batches[0])):
        out.append(
            tree.unflatten_as(
                batches_struct,
                [flat_batches[i][batch_pos]
                 for i in range(len(flat_batches))]))
    return out
