import gym
from gym.spaces import Tuple, Dict
import numpy as np
from ray.rllib.utils.annotations import DeveloperAPI
import tree  # pip install dm_tree
from typing import Any, List, Optional, Union


def flatten_space(space: gym.Space) -> List[gym.Space]:
    """Flattens a gym.Space into its primitive components.

    Primitive components are any non Tuple/Dict spaces.

    Args:
        space (gym.Space): The gym.Space to flatten. This may be any
            supported type (including nested Tuples and Dicts).

    Returns:
        List[gym.Space]: The flattened list of primitive Spaces. This list
            does not contain Tuples or Dicts anymore.
    """

    def _helper_flatten(space_, return_list):
        from ray.rllib.utils.spaces.flexdict import FlexDict
        if isinstance(space_, Tuple):
            for s in space_:
                _helper_flatten(s, return_list)
        elif isinstance(space_, (Dict, FlexDict)):
            for k in space_.spaces:
                _helper_flatten(space_[k], return_list)
        else:
            return_list.append(space_)

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


def get_dummy_batch_for_space(
        space: gym.Space,
        batch_size: int = 32,
        fill_value: Union[float, int, str] = 0.0,
        time_size: Optional[int] = None,
        time_major: bool = False,
) -> np.ndarray:
    """Returns batched dummy data (using `batch_size`) for the given `space`.

    Note: The returned batch will not pass a `space.contains(batch)` test
    as an additional batch dimension has to be added as dim=0.

    Args:
        space (gym.Space): The space to get a dummy batch for.
        batch_size(int): The required batch size (B). Note that this can also
            be 0 (only if `time_size` is None!), which will result in a
            non-batched sample for the given space (no batch dim).
        fill_value (Union[float, int, str]): The value to fill the batch with
            or "random" for random values.
        time_size (Optional[int]): If not None, add an optional time axis
            of `time_size` size to the returned batch.
        time_major (bool): If True AND `time_size` is not None, return batch
            as shape [T x B x ...], otherwise as [B x T x ...]. If `time_size`
            if None, ignore this setting and return [B x ...].

    Returns:
        The dummy batch of size `bqtch_size` matching the given space.
    """
    # Complex spaces. Perform recursive calls of this function.
    if isinstance(space, (gym.spaces.Dict, gym.spaces.Tuple)):
        return tree.map_structure(
            lambda s: get_dummy_batch_for_space(s, batch_size, fill_value),
            get_base_struct_from_space(space),
        )
    # Primivite spaces: Box, Discrete, MultiDiscrete.
    # Random values: Use gym's sample() method.
    elif fill_value == "random":
        if time_size is not None:
            assert batch_size > 0 and time_size > 0
            if time_major:
                return np.array(
                    [[space.sample() for _ in range(batch_size)]
                     for t in range(time_size)],
                    dtype=space.dtype)
            else:
                return np.array(
                    [[space.sample() for t in range(time_size)]
                     for _ in range(batch_size)],
                    dtype=space.dtype)
        else:
            return np.array(
                [space.sample() for _ in range(batch_size)]
                if batch_size > 0 else space.sample(),
                dtype=space.dtype)
    # Fill value given: Use np.full.
    else:
        if time_size is not None:
            assert batch_size > 0 and time_size > 0
            if time_major:
                shape = [time_size, batch_size]
            else:
                shape = [batch_size, time_size]
        else:
            shape = [batch_size] if batch_size > 0 else []
        return np.full(
            shape + list(space.shape),
            fill_value=fill_value,
            dtype=space.dtype)


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


def clip_action(action, action_space):
    """Clips all components in `action` according to the given Space.

    Only applies to Box components within the action space.

    Args:
        action (Any): The action to be clipped. This could be any complex
            action, e.g. a dict or tuple.
        action_space (Any): The action space struct,
            e.g. `{"a": Distrete(2)}` for a space: Dict({"a": Discrete(2)}).

    Returns:
        Any: The input action, but clipped by value according to the space's
            bounds.
    """

    def map_(a, s):
        if isinstance(s, gym.spaces.Box):
            a = np.clip(a, s.low, s.high)
        return a

    return tree.map_structure(map_, action, action_space)


def unsquash_action(action, action_space_struct):
    """Unsquashes all components in `action` according to the given Space.

    Inverse of `normalize_action()`. Useful for mapping policy action
    outputs (normalized between -1.0 and 1.0) to an env's action space.
    Unsquashing results in cont. action component values between the
    given Space's bounds (`low` and `high`). This only applies to Box
    components within the action space, whose dtype is float32 or float64.

    Args:
        action (Any): The action to be unsquashed. This could be any complex
            action, e.g. a dict or tuple.
        action_space_struct (Any): The action space struct,
            e.g. `{"a": Box()}` for a space: Dict({"a": Box()}).

    Returns:
        Any: The input action, but unsquashed, according to the space's
            bounds. An unsquashed action is ready to be sent to the
            environment (`BaseEnv.send_actions([unsquashed actions])`).
    """

    def map_(a, s):
        if isinstance(s, gym.spaces.Box) and \
                (s.dtype == np.float32 or s.dtype == np.float64) and \
                np.all(s.bounded_below) and np.all(s.bounded_above):
            # Assuming values are roughly between -1.0 and 1.0 ->
            # unsquash them to the given bounds.
            a = s.low + (a + 1.0) * (s.high - s.low) / 2.0
            # Clip to given bounds, just in case the squashed values were
            # outside [-1.0, 1.0].
            a = np.clip(a, s.low, s.high)
        return a

    return tree.map_structure(map_, action, action_space_struct)


def normalize_action(action, action_space_struct):
    """Normalizes all (Box) components in `action` to be in [-1.0, 1.0].

    Inverse of `unsquash_action()`. Useful for mapping an env's action
    (arbitrary bounded values) to a [-1.0, 1.0] interval.
    This only applies to Box components within the action space, whose
    dtype is float32 or float64.

    Args:
        action (Any): The action to be normalized. This could be any complex
            action, e.g. a dict or tuple.
        action_space_struct (Any): The action space struct,
            e.g. `{"a": Box()}` for a space: Dict({"a": Box()}).

    Returns:
        Any: The input action, but normalized, according to the space's
            bounds.
    """

    def map_(a, s):
        if isinstance(s, gym.spaces.Box) and \
                (s.dtype == np.float32 or s.dtype == np.float64):
            # Normalize values to be exactly between -1.0 and 1.0.
            a = ((a - s.low) * 2.0) / (s.high - s.low) - 1.0
        return a

    return tree.map_structure(map_, action, action_space_struct)


@DeveloperAPI
def convert_element_to_space_type(element: Any, sampled_element: Any) -> Any:
    """Convert all the components of the element to match the space dtypes.

    Args:
        element: The element to be converted.
        sampled_element: An element sampled from a space to be matched
            to.

    Returns:
        The input element, but with all its components converted to match
        the space dtypes.
    """

    def map_(elem, s):
        if isinstance(s, np.ndarray):
            if not isinstance(elem, np.ndarray):
                raise ValueError(
                    "Element should be of type np.ndarray but is instead of \
                        type {}".format(type(elem)))
            elif (s.dtype != elem.dtype):
                elem = elem.astype(s.dtype)

        elif isinstance(s, int):
            if isinstance(elem, float):
                elem = int(elem)
        return elem

    return tree.map_structure(
        map_, element, sampled_element, check_types=False)
