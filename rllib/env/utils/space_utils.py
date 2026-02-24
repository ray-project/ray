from functools import singledispatch
from typing import Any

import numpy as np
from gymnasium.spaces import (
    Box,
    Dict,
    Discrete,
    Graph,
    GraphInstance,
    MultiBinary,
    MultiDiscrete,
    OneOf,
    Sequence,
    Space,
    Text,
    Tuple,
)


@singledispatch
def create_mutable_array(
    space: Space, n: int = 1
) -> tuple[Any, ...] | dict[str, Any] | np.ndarray:
    """Create an empty (possibly nested and normally numpy-based) array, used in conjunction with ``concatenate(..., out=array)``.

    In most cases, the array will be contained within the batched space, however, this is not guaranteed.

    Args:
        space: Observation space of a single environment in the vectorized environment.
        n: Number of environments in the vectorized environment. If ``None``, creates an empty sample from ``space``.

    Returns:
        The output object. This object is a (possibly nested) numpy array.

    Raises:
        ValueError: Space is not a valid :class:`gymnasium.Space` instance

    Example:
        >>> from gymnasium.spaces import Box, Dict
        >>> import numpy as np
        >>> space = Dict({
        ... 'position': Box(low=0, high=1, shape=(3,), dtype=np.float32),
        ... 'velocity': Box(low=0, high=1, shape=(2,), dtype=np.float32)})
        >>> create_mutable_array(space, n=2, fn=np.zeros)
        {'position': array([[0., 0., 0.],
               [0., 0., 0.]], dtype=float32), 'velocity': array([[0., 0.],
               [0., 0.]], dtype=float32)}
    """
    raise TypeError(
        f"The space provided to `create_mutable_array` is not a gymnasium Space instance, type: {type(space)}, {space}"
    )


# It is possible for some of the Box low to be greater than 0, then array is not in space
@create_mutable_array.register(Box)
# If the Discrete start > 0 or start + length < 0 then array is not in space
@create_mutable_array.register(Discrete)
@create_mutable_array.register(MultiDiscrete)
@create_mutable_array.register(MultiBinary)
def _create_mutable_array_multi(space: Box, n: int = 1) -> np.ndarray:
    return np.zeros((n,) + space.shape, dtype=space.dtype)


@create_mutable_array.register(Tuple)
def _create_mutable_array_tuple(space: Tuple, n: int = 1) -> list[Any]:
    return [create_mutable_array(subspace, n=n) for subspace in space.spaces]


@create_mutable_array.register(Dict)
def _create_mutable_array_dict(space: Dict, n: int = 1, fn=np.zeros) -> dict[str, Any]:
    return {key: create_mutable_array(subspace, n=n) for key, subspace in space.items()}


@create_mutable_array.register(Graph)
def _create_mutable_array_graph(space: Graph, n: int = 1) -> list[GraphInstance]:
    if space.edge_space is not None:
        return [
            GraphInstance(
                nodes=np.zeros(
                    (1,) + space.node_space.shape, dtype=space.node_space.dtype
                ),
                edges=np.zeros(
                    (1,) + space.edge_space.shape, dtype=space.edge_space.dtype
                ),
                edge_links=np.zeros((1, 2), dtype=np.int64),
            )
            for _ in range(n)
        ]
    else:
        return [
            GraphInstance(
                nodes=np.zeros(
                    (1,) + space.node_space.shape, dtype=space.node_space.dtype
                ),
                edges=None,
                edge_links=None,
            )
            for _ in range(n)
        ]


@create_mutable_array.register(Text)
def _create_mutable_array_text(space: Text, n: int = 1) -> list[str]:
    return [space.characters[0] * space.min_length for _ in range(n)]


@create_mutable_array.register(Sequence)
def _create_mutable_array_sequence(space: Sequence, n: int = 1) -> list[Any]:
    if space.stack:
        return [create_mutable_array(space.feature_space, n=1) for _ in range(n)]
    else:
        return [list() for _ in range(n)]


@create_mutable_array.register(OneOf)
def _create_mutable_array_oneof(space: OneOf, n: int = 1):
    return [list() for _ in range(n)]


@singledispatch
def write_to_buffer(space: Space, buffer: Any, index: int | slice, sample: Any) -> None:
    """Write to a mutable buffer in the index position with the sample."""
    raise NotImplementedError


@write_to_buffer.register(Box)
@write_to_buffer.register(Discrete)
@write_to_buffer.register(MultiDiscrete)
@write_to_buffer.register(MultiBinary)
def _fundamental_write_to_buffer(
    space: Space, buffer: Any, index: int | slice, sample: Any
) -> None:
    buffer[index] = sample


@write_to_buffer.register(Tuple)
def _tuple_write_to_buffer(
    space: Tuple, buffer: Any, index: int | slice, sample: Any
) -> None:
    for subspace, subbuffer, subsample in zip(space, buffer, sample):
        write_to_buffer(subspace, subbuffer, index, subsample)


@write_to_buffer.register(Dict)
def _dict_write_to_buffer(
    space: Dict, buffer: Any, index: int | slice, sample: Any
) -> None:
    for key in space.keys():
        write_to_buffer(space[key], buffer[key], index, sample[key])


@write_to_buffer.register(Graph)
@write_to_buffer.register(Text)
@write_to_buffer.register(Sequence)
@write_to_buffer.register(OneOf)
def _list_write_to_buffer(
    space: Space, buffer: Any, index: int | slice, sample: Any
) -> None:
    buffer[index] = sample


@singledispatch
def read_from_buffer(space: Space, buffer: Any, index: int | slice) -> Any:
    """Read from a mutable buffer in the index position with the sample."""
    raise NotImplementedError


@read_from_buffer.register(Box)
@read_from_buffer.register(Discrete)
@read_from_buffer.register(MultiDiscrete)
@read_from_buffer.register(MultiBinary)
def _fundamental_read_from_buffer(space: Space, buffer: Any, index: int | slice) -> Any:
    return buffer[index]


@read_from_buffer.register(Tuple)
def _tuple_read_from_buffer(space: Tuple, buffer: Any, index: int | slice) -> Any:
    return tuple(
        read_from_buffer(ss, sb, index) for ss, sb in zip(space.spaces, buffer)
    )


@read_from_buffer.register(Dict)
def _dict_read_from_buffer(space: Dict, buffer: Any, index: int | slice) -> Any:
    return {
        key: read_from_buffer(space[key], buffer[key], index) for key in space.keys()
    }


@read_from_buffer.register(Graph)
@read_from_buffer.register(Text)
@read_from_buffer.register(Sequence)
@read_from_buffer.register(OneOf)
def _list_read_from_buffer(space: Space, buffer: Any, index: int | slice) -> Any:
    return buffer[index]
