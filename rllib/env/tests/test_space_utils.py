import numpy as np
import pytest
from gymnasium.spaces import (
    Box,
    Dict,
    Discrete,
    Graph,
    MultiBinary,
    MultiDiscrete,
    OneOf,
    Sequence,
    Space,
    Text,
    Tuple,
)
from gymnasium.utils.env_checker import data_equivalence
from gymnasium.vector.utils import concatenate
from space_utils import create_mutable_array, read_from_buffer, write_to_buffer

TESTING_FUNDAMENTAL_SPACES = [
    Discrete(3),
    Discrete(3, start=-1),
    Discrete(n=4, dtype=np.int32, start=1),
    Box(low=0.0, high=1.0),
    Box(low=0.0, high=np.inf, shape=(2, 2)),
    Box(low=np.array([-10.0, 0.0]), high=np.array([10.0, 10.0]), dtype=np.float64),
    Box(low=-np.inf, high=0.0, shape=(2, 1)),
    Box(low=0.0, high=np.inf, shape=(2, 1)),
    Box(low=0, high=255, shape=(2, 2, 3), dtype=np.uint8),
    Box(low=np.array([0, 0, 1]), high=np.array([1, 0, 1]), dtype=np.bool_),
    Box(
        low=np.array([-np.inf, -np.inf, 0, -10]),
        high=np.array([np.inf, 0, np.inf, 10]),
        dtype=np.int32,
    ),
    MultiDiscrete([2, 2]),
    MultiDiscrete([[2, 3], [3, 2]]),
    MultiDiscrete([2, 2], start=[10, 10]),
    MultiDiscrete([[2, 3], [3, 2]], start=[[10, 20], [30, 40]]),
    MultiDiscrete([2, 3], dtype=np.int8),
    MultiDiscrete([2, 3], dtype=np.uint16),
    MultiBinary(8),
    MultiBinary([2, 3]),
    Text(6),
    Text(min_length=3, max_length=6),
    Text(6, charset="abcdef"),
]
TESTING_FUNDAMENTAL_SPACES_IDS = [f"{space}" for space in TESTING_FUNDAMENTAL_SPACES]


TESTING_COMPOSITE_SPACES = [
    # Tuple spaces
    Tuple([Discrete(5), Discrete(4)]),
    Tuple(
        (
            Discrete(5),
            Box(
                low=np.array([0.0, 0.0]),
                high=np.array([1.0, 5.0]),
                dtype=np.float64,
            ),
        )
    ),
    Tuple((Discrete(5), Tuple((Box(low=0.0, high=1.0, shape=(3,)), Discrete(2))))),
    Tuple((Discrete(3), Dict(position=Box(low=0.0, high=1.0), velocity=Discrete(2)))),
    Tuple((Graph(node_space=Box(-1, 1, shape=(2, 1)), edge_space=None), Discrete(2))),
    # Dict spaces
    Dict(
        {
            "position": Discrete(5),
            "velocity": Box(
                low=np.array([0.0, 0.0]),
                high=np.array([1.0, 5.0]),
                dtype=np.float64,
            ),
        }
    ),
    Dict(
        position=Discrete(6),
        velocity=Box(
            low=np.array([0.0, 0.0]),
            high=np.array([1.0, 5.0]),
            dtype=np.float64,
        ),
    ),
    Dict(
        {
            "a": Box(low=0, high=1, shape=(3, 3)),
            "b": Dict(
                {
                    "b_1": Box(low=-100, high=100, shape=(2,)),
                    "b_2": Box(low=-1, high=1, shape=(2,)),
                }
            ),
            "c": Discrete(4),
        }
    ),
    Dict(
        a=Dict(
            a=Graph(node_space=Box(-100, 100, shape=(2, 2)), edge_space=None),
            b=Box(-100, 100, shape=(2, 2)),
        ),
        b=Tuple((Box(-100, 100, shape=(2,)), Box(-100, 100, shape=(2,)))),
    ),
    # Graph spaces
    Graph(node_space=Box(-1, 1, shape=(2,)), edge_space=None),
    Graph(node_space=Box(low=-100, high=100, shape=(3, 4)), edge_space=Discrete(5)),
    Graph(node_space=Discrete(5), edge_space=Box(low=-100, high=100, shape=(3, 4))),
    Graph(node_space=Discrete(3), edge_space=Discrete(4)),
    # Sequence spaces
    Sequence(Discrete(4)),
    Sequence(Dict({"feature": Box(0, 1, (3,))})),
    Sequence(Graph(node_space=Box(-100, 100, shape=(2, 2)), edge_space=Discrete(4))),
    Sequence(Box(low=0.0, high=1.0), stack=True),
    Sequence(Dict({"a": Box(0, 1, (3,)), "b": Discrete(5)}), stack=True),
    # OneOf spaces
    OneOf([Discrete(3), Box(low=0.0, high=1.0)]),
    OneOf([MultiBinary(2), MultiDiscrete([2, 2])]),
]
TESTING_COMPOSITE_SPACES_IDS = [f"{space}" for space in TESTING_COMPOSITE_SPACES]

TESTING_SPACES: list[Space] = TESTING_FUNDAMENTAL_SPACES + TESTING_COMPOSITE_SPACES
TESTING_SPACES_IDS = TESTING_FUNDAMENTAL_SPACES_IDS + TESTING_COMPOSITE_SPACES_IDS
assert len(TESTING_SPACES_IDS) == len(TESTING_SPACES)


def _fix_concatenate(space, result):
    if isinstance(space, (Text, Graph, Sequence, OneOf)):
        return list(result)
    elif isinstance(space, Tuple):
        return tuple(_fix_concatenate(ss, r) for ss, r in zip(space.spaces, result))
    elif isinstance(space, Dict):
        return {k: _fix_concatenate(space[k], result[k]) for k in space.keys()}
    return result  # numpy-backed: leave as-is


@pytest.mark.parametrize("space", TESTING_SPACES, ids=TESTING_SPACES_IDS)
def test_read_write(space, size: int = 10):
    buffer = create_mutable_array(space, size)

    samples = [space.sample() for _ in range(size)]
    for idx, sample in enumerate(samples):
        write_to_buffer(space, buffer, idx, sample)

    for idx in [0, 1, -1]:
        expected_sample = samples[idx]
        read_sample = read_from_buffer(space, buffer, idx)

        assert data_equivalence(expected_sample, read_sample)

    idxs = slice(3, 7)
    expected_samples = _fix_concatenate(
        space,
        concatenate(
            space, samples[idxs], create_mutable_array(space, len(samples[idxs]))
        ),
    )
    read_samples = read_from_buffer(space, buffer, idxs)
    assert data_equivalence(expected_samples, read_samples)
