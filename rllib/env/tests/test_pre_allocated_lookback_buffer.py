"""Tests for PreAllocatedLookbackBuffer.

Parity tests run against both InfiniteLookbackBuffer and
PreAllocatedLookbackBuffer to verify behavioural equivalence.
"""
import gymnasium as gym
import numpy as np
import pytest

from ray.rllib.env.utils.infinite_lookback_buffer import InfiniteLookbackBuffer
from ray.rllib.env.utils.pre_allocated_lookback_buffer import (
    PreAllocatedLookbackBuffer,
)
from ray.rllib.utils.test_utils import check

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

DICT_SPACE = gym.spaces.Dict(
    {
        "a": gym.spaces.Discrete(4),
        "b": gym.spaces.Box(0.0, 3.0, (2, 3), np.float32),
        "c": gym.spaces.Tuple(
            [
                gym.spaces.MultiDiscrete([4, 4]),
                gym.spaces.Box(0.0, 3.0, (1,), np.float32),
            ]
        ),
    }
)

_SAMPLES = [
    {
        "a": i % 4,
        "b": np.full((2, 3), i, dtype=np.float32),
        "c": (np.array([i % 4, i % 4]), np.array([i], dtype=np.float32)),
    }
    for i in range(8)
]

_SIMPLE_SPACE = gym.spaces.Discrete(20)

_BUFFER_CLASSES = [InfiniteLookbackBuffer, PreAllocatedLookbackBuffer]
_IDS = ["InfiniteLookbackBuffer", "PreAllocatedLookbackBuffer"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_pair():
    """ILB and PALB seeded with range(6), no lookback."""
    data = list(range(6))
    ilb = InfiniteLookbackBuffer(data=data, space=_SIMPLE_SPACE)
    ilb.finalize()
    palb = PreAllocatedLookbackBuffer(
        data=data, space=_SIMPLE_SPACE, initial_capacity=8
    )
    palb.finalize()
    return ilb, palb


@pytest.fixture
def lookback_pair():
    """ILB and PALB seeded with range(6), lookback=2."""
    data = list(range(6))
    ilb = InfiniteLookbackBuffer(data=data, lookback=2, space=_SIMPLE_SPACE)
    ilb.finalize()
    palb = PreAllocatedLookbackBuffer(
        data=data, lookback=2, space=_SIMPLE_SPACE, initial_capacity=8
    )
    palb.finalize()
    return ilb, palb


@pytest.fixture
def dict_pair():
    """ILB and PALB seeded with DICT_SPACE samples, lookback=2."""
    data = _SAMPLES[:6]
    ilb = InfiniteLookbackBuffer(data=data, lookback=2, space=DICT_SPACE)
    ilb.finalize()
    palb = PreAllocatedLookbackBuffer(
        data=data, lookback=2, space=DICT_SPACE, initial_capacity=8
    )
    palb.finalize()
    return ilb, palb


def _make_simple_pair():
    """Fresh mutable pair for tests that mutate buffers."""
    ilb = InfiniteLookbackBuffer(data=list(range(6)), space=_SIMPLE_SPACE)
    ilb.finalize()
    palb = PreAllocatedLookbackBuffer(
        data=list(range(6)), space=_SIMPLE_SPACE, initial_capacity=12
    )
    palb.finalize()
    return ilb, palb


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_requires_space():
    with pytest.raises(ValueError):
        PreAllocatedLookbackBuffer(space=None)


def test_empty_construction():
    buf = PreAllocatedLookbackBuffer(space=_SIMPLE_SPACE, initial_capacity=4)
    assert len(buf) == 0
    assert buf.len_incl_lookback() == 0
    assert buf.finalized
    assert buf._growing


def test_construction_with_data():
    data = list(range(5))
    buf = PreAllocatedLookbackBuffer(
        data=data, lookback=2, space=_SIMPLE_SPACE, initial_capacity=4
    )
    assert len(buf) == 3
    assert buf.lookback == 2
    assert buf._capacity >= len(data)
    assert buf._length == 5
    check(buf.get(), [2, 3, 4])


# ---------------------------------------------------------------------------
# Append / extend / pop
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", _BUFFER_CLASSES, ids=_IDS)
def test_append_extend_pop(cls):
    if cls is InfiniteLookbackBuffer:
        buf = cls(data=[0, 1, 2, 3], space=_SIMPLE_SPACE)
    else:
        buf = cls(data=[0, 1, 2, 3], space=_SIMPLE_SPACE, initial_capacity=8)

    assert len(buf) == 4
    buf.append(4)
    assert len(buf) == 5
    buf.append(5)
    assert len(buf) == 6
    buf.pop()
    assert len(buf) == 5
    buf.pop()
    assert len(buf) == 4
    buf.append(10)
    assert len(buf) == 5
    buf.finalize()

    # Post-finalize appends still work.
    buf.append(11)
    check(buf.get(indices=-1), 11)
    buf.extend([12, 13])
    check(buf.get(indices=-1), 13)
    buf.pop()
    check(buf.get(indices=-1), 12)
    check(buf.get(indices=0), 0)


def test_grow_doubles_capacity():
    buf = PreAllocatedLookbackBuffer(space=_SIMPLE_SPACE, initial_capacity=2)
    for i in range(10):
        buf.append(i)
    assert len(buf) == 10
    check(buf.get(), list(range(10)))
    assert buf._capacity >= 10


# ---------------------------------------------------------------------------
# finalize()
# ---------------------------------------------------------------------------


def test_finalize_trims_capacity():
    buf = PreAllocatedLookbackBuffer(space=_SIMPLE_SPACE, initial_capacity=128)
    for i in range(5):
        buf.append(i)
    assert buf._capacity == 128
    buf.finalize()
    assert buf._capacity == 5
    assert buf._length == 5
    assert not buf._growing
    check(buf.get(), list(range(5)))


def test_finalize_idempotent():
    buf = PreAllocatedLookbackBuffer(space=_SIMPLE_SPACE, initial_capacity=8)
    for i in range(3):
        buf.append(i)
    buf.finalize()
    buf.finalize()  # second call is a no-op
    assert buf._length == 3


def test_empty_finalize():
    buf = PreAllocatedLookbackBuffer(space=_SIMPLE_SPACE, initial_capacity=8)
    buf.finalize()
    assert buf._length == 0
    assert not buf._growing


# ---------------------------------------------------------------------------
# get() parity — simple space, no lookback
# ---------------------------------------------------------------------------


def test_get_all_simple(simple_pair):
    ilb, palb = simple_pair
    check(ilb.get(), palb.get())


def test_get_int_simple(simple_pair):
    ilb, palb = simple_pair
    for i in range(6):
        check(ilb.get(i), palb.get(i))


def test_get_negative_int_simple(simple_pair):
    ilb, palb = simple_pair
    for i in range(-1, -7, -1):
        check(ilb.get(i), palb.get(i))


def test_get_slice_simple(simple_pair):
    ilb, palb = simple_pair
    check(ilb.get(slice(1, 4)), palb.get(slice(1, 4)))
    check(ilb.get(slice(-3, None)), palb.get(slice(-3, None)))
    check(ilb.get(slice(None, -2)), palb.get(slice(None, -2)))


def test_get_list_indices_simple(simple_pair):
    ilb, palb = simple_pair
    check(ilb.get([0, 2, 4]), palb.get([0, 2, 4]))


def test_get_with_fill_simple(simple_pair):
    ilb, palb = simple_pair
    check(
        ilb.get(slice(-10, None), fill=99),
        palb.get(slice(-10, None), fill=99),
    )
    check(
        ilb.get(slice(3, 12), fill=99),
        palb.get(slice(3, 12), fill=99),
    )


# ---------------------------------------------------------------------------
# get() parity — with lookback
# ---------------------------------------------------------------------------


def test_len_lookback(lookback_pair):
    ilb, palb = lookback_pair
    assert len(ilb) == len(palb)


def test_len_incl_lookback(lookback_pair):
    ilb, palb = lookback_pair
    assert ilb.len_incl_lookback() == palb.len_incl_lookback()


def test_get_all_lookback(lookback_pair):
    ilb, palb = lookback_pair
    check(ilb.get(), palb.get())


def test_get_int_positive_lookback(lookback_pair):
    ilb, palb = lookback_pair
    for i in range(4):
        check(ilb.get(i), palb.get(i))


def test_get_int_negative_lookback(lookback_pair):
    ilb, palb = lookback_pair
    for i in range(-1, -5, -1):
        check(ilb.get(i), palb.get(i))


@pytest.mark.parametrize(
    "sl",
    [slice(-4, None), slice(None, -1), slice(0, 3), slice(-2, 2)],
    ids=["neg4_to_end", "start_to_neg1", "0_to_3", "neg2_to_2"],
)
def test_get_slice_lookback(lookback_pair, sl):
    ilb, palb = lookback_pair
    check(ilb.get(sl), palb.get(sl))


@pytest.mark.parametrize("i", [-1, -2])
def test_neg_index_as_lookback(lookback_pair, i):
    ilb, palb = lookback_pair
    check(
        ilb.get(i, neg_index_as_lookback=True),
        palb.get(i, neg_index_as_lookback=True),
    )


def test_fill_left_lookback(lookback_pair):
    ilb, palb = lookback_pair
    check(
        ilb.get(slice(-8, None), fill=0),
        palb.get(slice(-8, None), fill=0),
    )


def test_fill_right_lookback(lookback_pair):
    ilb, palb = lookback_pair
    check(
        ilb.get(slice(2, 8), fill=0),
        palb.get(slice(2, 8), fill=0),
    )


def test_out_of_range_raises(lookback_pair):
    _, palb = lookback_pair
    with pytest.raises(IndexError):
        palb.get(100)
    with pytest.raises(IndexError):
        palb.get(-100)


# ---------------------------------------------------------------------------
# get() parity — dict space
# ---------------------------------------------------------------------------


def test_get_all_dict(dict_pair):
    ilb, palb = dict_pair
    ilb_res = ilb.get()
    palb_res = palb.get()
    for key in ["a", "b"]:
        check(ilb_res[key], palb_res[key])


def test_get_int_dict(dict_pair):
    ilb, palb = dict_pair
    for i in range(4):
        ilb_res = ilb.get(i)
        palb_res = palb.get(i)
        check(ilb_res["a"], palb_res["a"])
        np.testing.assert_array_equal(ilb_res["b"], palb_res["b"])


def test_get_slice_dict(dict_pair):
    ilb, palb = dict_pair
    ilb_res = ilb.get(slice(0, 3))
    palb_res = palb.get(slice(0, 3))
    check(ilb_res["a"], palb_res["a"])
    np.testing.assert_array_equal(ilb_res["b"], palb_res["b"])


# ---------------------------------------------------------------------------
# set() parity
# ---------------------------------------------------------------------------


def test_set_int_index():
    ilb, palb = _make_simple_pair()
    ilb.set(15, at_indices=2)
    palb.set(15, at_indices=2)
    check(ilb.get(2), palb.get(2))


def test_set_slice():
    ilb, palb = _make_simple_pair()
    # new_data must be a numpy array so tree.map_structure sees matching
    # leaf types (ndarray vs ndarray, not ndarray vs list).
    new_data = np.array([10, 11])
    ilb.set(new_data, at_indices=slice(1, 3))
    palb.set(new_data, at_indices=slice(1, 3))
    check(ilb.get(slice(1, 3)), palb.get(slice(1, 3)))


def test_setitem():
    ilb, palb = _make_simple_pair()
    ilb[3] = 5
    palb[3] = 5
    check(ilb.get(3), palb.get(3))


# ---------------------------------------------------------------------------
# pop() parity
# ---------------------------------------------------------------------------


def test_pop_last():
    ilb = InfiniteLookbackBuffer(data=[0, 1, 2, 3, 4], space=_SIMPLE_SPACE)
    ilb.finalize()
    palb = PreAllocatedLookbackBuffer(
        data=[0, 1, 2, 3, 4], space=_SIMPLE_SPACE, initial_capacity=8
    )
    palb.finalize()
    ilb.pop(-1)
    palb.pop(-1)
    check(ilb.get(), palb.get())


def test_pop_middle():
    data = list(range(6))
    ilb = InfiniteLookbackBuffer(data=data, space=_SIMPLE_SPACE)
    ilb.finalize()
    palb = PreAllocatedLookbackBuffer(
        data=data, space=_SIMPLE_SPACE, initial_capacity=8
    )
    palb.finalize()
    ilb.pop(-3)
    palb.pop(-3)
    check(ilb.get(), palb.get())


def test_pop_out_of_range():
    palb = PreAllocatedLookbackBuffer(
        data=[0, 1, 2], space=_SIMPLE_SPACE, initial_capacity=4
    )
    with pytest.raises(IndexError):
        palb.pop(100)
    with pytest.raises(IndexError):
        palb.pop(-100)


# ---------------------------------------------------------------------------
# concat() / extend()
# ---------------------------------------------------------------------------


def test_concat():
    a = PreAllocatedLookbackBuffer(
        data=[0, 1, 2], space=_SIMPLE_SPACE, initial_capacity=8
    )
    b = PreAllocatedLookbackBuffer(
        data=[3, 4, 5], space=_SIMPLE_SPACE, initial_capacity=8
    )
    a.concat(b)
    check(a.get(), list(range(6)))


def test_extend():
    buf = PreAllocatedLookbackBuffer(
        data=[0, 1], space=_SIMPLE_SPACE, initial_capacity=8
    )
    buf.extend([2, 3, 4])
    check(buf.get(), list(range(5)))


# ---------------------------------------------------------------------------
# State serialization
# ---------------------------------------------------------------------------


def test_state_roundtrip():
    data = list(range(6))
    original = PreAllocatedLookbackBuffer(
        data=data, lookback=2, space=_SIMPLE_SPACE, initial_capacity=8
    )
    original.finalize()
    state = original.get_state()
    restored = PreAllocatedLookbackBuffer.from_state(state)
    assert restored._length == original._length
    assert restored.lookback == original.lookback
    assert not restored._growing
    check(restored.get(), original.get())


def test_state_roundtrip_dict_space():
    data = _SAMPLES[:4]
    original = PreAllocatedLookbackBuffer(
        data=data, lookback=1, space=DICT_SPACE, initial_capacity=8
    )
    original.finalize()
    state = original.get_state()
    restored = PreAllocatedLookbackBuffer.from_state(state)
    orig_res = original.get()
    rest_res = restored.get()
    check(orig_res["a"], rest_res["a"])
    np.testing.assert_array_equal(orig_res["b"], rest_res["b"])


# ---------------------------------------------------------------------------
# one_hot_discrete parity
# ---------------------------------------------------------------------------


def test_one_hot_discrete():
    space = gym.spaces.Discrete(4)
    data = [0, 1, 2, 3, 0, 1]
    ilb = InfiniteLookbackBuffer(data=data, space=space)
    ilb.finalize()
    palb = PreAllocatedLookbackBuffer(data=data, space=space, initial_capacity=8)
    palb.finalize()
    ilb_res = ilb.get(one_hot_discrete=True)
    palb_res = palb.get(one_hot_discrete=True)
    np.testing.assert_array_equal(ilb_res, palb_res)


