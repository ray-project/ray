import os
import sys

import pytest

from ray import (
    ActorClassID,
    ActorID,
    ClusterID,
    FunctionID,
    JobID,
    NodeID,
    PlacementGroupID,
    TaskID,
    WorkerID,
)


@pytest.mark.parametrize(
    "id_cls, size",
    [
        (JobID, 4),
        (ActorID, 16),
        (TaskID, 24),
        (NodeID, 28),
        (WorkerID, 28),
        (FunctionID, 28),
        (ActorClassID, 28),
        (ClusterID, 28),
        (PlacementGroupID, 18),
    ],
)
def test_id_methods(id_cls, size):
    """
    Tests for ID class methods:

    Methods:
    - `ctor`
    - `from_binary(bytes: bytes) -> id_cls`
    - `binary() -> bytes`
    - `hex() -> str`
    - `from_hex(hex_str: str) -> id_cls`
    - `size(cls) -> int`
    - `nil() -> id_cls`
    - `is_nil() -> bool`
    - `__hash__() -> int`

    Invariants:
    - `ctor` is same as `from_binary`
    - `id_cls.from_binary(id.binary()) == id`
    - `id_cls.from_hex(id.hex()) == id`
    - `id_cls.size() == expected_size`
    - `id_cls.nil().binary() == b'\xff' * expected_size`
    - `id_cls.nil().hex() == 'ff' * expected_size`
    - `id_cls.nil().is_nil() is True`

    Note: we don't test ObjectID (that is, ObjectRef) here. It lacks from_binary and
    from_hex methods.
    """
    # Generate random bytes of the given size
    random_bytes = os.urandom(size)

    id_instance = id_cls(random_bytes)

    # binary and from_binary
    id_from_binary = id_cls.from_binary(random_bytes)
    assert id_instance == id_from_binary
    assert id_from_binary.binary() == random_bytes

    # hex and from_hex methods
    hex_str = id_instance.hex()
    assert isinstance(hex_str, str)
    assert id_cls.from_hex(hex_str) == id_instance

    # size() is classmethod, can be called on both class and instance
    assert id_cls.size() == size
    assert id_instance.size() == size

    # Test the nil method
    nil_instance = id_cls.nil()
    assert nil_instance.binary() == b"\xff" * size
    assert nil_instance.hex() == "ff" * size
    assert nil_instance.is_nil()

    # hash is implemented
    id_instance.__hash__() is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
