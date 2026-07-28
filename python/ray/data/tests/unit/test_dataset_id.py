from unittest.mock import patch

from ray.data._internal.dataset_id import generate_dataset_ulid

BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def test_generate_dataset_ulid():
    """Dataset ULIDs are fixed-width, Base62, unique, and time-sortable."""
    with (
        patch(
            "ray.data._internal.dataset_id.time.time_ns",
            side_effect=[0, 1_000_000, 1_000_000, 2_000_000],
        ),
        patch(
            "ray.data._internal.dataset_id.os.urandom",
            side_effect=[
                b"\x00" * 10,
                b"\x00" * 10,
                b"\xff" * 10,
                b"\x00" * 10,
            ],
        ),
    ):
        zero_ulid = generate_dataset_ulid()
        same_time_low = generate_dataset_ulid()
        same_time_high = generate_dataset_ulid()
        later_ulid = generate_dataset_ulid()

    assert zero_ulid == "0" * 22

    for ulid in (zero_ulid, same_time_low, same_time_high, later_ulid):
        assert len(ulid) == 22
        assert all(char in BASE62_ALPHABET for char in ulid)

    assert same_time_low != same_time_high
    assert same_time_high < later_ulid


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
