import string

from ray.data._internal.dataset_id import generate_dataset_ulid

BASE62_ALPHABET = string.digits + string.ascii_uppercase + string.ascii_lowercase


def test_generate_dataset_ulid_returns_time_sortable_ids():
    earlier_ulid = generate_dataset_ulid(get_time_ns=lambda: 0)
    later_ulid = generate_dataset_ulid(get_time_ns=lambda: 1_000_000)

    assert earlier_ulid < later_ulid


def test_generate_dataset_ulid_does_not_return_collisions():
    ulids = {generate_dataset_ulid() for _ in range(100_000)}

    assert len(ulids) == 100_000


def test_generate_dataset_ulid_only_uses_base62_chars():
    ulid = generate_dataset_ulid()

    assert all(char in BASE62_ALPHABET for char in ulid)


def test_generate_dataset_ulid_returns_short_id():
    ulid = generate_dataset_ulid()

    assert len(ulid) == 22


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
