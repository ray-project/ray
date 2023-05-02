# coding: utf-8
import os
import sys

import pytest  # noqa
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage


@pytest.mark.parametrize("storage", [InMemoryStorage()])
def test_storage(storage):
    assert storage.get_version() == 0
    assert storage.get_all(table="test_table") == ({}, 0)
    assert storage.get(table="test_table", keys=[]) == ({}, 0)
    assert storage.get(table="test_table", keys=["key1"]) == ({}, 0)

    assert storage.batch_update(table="test_table", mutation={"key1": "value1"}) == [
        True,
        1,
    ]

    assert storage.get_version() == 1

    assert storage.get_all(table="test_table") == ({"key1": ("value1", 1)}, 1)
    assert storage.get(table="test_table", keys=[]) == ({"key1": ("value1", 1)}, 1)

    assert storage.batch_update(
        table="test_table", mutation={"key1": "value2"}, expected_version=0
    ) == [False, 1]

    assert storage.batch_update(
        table="test_table", mutation={"key1": "value2"}, expected_version=1
    ) == [True, 2]

    assert storage.get_all(table="test_table") == ({"key1": ("value2", 2)}, 2)

    assert storage.batch_update(
        table="test_table",
        mutation={"key2": "value3", "key3": "value4"},
        deletion=["key1"],
        expected_version=2,
    ) == [True, 3]

    assert storage.get_all(table="test_table") == (
        {"key2": ("value3", 3), "key3": ("value4", 3)},
        3,
    )

    assert storage.get(table="test_table", keys=["key2", "key1"]) == (
        {"key2": ("value3", 3)},
        3,
    )

    assert storage.update(table="test_table", key="key2", value="value5") == [True, 4]
    assert storage.update(
        table="test_table", key="key2", value="value5", insert_only=True
    ) == [False, 4]
    assert storage.update(
        table="test_table", key="key2", value="value5", expected_entry_version=3
    ) == [False, 4]
    assert storage.update(
        table="test_table", key="key2", value="value6", expected_entry_version=4
    ) == [True, 5]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
