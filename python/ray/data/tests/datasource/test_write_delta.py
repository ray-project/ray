"""Tests for ``Dataset.write_delta`` (PR 3 -- APPEND MVP).

This file is added in PR 3 and grows across PRs 4-7 as new modes/features
land. Each PR's tests are clearly grouped and labelled.

Master's existing ``test_delta.py`` covers ``ray.data.read_delta`` and is
not modified by any of these PRs.
"""

import os
from typing import Any, Dict, List

import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

try:
    import deltalake as _deltalake_check  # noqa: F401
except ImportError:
    _deltalake_check = None

_pa_version = get_pyarrow_version()
# Use skipif (not importorskip) so tests are still collected and show up as
# skipped with a clear reason when optional deps are missing.
pytestmark = [
    pytest.mark.skipif(
        _deltalake_check is None,
        reason="Missing optional dependency: pip install deltalake",
    ),
    pytest.mark.skipif(
        _pa_version is None or _pa_version < parse_version("15.0.0"),
        reason="deltalake requires pyarrow >= 15.0",
    ),
]


@pytest.fixture
def temp_delta_path(tmp_path):
    """Clean per-test path for a Delta table."""
    return os.path.join(str(tmp_path), "delta_table")


def _write_append(rows: List[Dict[str, Any]], path: str, **kwargs) -> None:
    ray.data.from_items(rows).write_delta(path, mode="append", **kwargs)


def _read_all(path: str, **kwargs) -> List[Dict[str, Any]]:
    return ray.data.read_delta(path, **kwargs).take_all()


def _log_exists(path: str) -> bool:
    return os.path.isdir(os.path.join(path, "_delta_log"))


# ----------------------------------------------------------------------
# PR 3 -- APPEND MVP.
# ----------------------------------------------------------------------


def test_append_to_new_path_creates_table(temp_delta_path):
    rows = [{"id": i, "v": f"r{i}"} for i in range(5)]
    _write_append(rows, temp_delta_path)
    assert _log_exists(temp_delta_path)
    assert sorted(_read_all(temp_delta_path), key=lambda r: r["id"]) == rows


def test_append_to_existing_table_sums_rows(temp_delta_path):
    first = [{"id": i, "v": f"a{i}"} for i in range(3)]
    second = [{"id": i + 100, "v": f"b{i}"} for i in range(2)]
    _write_append(first, temp_delta_path)
    _write_append(second, temp_delta_path)
    assert len(_read_all(temp_delta_path)) == 5


def test_explicit_schema_round_trip(temp_delta_path):
    schema = pa.schema([("id", pa.int64()), ("v", pa.string())])
    rows = [{"id": 1, "v": "x"}, {"id": 2, "v": "y"}]
    _write_append(rows, temp_delta_path, schema=schema)
    out = _read_all(temp_delta_path)
    assert len(out) == 2


def test_append_empty_dataset_with_schema_creates_empty_table(temp_delta_path):
    schema = pa.schema([("id", pa.int64())])
    ray.data.from_items([], override_num_blocks=1).write_delta(
        temp_delta_path, mode="append", schema=schema
    )
    # Table exists, no rows.
    assert _log_exists(temp_delta_path)
    assert _read_all(temp_delta_path) == []


def _delta_log_json_count(path: str) -> int:
    """Count the *.json files inside _delta_log/ (one per Delta version)."""
    log_dir = os.path.join(path, "_delta_log")
    if not os.path.isdir(log_dir):
        return 0
    return sum(1 for f in os.listdir(log_dir) if f.endswith(".json"))


def test_append_creates_one_delta_log_file_per_call(temp_delta_path):
    _write_append([{"id": 1}], temp_delta_path)
    assert _delta_log_json_count(temp_delta_path) == 1
    assert os.path.isfile(
        os.path.join(temp_delta_path, "_delta_log", "00000000000000000000.json")
    )
    _write_append([{"id": 2}], temp_delta_path)
    assert _delta_log_json_count(temp_delta_path) == 2
    assert os.path.isfile(
        os.path.join(temp_delta_path, "_delta_log", "00000000000000000001.json")
    )


def test_multiple_blocks_commit_in_single_version(temp_delta_path):
    """Multi-block, multi-task write must produce exactly one new Delta
    log entry, even though every task writes its own file(s)."""
    import json

    rows = [{"id": i, "v": f"r{i}"} for i in range(20)]
    ray.data.from_items(rows, override_num_blocks=4).write_delta(temp_delta_path)
    assert _delta_log_json_count(temp_delta_path) == 1

    log_path = os.path.join(temp_delta_path, "_delta_log", "00000000000000000000.json")
    add_count = 0
    with open(log_path) as f:
        for line in f:
            entry = json.loads(line)
            if "add" in entry:
                add_count += 1
    assert add_count >= 4, f"Expected >=4 AddActions, got {add_count}"
    assert len(_read_all(temp_delta_path)) == 20


def test_unsupported_mode_raises_clear_error(temp_delta_path):
    """OVERWRITE/ERROR/IGNORE land in PR 4 -- until then they must be
    rejected cleanly by the framework."""
    with pytest.raises(ValueError, match="does not support mode"):
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode="overwrite")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
