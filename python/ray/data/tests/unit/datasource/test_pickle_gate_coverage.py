"""every datasource module that reads Arrow data from an external
source must call ``raise_on_pickle_object_columns``.

``ray.data.arrow_pickled_object`` columns unpickle on access. A reader that hands
such a column from an untrusted file to the user is a remote code execution bug.
This scan is heuristic: it flags modules that contain a
known Arrow-producing read call but never reference the gate. If a module only
builds blocks from in-process Python values and matches by accident, add it to
``_ALLOWLIST`` with a reason.
"""

import re
import sys
from pathlib import Path

import pytest

import ray.data._internal.datasource as _datasource_pkg
import ray.data._internal.datasource_v2 as _datasource_v2_pkg

_GATE = "raise_on_pickle_object_columns"

# Calls that materialize a ``pa.Table`` / ``RecordBatch`` from bytes read outside
# this process. Extend this list when a new reader library shows up.
_ARROW_READ_PATTERNS = [
    r"pq\.read_table\(",
    r"ParquetFile\(",
    r"\.read_all\(",
    r"ipc\.open_stream\(",
    r"ipc\.open_file\(",
    r"\.to_arrow\(",
    r"\.to_table\(",
    r"\.to_record_batches\(",
    r"\.to_reader\(",
    r"\.read_stripe\(",
    r"query_arrow_stream\(",
    r"read_file_slice",
    r'with_format\("arrow"\)',
]
_ARROW_READ_RE = re.compile("|".join(_ARROW_READ_PATTERNS))

# Module file name -> why an Arrow-read match there does not need the gate.
_ALLOWLIST = {}


def _datasource_modules():
    roots = [
        Path(_datasource_pkg.__file__).parent,
        Path(_datasource_v2_pkg.__file__).parent,
    ]
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            if "tests" in path.parts or path.name == "__init__.py":
                continue
            if path.name.endswith("_datasink.py"):
                continue
            yield path


@pytest.mark.parametrize("path", list(_datasource_modules()), ids=lambda p: p.name)
def test_arrow_readers_call_pickle_object_gate(path: Path):
    source = path.read_text()
    reads = sorted(
        {
            m.group(0)
            for line in source.splitlines()
            # ``BlockAccessor.for_block(block).to_arrow()`` converts an in-memory
            # block; it is not an external read.
            if "BlockAccessor" not in line
            for m in _ARROW_READ_RE.finditer(line)
        }
    )
    if not reads or path.name in _ALLOWLIST:
        return
    assert _GATE in source, (
        f"{path.name} reads Arrow data from an external source ({reads}) but never "
        f"calls {_GATE}(). Unpickling untrusted 'ray.data.arrow_pickled_object' "
        f"columns executes arbitrary code. Call the gate right after the read and "
        f"before yielding or materializing the table, and add a reject test. If this "
        f"module only builds blocks from in-process Python values, add it to "
        f"_ALLOWLIST."
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
