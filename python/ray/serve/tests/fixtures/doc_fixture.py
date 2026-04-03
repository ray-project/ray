"""Adds doc/source/serve/doc_code to sys.path at import time.

Import this module in test files that need to use doc examples such as
``capacity_queue_request_router``.  The path setup happens at import time
so that module-level imports work under both pytest and Bazel (which runs
the test file as a script before pytest is invoked).

Usage in a test file::

    import ray.serve.tests.fixtures.doc_fixture  # noqa: F401
    from capacity_queue_request_router import CapacityQueue
"""

import os
import sys

# The __file__-relative path covers pytest; the cwd-relative path covers
# Bazel runfiles.
_DOC_CODE_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "..",
        "doc",
        "source",
        "serve",
        "doc_code",
    )
)
_DOC_CODE_CWD = os.path.join(os.getcwd(), "doc", "source", "serve", "doc_code")

if _DOC_CODE_DIR not in sys.path:
    sys.path.insert(0, _DOC_CODE_DIR)
if _DOC_CODE_CWD not in sys.path:
    sys.path.insert(0, _DOC_CODE_CWD)
