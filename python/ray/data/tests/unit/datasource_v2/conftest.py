"""Fixtures shared by the MCAP datasource unit tests."""

import os

import pytest

from ray.data.tests.unit.datasource_v2.mcap_testing import (
    CHUNKED_FILE_MESSAGES,
    round_robin_messages,
    write_mcap,
)


@pytest.fixture
def chunked_file(tmp_path):
    """Nine JSON messages on ``/a``, ``/b`` and ``/c`` in turn, one per chunk.

        chunk / seq  0   1   2   3   4   5   6   7   8
        topic        /a  /b  /c  /a  /b  /c  /a  /b  /c

    Message ``seq`` is ``{"seq": seq}``, logged at ``BASE_TIME + seq * STEP``.
    """
    path = os.path.join(tmp_path, "chunked.mcap")
    write_mcap(path, round_robin_messages(CHUNKED_FILE_MESSAGES))
    return path
