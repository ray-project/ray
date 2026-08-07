import os
import sys

import pytest

from ray._private.node import Node


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
def test_link_default_logs_dir_removes_stale_symlink(tmp_path):
    logs_dir = tmp_path
    session_dir = logs_dir / "session"
    session_dir.mkdir()
    stale_logs_dir = logs_dir / "stale_logs"
    stale_logs_dir.mkdir()
    default_logs_dir = session_dir / "logs"
    default_logs_dir.symlink_to(stale_logs_dir, target_is_directory=True)

    node = Node.__new__(Node)
    node._session_dir = str(session_dir)
    node._logs_dir = str(logs_dir)
    node._link_default_logs_dir(str(default_logs_dir))

    assert not os.path.lexists(default_logs_dir)
