import logging
import os
import sys

import pytest

from ray._private.node import Node
from ray._private.parameter import RayParams


def _init_node_temp(temp_dir, logs_dir=None):
    node = Node.__new__(Node)
    node._session_name = "session"
    node._ray_params = RayParams(
        temp_dir=str(temp_dir),
        logs_dir=str(logs_dir) if logs_dir is not None else None,
    )
    node._init_temp(None)
    return node


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
def test_default_logs_dir_preserves_symlink_parent_traversal(tmp_path):
    physical_dir = tmp_path / "physical"
    (physical_dir / "nested").mkdir(parents=True)
    alias = tmp_path / "alias"
    alias.symlink_to(physical_dir / "nested", target_is_directory=True)
    temp_dir = os.path.join(str(alias), "..", "ray")

    node = _init_node_temp(temp_dir)

    assert os.path.samefile(
        node.get_logs_dir_path(), physical_dir / "ray" / "session" / "logs"
    )
    assert os.path.isdir(os.path.join(node.get_session_dir_path(), "logs"))
    assert not (tmp_path / "ray").exists()


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
def test_custom_logs_dir_preserves_symlink_parent_traversal(tmp_path):
    physical_dir = tmp_path / "physical"
    (physical_dir / "nested").mkdir(parents=True)
    alias = tmp_path / "alias"
    alias.symlink_to(physical_dir / "nested", target_is_directory=True)
    logs_dir = os.path.join(str(alias), "..", "logs")

    node = _init_node_temp(tmp_path / "ray", logs_dir)

    assert node.get_logs_dir_path() == str((physical_dir / "logs").resolve())
    assert os.path.samefile(logs_dir, node.get_logs_dir_path())
    assert not (tmp_path / "logs").exists()


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
def test_custom_logs_dir_resolves_symlink_chain(tmp_path):
    temp_dir = tmp_path / "ray"
    temp_dir.mkdir()
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    intermediate = tmp_path / "intermediate"
    intermediate.symlink_to(logs_dir, target_is_directory=True)
    alias = temp_dir / "logs-alias"
    alias.symlink_to("../intermediate", target_is_directory=True)

    node = _init_node_temp(temp_dir, alias)

    assert node.get_logs_dir_path() == str(logs_dir.resolve())
    assert os.path.samefile(os.path.join(node.get_session_dir_path(), "logs"), logs_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
def test_custom_logs_dir_matching_default_path(tmp_path, caplog, propagate_logs):
    physical_dir = tmp_path / "physical"
    physical_dir.mkdir()
    temp_dir = tmp_path / "ray"
    temp_dir.symlink_to(physical_dir, target_is_directory=True)
    logs_dir = physical_dir / "session" / "logs"

    with caplog.at_level(logging.WARNING):
        node = _init_node_temp(temp_dir, logs_dir)

    default_logs_dir = os.path.join(node.get_session_dir_path(), "logs")
    assert os.path.samefile(default_logs_dir, logs_dir)
    assert not os.path.islink(default_logs_dir)
    assert "Failed to create" not in caplog.text


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


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
@pytest.mark.parametrize("dangling", [False, True])
def test_link_default_logs_dir_replaces_old_target(tmp_path, dangling):
    temp_dir = tmp_path / "ray"
    session_dir = temp_dir / "session"
    session_dir.mkdir(parents=True)
    old_logs_dir = tmp_path / "old_logs"
    if not dangling:
        old_logs_dir.mkdir()
    default_logs_dir = session_dir / "logs"
    default_logs_dir.symlink_to(old_logs_dir, target_is_directory=True)
    logs_dir = tmp_path / "new_logs"

    _init_node_temp(temp_dir, logs_dir)

    assert os.path.samefile(default_logs_dir, logs_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
def test_link_default_logs_dir_warns_on_wrong_target(
    tmp_path, monkeypatch, caplog, propagate_logs
):
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    old_logs_dir = tmp_path / "old_logs"
    old_logs_dir.mkdir()
    default_logs_dir = session_dir / "logs"
    default_logs_dir.symlink_to(old_logs_dir, target_is_directory=True)
    logs_dir = tmp_path / "new_logs"
    logs_dir.mkdir()
    node = Node.__new__(Node)
    node._session_dir = str(session_dir)
    node._logs_dir = str(logs_dir)

    def fail_to_remove(path):
        raise PermissionError("cannot replace compatibility symlink")

    monkeypatch.setattr("ray._private.utils.os.remove", fail_to_remove)
    with caplog.at_level(logging.WARNING):
        node._link_default_logs_dir(str(default_logs_dir))

    assert os.path.samefile(default_logs_dir, old_logs_dir)
    assert "Failed to create" in caplog.text


def test_link_default_logs_dir_preserves_existing_directory(
    tmp_path, caplog, propagate_logs
):
    temp_dir = tmp_path / "ray"
    default_logs_dir = temp_dir / "session" / "logs"
    default_logs_dir.mkdir(parents=True)
    old_log = default_logs_dir / "previous.log"
    old_log.write_text("existing logs")

    with caplog.at_level(logging.WARNING):
        _init_node_temp(temp_dir, tmp_path / "new_logs")

    assert not default_logs_dir.is_symlink()
    assert old_log.read_text() == "existing logs"
    assert "Failed to create" in caplog.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
