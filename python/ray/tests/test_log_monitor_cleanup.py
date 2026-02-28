import os
from unittest.mock import MagicMock

from ray._private.log_monitor import LogMonitor


def test_log_monitor_cleanup_deleted_files(tmp_path):
    """
    Test that files are stopped being tracked if they are deleted or rotated out.
    """
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "old").mkdir()

    # Create dummy files
    f1 = log_dir / "worker-100-200.out"
    f2 = log_dir / "worker-100-201.out"
    f1.write_text("log line 1\n", encoding="utf-8")
    f2.write_text("log line 1\n", encoding="utf-8")

    mock_publisher = MagicMock()

    # Mock proc_alive: pid 201 is alive (f2), others dead
    def mock_is_proc_alive(pid):
        return pid == 201

    log_monitor = LogMonitor(
        "127.0.0.1", str(log_dir), mock_publisher, mock_is_proc_alive, max_files_open=10
    )

    # 1. Start tracking files
    log_monitor.update_log_filenames()
    assert len(log_monitor.log_filenames) == 2

    # Files are initially closed
    assert len(log_monitor.closed_file_infos) == 2
    assert len(log_monitor.open_file_infos) == 0

    # 2. Open files
    log_monitor.open_closed_files()
    assert len(log_monitor.open_file_infos) == 2
    assert len(log_monitor.closed_file_infos) == 0

    # 3. Delete one file
    os.remove(f1)

    # 4. Cycle through updates.
    # Note: If files are OPEN, they won't be removed from tracking immediately.
    log_monitor.update_log_filenames()
    # Still tracked because it's open
    assert len(log_monitor.log_filenames) == 2

    # 5. Close files (simulate low FD condition or rotation)
    # This will check process liveness (False), so it moves files to 'old/'
    log_monitor._close_all_files()

    # NOW f1 should be untracked because:
    # - It's removed from open_file_infos by _close_all_files
    # - It's not in closed_file_infos
    # - It's not on disk (monitor_log_paths wont find it)

    log_monitor.update_log_filenames()

    # Should only track f2 now
    assert len(log_monitor.log_filenames) == 1
    assert str(f2) in log_monitor.log_filenames
    assert str(f1) not in log_monitor.log_filenames

    # 6. Verify closed_file_infos cleanup
    # Let's create a file, track it, then delete it without opening it
    f3 = log_dir / "worker-100-202.out"
    f3.write_text("log line\n", encoding="utf-8")

    log_monitor.update_log_filenames()
    assert len(log_monitor.log_filenames) == 2  # f2 and f3

    # f3 is in closed_file_infos

    os.remove(f3)
    log_monitor.update_log_filenames()

    assert len(log_monitor.log_filenames) == 1  # Only f2 left
    assert len(log_monitor.closed_file_infos) == 1
    assert log_monitor.closed_file_infos[0].filename == str(f2)


if __name__ == "__main__":
    import shutil
    import tempfile
    from pathlib import Path

    # Simple runner implementation to verify without pytest
    tmp_dir = tempfile.mkdtemp()
    try:
        test_log_monitor_cleanup_deleted_files(Path(tmp_dir))
        print("Test passed!")
    finally:
        shutil.rmtree(tmp_dir)
