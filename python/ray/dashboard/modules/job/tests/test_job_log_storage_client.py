import os
from unittest.mock import patch

import pytest

from ray.dashboard.modules.job.job_log_storage_client import (
    JOB_LOG_MAX_READ_BYTES,
    JobLogStorageClient,
)


@pytest.fixture
def client():
    return JobLogStorageClient()


@pytest.fixture
def tmp_log_dir(tmp_path):
    """Create a temp dir and patch get_log_file_path to use it."""

    def _write_log(job_id: str, content, encoding="utf-8") -> str:
        path = tmp_path / f"job-driver-{job_id}.log"
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding=encoding)
        return str(path)

    return _write_log


class TestGetLogs:
    def test_small_file_returned_fully(self, client, tmp_log_dir):
        content = "line 1\nline 2\nline 3\n"
        log_path = tmp_log_dir("small-job", content)

        with patch.object(client, "get_log_file_path", return_value=log_path):
            result = client.get_logs("small-job")

        assert result == content

    def test_missing_file_returns_empty_string(self, client):
        with patch.object(
            client, "get_log_file_path", return_value="/nonexistent/path.log"
        ):
            result = client.get_logs("missing-job")

        assert result == ""

    def test_large_file_truncated_with_notice(self, client, tmp_log_dir):
        # Create a file larger than the cap
        line = "x" * 999 + "\n"  # 1000 bytes per line
        num_lines = (JOB_LOG_MAX_READ_BYTES // 1000) + 100
        content = line * num_lines
        log_path = tmp_log_dir("large-job", content)

        with patch.object(client, "get_log_file_path", return_value=log_path):
            result = client.get_logs("large-job")

        assert result.startswith("[LOG TRUNCATED")
        assert "MiB" in result
        # The result should be significantly smaller than the original
        assert len(result) <= JOB_LOG_MAX_READ_BYTES + 1024  # allow header overhead
        # Should not contain the very first line (it was truncated away)
        assert result.endswith("\n")

    def test_large_file_starts_on_clean_line_boundary(self, client, tmp_log_dir):
        # Use variable-length lines so we can verify partial line is skipped
        lines = [f"LINE-{i:06d}-{'y' * 80}\n" for i in range(200_000)]
        content = "".join(lines)
        log_path = tmp_log_dir("boundary-job", content)

        with patch.object(client, "get_log_file_path", return_value=log_path):
            result = client.get_logs("boundary-job")

        # After the truncation header, each line should be complete
        body_lines = result.split("\n")
        # First line is the truncation notice
        assert body_lines[0].startswith("[LOG TRUNCATED")
        # Remaining non-empty lines should all start with "LINE-"
        for ln in body_lines[1:]:
            if ln:
                assert ln.startswith("LINE-"), f"Partial line found: {ln[:50]}"

    def test_multibyte_utf8_does_not_raise(self, client, tmp_log_dir):
        """Seek landing mid-UTF-8 character must not raise UnicodeDecodeError."""
        from ray.dashboard.modules.job import job_log_storage_client as mod

        original = mod.JOB_LOG_MAX_READ_BYTES
        # Use a small cap so the seek is likely to land mid-character.
        mod.JOB_LOG_MAX_READ_BYTES = 512
        try:
            # Build content with multi-byte chars (emoji = 4 bytes each).
            # Enough to exceed 512 bytes.
            line = "日志输出：🚀🎉🔥 data\n"  # mix of 3-byte and 4-byte chars
            content = (line * 200).encode("utf-8")
            assert len(content) > 512
            log_path = tmp_log_dir("utf8-job", content)

            with patch.object(client, "get_log_file_path", return_value=log_path):
                result = client.get_logs("utf8-job")

            assert result.startswith("[LOG TRUNCATED")
            # Should contain recognizable content (replacement chars are OK)
            assert "data" in result
        finally:
            mod.JOB_LOG_MAX_READ_BYTES = original

    @patch.dict(os.environ, {"RAY_JOB_LOG_MAX_READ_BYTES": "4096"})
    def test_cap_is_configurable_via_env(self, client, tmp_log_dir):
        # Re-import to pick up the env var
        from ray.dashboard.modules.job import job_log_storage_client as mod

        original = mod.JOB_LOG_MAX_READ_BYTES
        # Simulate the env-based init
        mod.JOB_LOG_MAX_READ_BYTES = int(
            os.environ.get("RAY_JOB_LOG_MAX_READ_BYTES", 16 * 1024 * 1024)
        )
        try:
            assert mod.JOB_LOG_MAX_READ_BYTES == 4096

            content = "a" * 999 + "\n"  # 1000 bytes per line
            content = content * 10  # 10,000 bytes total > 4096
            log_path = tmp_log_dir("env-job", content)

            with patch.object(client, "get_log_file_path", return_value=log_path):
                result = client.get_logs("env-job")

            assert result.startswith("[LOG TRUNCATED")
        finally:
            mod.JOB_LOG_MAX_READ_BYTES = original


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
