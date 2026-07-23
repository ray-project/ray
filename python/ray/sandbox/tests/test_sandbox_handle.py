from unittest.mock import MagicMock

from ray.sandbox.backend.base import ExecResult
from ray.sandbox.config import SandboxConfig
from ray.sandbox.sandbox import Sandbox


def test_sandbox_context_manager_and_exec():
    mock_backend = MagicMock()
    mock_backend.exec_command.return_value = ExecResult(
        exit_code=0, stdout="Hello world\n", stderr="", duration_seconds=0.1
    )
    config = SandboxConfig()

    with Sandbox("sb-123", mock_backend, config) as sb:
        res = sb.exec("echo 'Hello world'")
        assert res.exit_code == 0
        assert res.stdout == "Hello world\n"
        mock_backend.exec_command.assert_called_once_with(
            "sb-123", "echo 'Hello world'", timeout=None, cwd=None, env=None
        )

    # Deletion should be called on context exit
    mock_backend.delete_sandbox.assert_called_once_with("sb-123")


def test_sandbox_file_operations():
    mock_backend = MagicMock()
    mock_backend.read_file.return_value = b"test content"
    config = SandboxConfig()

    sb = Sandbox("sb-456", mock_backend, config)
    sb.write_file("/workspace/test.txt", "test content")
    mock_backend.write_file.assert_called_once_with(
        "sb-456", "/workspace/test.txt", "test content"
    )

    data = sb.read_file("/workspace/test.txt")
    assert data == b"test content"
    mock_backend.read_file.assert_called_once_with("sb-456", "/workspace/test.txt")
