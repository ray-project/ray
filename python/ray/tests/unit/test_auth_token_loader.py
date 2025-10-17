"""Unit tests for ray._private.auth_token_loader module."""

import os
import tempfile
import threading
from pathlib import Path

import pytest

from ray._private import auth_token_loader


@pytest.fixture(autouse=True)
def reset_cached_token():
    """Reset the cached token before each test."""
    auth_token_loader._cached_token = None
    yield
    auth_token_loader._cached_token = None


@pytest.fixture(autouse=True)
def clean_env_vars():
    """Clean up environment variables before and after each test."""
    env_vars = ["RAY_AUTH_TOKEN", "RAY_AUTH_TOKEN_PATH"]
    old_values = {var: os.environ.get(var) for var in env_vars}

    # Clear environment variables
    for var in env_vars:
        if var in os.environ:
            del os.environ[var]

    yield

    # Restore old values
    for var, value in old_values.items():
        if value is not None:
            os.environ[var] = value
        elif var in os.environ:
            del os.environ[var]


@pytest.fixture
def temp_token_file():
    """Create a temporary token file and clean it up after the test."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".token") as f:
        temp_path = f.name
        f.write("test-token-from-file")
    yield temp_path
    try:
        os.unlink(temp_path)
    except FileNotFoundError:
        pass


@pytest.fixture
def default_token_path():
    """Return the default token path and clean it up after the test."""
    path = Path.home() / ".ray" / "auth_token"
    yield path
    try:
        path.unlink()
    except FileNotFoundError:
        pass


class TestLoadAuthToken:
    """Tests for load_auth_token function."""

    def test_load_from_env_variable(self):
        """Test loading token from RAY_AUTH_TOKEN environment variable."""
        os.environ["RAY_AUTH_TOKEN"] = "token-from-env"
        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == "token-from-env"

    def test_load_from_env_path(self, temp_token_file):
        """Test loading token from RAY_AUTH_TOKEN_PATH environment variable."""
        os.environ["RAY_AUTH_TOKEN_PATH"] = temp_token_file
        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == "test-token-from-file"

    def test_load_from_default_path(self, default_token_path):
        """Test loading token from default ~/.ray/auth_token path."""
        default_token_path.parent.mkdir(parents=True, exist_ok=True)
        default_token_path.write_text("token-from-default")

        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == "token-from-default"

    def test_precedence_order(self, temp_token_file, default_token_path):
        """Test that token loading follows correct precedence order."""
        # Set all three sources
        os.environ["RAY_AUTH_TOKEN"] = "token-from-env"
        os.environ["RAY_AUTH_TOKEN_PATH"] = temp_token_file

        default_token_path.parent.mkdir(parents=True, exist_ok=True)
        default_token_path.write_text("token-from-default")

        # Environment variable should have highest precedence
        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == "token-from-env"

    def test_env_path_over_default(self, temp_token_file, default_token_path):
        """Test that RAY_AUTH_TOKEN_PATH has precedence over default path."""
        os.environ["RAY_AUTH_TOKEN_PATH"] = temp_token_file

        default_token_path.parent.mkdir(parents=True, exist_ok=True)
        default_token_path.write_text("token-from-default")

        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == "test-token-from-file"

    def test_no_token_found(self):
        """Test behavior when no token is found."""
        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == ""

    def test_whitespace_handling(self, temp_token_file):
        """Test that whitespace is properly trimmed from token files."""
        # Overwrite the temp file with whitespace
        with open(temp_token_file, "w") as f:
            f.write("  token-with-spaces  \n\t")

        os.environ["RAY_AUTH_TOKEN_PATH"] = temp_token_file
        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == "token-with-spaces"

    def test_empty_env_variable(self):
        """Test that empty environment variable is ignored."""
        os.environ["RAY_AUTH_TOKEN"] = "   "  # Empty after strip
        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == ""

    def test_nonexistent_path_in_env(self):
        """Test that nonexistent path in RAY_AUTH_TOKEN_PATH is handled gracefully."""
        os.environ["RAY_AUTH_TOKEN_PATH"] = "/nonexistent/path/to/token"
        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == ""


class TestTokenGeneration:
    """Tests for token generation functionality."""

    def test_generate_token(self, default_token_path):
        """Test token generation when no token exists."""
        token = auth_token_loader.load_auth_token(generate_if_not_found=True)

        # Token should be a 32-character hex string (UUID without dashes)
        assert len(token) == 32
        assert all(c in "0123456789abcdef" for c in token)

        # Token should be saved to default path
        assert default_token_path.exists()
        saved_token = default_token_path.read_text().strip()
        assert saved_token == token

    def test_no_generation_without_flag(self):
        """Test that token is not generated when flag is False."""
        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == ""

    def test_dont_generate_when_token_exists(self):
        """Test that token is not generated when one already exists."""
        os.environ["RAY_AUTH_TOKEN"] = "existing-token"
        token = auth_token_loader.load_auth_token(generate_if_not_found=True)
        assert token == "existing-token"


class TestTokenCaching:
    """Tests for token caching behavior."""

    def test_caching_behavior(self):
        """Test that token is cached after first load."""
        os.environ["RAY_AUTH_TOKEN"] = "cached-token"
        token1 = auth_token_loader.load_auth_token(generate_if_not_found=False)

        # Change environment variable (shouldn't affect cached value)
        os.environ["RAY_AUTH_TOKEN"] = "new-token"
        token2 = auth_token_loader.load_auth_token(generate_if_not_found=False)

        # Should still return the cached token
        assert token1 == token2 == "cached-token"

    def test_cache_empty_result(self):
        """Test that even empty results are cached."""
        # First call with no token
        token1 = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token1 == ""

        # Set environment variable after first call
        os.environ["RAY_AUTH_TOKEN"] = "new-token"
        token2 = auth_token_loader.load_auth_token(generate_if_not_found=False)

        # Should still return cached empty string
        assert token2 == ""


class TestHasAuthToken:
    """Tests for has_auth_token function."""

    def test_has_token_true(self):
        """Test has_auth_token returns True when token exists."""
        os.environ["RAY_AUTH_TOKEN"] = "test-token"
        assert auth_token_loader.has_auth_token() is True

    def test_has_token_false(self):
        """Test has_auth_token returns False when no token exists."""
        assert auth_token_loader.has_auth_token() is False

    def test_has_token_caches_result(self):
        """Test that has_auth_token doesn't trigger generation."""
        # This should return False without generating a token
        assert auth_token_loader.has_auth_token() is False

        # Verify no token was generated
        default_path = Path.home() / ".ray" / "auth_token"
        assert not default_path.exists()


class TestThreadSafety:
    """Tests for thread safety of token loading."""

    def test_concurrent_loads(self):
        """Test that concurrent token loads are thread-safe."""
        os.environ["RAY_AUTH_TOKEN"] = "thread-safe-token"

        results = []
        threads = []

        def load_token():
            token = auth_token_loader.load_auth_token(generate_if_not_found=False)
            results.append(token)

        # Create multiple threads that try to load token simultaneously
        for _ in range(10):
            thread = threading.Thread(target=load_token)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # All threads should get the same token
        assert len(results) == 10
        assert all(result == "thread-safe-token" for result in results)

    def test_concurrent_generation(self, default_token_path):
        """Test that concurrent token generation is thread-safe."""
        results = []
        threads = []

        def generate_token():
            token = auth_token_loader.load_auth_token(generate_if_not_found=True)
            results.append(token)

        # Create multiple threads that try to generate token simultaneously
        for _ in range(5):
            thread = threading.Thread(target=generate_token)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # All threads should get the same token (only generated once)
        assert len(results) == 5
        assert len(set(results)) == 1  # All tokens should be identical


class TestFilePermissions:
    """Tests for file permissions when saving tokens."""

    def test_file_permissions_on_unix(self, default_token_path, monkeypatch):
        """Test that token file has 0600 permissions on Unix systems."""
        # Skip on Windows
        if os.name == "nt":
            pytest.skip("Test only relevant on Unix systems")

        token = auth_token_loader.load_auth_token(generate_if_not_found=True)
        assert token

        # Check file permissions (should be 0600)
        stat_info = default_token_path.stat()
        assert stat_info.st_mode & 0o777 == 0o600

    def test_file_permissions_error_handling(self, monkeypatch):
        """Test that permission errors are handled gracefully."""

        # Mock os.chmod to raise an exception
        def mock_chmod(path, mode):
            raise OSError("Permission denied")

        monkeypatch.setattr(os, "chmod", mock_chmod)

        # Should still generate and return token, just not set permissions
        token = auth_token_loader.load_auth_token(generate_if_not_found=True)
        assert len(token) == 32


class TestIntegration:
    """Integration tests with ray.init() and ray start CLI."""

    def test_token_loader_with_ray_init(self, default_token_path):
        """Test that token loader works with ray.init() enable_token_auth parameter."""
        # This is more of a smoke test to ensure the module can be imported
        # and used in the context where it will be called
        from ray._private import auth_token_loader

        # Generate a token
        token = auth_token_loader.load_auth_token(generate_if_not_found=True)
        assert token
        assert len(token) == 32

        # Verify it was saved
        assert default_token_path.exists()
        saved_token = default_token_path.read_text().strip()
        assert saved_token == token
