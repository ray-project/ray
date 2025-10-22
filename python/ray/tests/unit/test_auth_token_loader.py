"""Unit tests for ray._private.auth_token_loader module."""

import os
import sys
import tempfile
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

    @pytest.mark.parametrize(
        "set_env,set_file,set_default,expected_token",
        [
            # All set: env should win
            (True, True, True, "token-from-env"),
            # File and default file set: file should win
            (False, True, True, "test-token-from-file"),
            # Only default file set
            (False, False, True, "token-from-default"),
        ],
    )
    def test_token_precedence_parametrized(
        self,
        temp_token_file,
        default_token_path,
        set_env,
        set_file,
        set_default,
        expected_token,
    ):
        """Parametrized test for token loading precedence: env var > user-specified file > default file."""
        # Optionally set environment variable
        if set_env:
            os.environ["RAY_AUTH_TOKEN"] = "token-from-env"
        else:
            os.environ.pop("RAY_AUTH_TOKEN", None)

        # Optionally create file and set path
        if set_file:
            os.environ["RAY_AUTH_TOKEN_PATH"] = temp_token_file
        else:
            os.environ.pop("RAY_AUTH_TOKEN_PATH", None)

        # Optionally create default file
        if set_default:
            default_token_path.parent.mkdir(parents=True, exist_ok=True)
            default_token_path.write_text("token-from-default")
        else:
            if default_token_path.exists():
                default_token_path.unlink()

        # Load token and verify expected precedence
        token = auth_token_loader.load_auth_token(generate_if_not_found=False)
        assert token == expected_token

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
        with pytest.raises(FileNotFoundError):
            auth_token_loader.load_auth_token(generate_if_not_found=False)


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

    def test_dont_generate_when_token_exists(self, default_token_path):
        """Test that token is not generated when one already exists."""
        os.environ["RAY_AUTH_TOKEN"] = "existing-token"
        token = auth_token_loader.load_auth_token(generate_if_not_found=True)
        assert token == "existing-token"
        generated_token = auth_token_loader.generate_and_save_token()
        assert generated_token == "existing-token"  # does not generate a new token
        assert not default_token_path.exists()

    def test_public_generate_and_save_token(self, default_token_path):
        """Test the public generate_and_save_token function."""
        token = auth_token_loader.generate_and_save_token()

        # Token should be a 32-character hex string (UUID without dashes)
        assert len(token) == 32
        assert all(c in "0123456789abcdef" for c in token)

        # Token should be saved to default path
        assert default_token_path.exists()
        saved_token = default_token_path.read_text().strip()
        assert saved_token == token


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
