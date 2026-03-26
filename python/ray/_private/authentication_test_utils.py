import os
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from ray._raylet import AuthenticationTokenLoader, Config

_AUTH_ENV_VARS = ("RAY_AUTH_MODE", "RAY_AUTH_TOKEN", "RAY_AUTH_TOKEN_PATH")
_DEFAULT_AUTH_TOKEN_RELATIVE_PATH = Path(".ray") / "auth_token"


def reset_auth_token_state() -> None:
    """Reset authentication token and AUTH_MODE ray config."""

    AuthenticationTokenLoader.instance().reset_cache()
    Config.initialize("")


def set_auth_mode(mode: str) -> None:
    """Set the authentication mode environment variable."""

    os.environ["RAY_AUTH_MODE"] = mode


def set_env_auth_token(token: str) -> None:
    """Configure the authentication token via environment variable."""

    os.environ["RAY_AUTH_TOKEN"] = token
    os.environ.pop("RAY_AUTH_TOKEN_PATH", None)


def set_auth_token_path(token: str, path: Path) -> None:
    """Write the authentication token to a specific path and point the loader to it."""

    token_path = Path(path)
    if token is not None:
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(token)
    os.environ["RAY_AUTH_TOKEN_PATH"] = str(token_path)
    os.environ.pop("RAY_AUTH_TOKEN", None)


def set_default_auth_token(token: str) -> Path:
    """Write the authentication token to the default ~/.ray/auth_token location."""

    default_path = Path.home() / _DEFAULT_AUTH_TOKEN_RELATIVE_PATH
    default_path.parent.mkdir(parents=True, exist_ok=True)
    default_path.write_text(token)
    return default_path


def clear_auth_token_sources(remove_default: bool = False) -> None:
    """Clear authentication-related environment variables and optional default token file."""

    for var in ("RAY_AUTH_TOKEN", "RAY_AUTH_TOKEN_PATH"):
        os.environ.pop(var, None)

    if remove_default:
        default_path = Path.home() / _DEFAULT_AUTH_TOKEN_RELATIVE_PATH
        default_path.unlink(missing_ok=True)


@dataclass
class AuthenticationEnvSnapshot:
    original_env: Dict[str, Optional[str]]
    original_home: Optional[str]
    home_was_set: bool
    temp_home: Optional[Path]
    default_token_path: Path
    default_token_exists: bool
    default_token_contents: Optional[str]

    @classmethod
    def capture(cls) -> "AuthenticationEnvSnapshot":
        """Capture current authentication-related environment state."""

        original_env = {var: os.environ.get(var) for var in _AUTH_ENV_VARS}
        home_was_set = "HOME" in os.environ
        original_home = os.environ.get("HOME")
        temp_home: Optional[Path] = None

        if not home_was_set:
            # in CI $HOME may not be set which can cause issues with tests related to default auth token file.
            test_tmpdir = os.environ.get("TEST_TMPDIR")
            base_dir = Path(test_tmpdir) if test_tmpdir else Path(tempfile.gettempdir())
            temp_home = base_dir / "ray_test_home"
            temp_home.mkdir(parents=True, exist_ok=True)
            os.environ["HOME"] = str(temp_home)

        default_token_path = Path.home() / _DEFAULT_AUTH_TOKEN_RELATIVE_PATH
        default_token_exists = default_token_path.exists()
        default_token_contents = (
            default_token_path.read_text() if default_token_exists else None
        )

        return cls(
            original_env=original_env,
            original_home=original_home,
            home_was_set=home_was_set,
            temp_home=temp_home,
            default_token_path=default_token_path,
            default_token_exists=default_token_exists,
            default_token_contents=default_token_contents,
        )

    def clear_default_token(self) -> None:
        """Remove the default token file for the current HOME."""

        self.default_token_path.unlink(missing_ok=True)

    def restore(self) -> None:
        """Restore the captured environment, HOME, and default token file state."""
        # delete any custom token files that may have been created during the test
        custom_token_path = os.environ.get("RAY_AUTH_TOKEN_PATH")
        if custom_token_path is not None:
            custom_token_path = Path(custom_token_path)
            if custom_token_path.exists():
                custom_token_path.unlink(missing_ok=True)

        for var, value in self.original_env.items():
            if value is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = value

        if self.home_was_set:
            if self.original_home is None:
                os.environ.pop("HOME", None)
            else:
                os.environ["HOME"] = self.original_home

        if self.default_token_exists:
            self.default_token_path.parent.mkdir(parents=True, exist_ok=True)
            self.default_token_path.write_text(self.default_token_contents or "")
        else:
            self.default_token_path.unlink(missing_ok=True)

        if not self.home_was_set:
            current_home = os.environ.get("HOME")
            if self.temp_home is not None and current_home == str(self.temp_home):
                os.environ.pop("HOME", None)
            if self.temp_home is not None and self.temp_home.exists():
                shutil.rmtree(self.temp_home, ignore_errors=True)


@contextmanager
def authentication_env_guard():
    """Context manager that restores authentication environment state on exit."""

    snapshot = AuthenticationEnvSnapshot.capture()
    try:
        yield snapshot
    finally:
        snapshot.restore()
