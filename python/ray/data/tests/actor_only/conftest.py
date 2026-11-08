"""Shared fixtures for the actor-only-backend test suite."""
import os
from typing import Dict, Iterator, Optional

import pytest

# Env vars that must be set before any Ray cluster starts so raylet/workers
# inherit them. A function-scoped ``monkeypatch.setenv`` only reaches the
# driver, and shared Ray fixtures are module-scoped -- they start the cluster
# before any function-scoped fixture runs -- so a driver-only setting never
# reaches worker processes. That matters for execution that runs off-driver:
# e.g. ``streaming_split`` runs the executor inside a remote
# ``SplitCoordinator`` actor, which would otherwise fall back to the default
# (non-actor-only) backend and silently skip what these tests mean to exercise.
#
# Session scope runs before module-scoped ``ray_start_*`` fixtures, and we
# restore on teardown so later suites in the same process are not polluted.
_ACTOR_ONLY_ENV = {
    "RAY_DATA_ACTOR_ONLY_BACKEND": "1",
}


@pytest.fixture(scope="session", autouse=True)
def actor_only_session_env() -> Iterator[None]:
    """Enable the actor-only backend for this directory's test session."""
    previous: Dict[str, Optional[str]] = {
        key: os.environ.get(key) for key in _ACTOR_ONLY_ENV
    }
    os.environ.update(_ACTOR_ONLY_ENV)
    try:
        yield
    finally:
        for key, old in previous.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old
