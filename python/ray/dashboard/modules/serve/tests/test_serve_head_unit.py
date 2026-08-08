import asyncio
import sys
from unittest.mock import Mock, patch

import pytest

import ray
from ray.dashboard.modules.serve.serve_head import ServeHead
from ray.exceptions import (
    ActorUnschedulableError,
    RayActorError,
    RuntimeEnvSetupError,
)


def _controller_whose_check_alive_raises(exc):
    """Stand-in for a cached controller handle whose check_alive fails."""
    controller = Mock()

    def _remote(*args, **kwargs):
        async def _raise():
            raise exc

        return _raise()

    controller.check_alive.remote.side_effect = _remote
    return controller


@pytest.mark.parametrize(
    "exc",
    [
        # Not a RayActorError: the controller is dead AND cannot be recreated
        # because its runtime_env working_dir package was GC'd from the GCS.
        RuntimeEnvSetupError("working_dir package missing from GCS"),
        # Not a RayActorError: the controller became unschedulable.
        ActorUnschedulableError("hard-pinned to a removed node"),
        # RayActorError path must keep working (no regression).
        RayActorError(),
    ],
)
def test_get_serve_controller_reresolves_when_cached_handle_unusable(exc):
    """A cached controller handle whose check_alive fails must be dropped and
    the controller re-resolved by name -- including non-RayActorError failures.

    Regression test for the Serve REST API returning 500 forever: when the
    cached handle raised e.g. RuntimeEnvSetupError, the exception used to
    propagate (only RayActorError was caught), so the re-resolution below --
    which would have found a healthy, freshly-started controller -- never ran.
    """

    async def _run():
        stale = _controller_whose_check_alive_raises(exc)
        fresh = Mock(name="fresh_controller")
        head = ServeHead.__new__(ServeHead)  # bypass SubprocessModule.__init__
        head._controller = stale
        head._controller_lock = asyncio.Lock()
        with patch.object(ray, "get_actor", return_value=fresh) as mock_get_actor:
            result = await head.get_serve_controller()
        return result, head._controller, fresh, mock_get_actor

    result, cached_after, fresh, mock_get_actor = asyncio.run(_run())

    assert result is fresh, "should return the re-resolved controller"
    assert cached_after is fresh, "stale handle should be replaced in the cache"
    mock_get_actor.assert_called_once()


def test_get_serve_controller_propagates_non_ray_errors():
    """A non-RayError from check_alive (i.e. an unexpected bug, not a remote
    actor/runtime-env failure) must NOT be swallowed: it should propagate
    instead of silently re-resolving, so real bugs stay visible."""

    async def _run():
        stale = _controller_whose_check_alive_raises(ValueError("unexpected bug"))
        head = ServeHead.__new__(ServeHead)
        head._controller = stale
        head._controller_lock = asyncio.Lock()
        with patch.object(ray, "get_actor") as mock_get_actor:
            with pytest.raises(ValueError):
                await head.get_serve_controller()
            mock_get_actor.assert_not_called()

    asyncio.run(_run())


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
