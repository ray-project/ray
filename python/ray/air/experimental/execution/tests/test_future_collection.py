import pytest
from ray.air.experimental.execution.actor_manager import _FutureCollection


class _FakeActor:
    pass


def test_track_future():
    """Test that actor requests are not the same if they share the same properties."""
    collection = _FutureCollection()

    actor = _FakeActor()
    future = {"fake": "future"}

    collection.track_future(actor, future)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
