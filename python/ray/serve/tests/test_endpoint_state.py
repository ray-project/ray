import sys
from typing import Any, Tuple
from unittest.mock import patch, Mock

import pytest
from ray.serve._private.endpoint_state import EndpointState


class MockKVStore:
    def __init__(self):
        self.store = dict()

    def put(self, key: str, val: Any) -> bool:
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        self.store[key] = val
        return True

    def get(self, key: str) -> Any:
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        return self.store.get(key, None)

    def delete(self, key: str) -> bool:
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        if key in self.store:
            del self.store[key]
            return True

        return False


@pytest.fixture
def mock_endpoint_state() -> Tuple[EndpointState, Mock]:
    with patch("ray.serve._private.long_poll.LongPollHost") as mock_long_poll:
        endpoint_state = EndpointState(
            kv_store=MockKVStore(),
            long_poll_host=mock_long_poll,
        )
        yield endpoint_state


def test_is_ready_for_shutdown(mock_endpoint_state):
    """Test `is_ready_for_shutdown()` returns the correct state.

    Before shutting down endpoint `is_ready_for_shutdown()` should return False.
    After shutting down endpoint `is_ready_for_shutdown()` should return True.
    """
    # Setup endpoint state with checkpoint
    endpoint_state = mock_endpoint_state
    endpoint_state._checkpoint()

    # Before shutdown is called, `is_ready_for_shutdown()` should return False
    assert not endpoint_state.is_ready_for_shutdown()

    endpoint_state.shutdown()

    # After shutdown is called, `is_ready_for_shutdown()` should return True
    assert endpoint_state.is_ready_for_shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
