import os
import time
from unittest.mock import Mock, patch

import pytest

from ray.util.client.ray_client_helpers import ray_start_client_server


def test_dataclient_disconnect_on_request():
    # Client can't signal graceful shutdown to server after unrecoverable
    # error. Lower grace period so we don't have to sleep as long before
    # checking new connection data.
    with patch.dict(
        os.environ, {"RAY_CLIENT_RECONNECT_GRACE_PERIOD": "5"}
    ), ray_start_client_server() as ray:
        assert ray.is_connected()

        @ray.remote
        def f():
            return 42

        assert ray.get(f.remote()) == 42
        # Force grpc to error by sending garbage request
        with pytest.raises(ConnectionError):
            ray.worker.data_client._blocking_send(Mock())

        # Client should be disconnected
        assert not ray.is_connected()

        # Test that a new connection can be made
        time.sleep(5)  # Give server time to clean up old connection
        connection_data = ray.connect("localhost:50051")
        assert connection_data["num_clients"] == 1
        assert ray.get(f.remote()) == 42


def test_dataclient_disconnect_before_request():
    # Client can't signal graceful shutdown to server after unrecoverable
    # error. Lower grace period so we don't have to sleep as long before
    # checking new connection data.
    with patch.dict(
        os.environ, {"RAY_CLIENT_RECONNECT_GRACE_PERIOD": "5"}
    ), ray_start_client_server() as ray:
        assert ray.is_connected()

        @ray.remote
        def f():
            return 42

        assert ray.get(f.remote()) == 42
        # Force grpc to error by queueing garbage request. This simulates
        # the data channel shutting down for connection issues between
        # different remote calls.
        ray.worker.data_client.request_queue.put(Mock())

        # The following two assertions are relatively brittle. Consider a more
        # robust mechanism if they fail with code changes or become flaky.

        # The next remote call should error since the data channel will shut
        # down because of the invalid input above. Two cases can happen:
        # (1) Data channel shuts down after `f.remote()` finishes.
        #     error is raised to `ray.get()`. The next background operation
        #     will disconnect Ray client.
        # (2) Data channel shuts down before `f.remote()` is called.
        #     `f.remote()` will raise the error and disconnect the client.
        with pytest.raises(ConnectionError):
            ray.get(f.remote())

        with pytest.raises(
            ConnectionError, match="Ray client has already been disconnected"
        ):
            ray.get(f.remote())

        # Client should be disconnected
        assert not ray.is_connected()

        # Test that a new connection can be made
        time.sleep(5)  # Give server time to clean up old connection
        connection_data = ray.connect("localhost:50051")
        assert connection_data["num_clients"] == 1
        assert ray.get(f.remote()) == 42


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
