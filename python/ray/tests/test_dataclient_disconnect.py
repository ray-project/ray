from ray.util.client.ray_client_helpers import ray_start_client_server_pair
import time
import pytest


def test_dataclient_disconnect_before_request():
    with ray_start_client_server_pair() as (ray, server):
        assert ray.is_connected()

        @ray.remote
        def f():
            return 42

        assert ray.get(f.remote()) == 42

        # Kill grpc server
        server.stop(0)
        time.sleep(3)

        # The next remote call should error since the data channel has shut
        # down, which should also disconnect the client.
        with pytest.raises(ConnectionError):
            ray.get(f.remote())

        assert not ray.is_connected()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
