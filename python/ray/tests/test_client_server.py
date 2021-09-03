import pytest
from unittest.mock import patch, Mock

from ray.ray_constants import REDIS_DEFAULT_PASSWORD
import ray.util.client.server.server as client_server


@pytest.mark.parametrize("redis_password", [None, "random_password"])
def test_try_create_redis_client(redis_password):
    create_mock = Mock(side_effect=lambda x, y: x)
    with patch(
            "ray._private.services", create_redis_client=create_mock), patch(
                "ray._private.services.find_redis_address",
                side_effect=[["address0", "address1"], [], ["address0"]]):
        # Two redis addresses found
        assert client_server.try_create_redis_client(None,
                                                     redis_password) is None
        create_mock.assert_not_called()
        # No redis addresses found
        assert client_server.try_create_redis_client(None,
                                                     redis_password) is None
        create_mock.assert_not_called()
        # Exactly one redis address found
        assert client_server.try_create_redis_client(
            None, redis_password) == "address0"
        create_mock.assert_called_once_with(
            "address0", redis_password or REDIS_DEFAULT_PASSWORD)
        create_mock.reset_mock()
        # Manually specify redis
        client_server.try_create_redis_client("address100", redis_password)
        create_mock.assert_called_once_with(
            "address100", redis_password or REDIS_DEFAULT_PASSWORD)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
