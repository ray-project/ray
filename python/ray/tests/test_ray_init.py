import os
import pytest
import redis

import ray
from ray.cluster_utils import Cluster


@pytest.fixture
def password():
    random_bytes = os.urandom(128)
    if hasattr(random_bytes, "hex"):
        return random_bytes.hex()  # Python 3
    return random_bytes.encode("hex")  # Python 2


class TestRedisPassword:
    @pytest.mark.skipif(
        os.environ.get("RAY_USE_NEW_GCS") == "on",
        reason="New GCS API doesn't support Redis authentication yet.")
    def test_redis_password(self, password, shutdown_only):
        @ray.remote
        def f():
            return 1

        info = ray.init(redis_password=password)
        address = info["redis_address"]
        redis_ip, redis_port = address.split(":")

        # Check that we can run a task
        object_ref = f.remote()
        ray.get(object_ref)

        # Check that Redis connections require a password
        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=None)
        with pytest.raises(redis.exceptions.AuthenticationError):
            redis_client.ping()

        # Check that we can connect to Redis using the provided password
        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=password)
        assert redis_client.ping()

    @pytest.mark.skipif(
        os.environ.get("RAY_USE_NEW_GCS") == "on",
        reason="New GCS API doesn't support Redis authentication yet.")
    def test_redis_password_cluster(self, password, shutdown_only):
        @ray.remote
        def f():
            return 1

        node_args = {"redis_password": password}
        cluster = Cluster(
            initialize_head=True, connect=True, head_node_args=node_args)
        cluster.add_node(**node_args)

        object_ref = f.remote()
        ray.get(object_ref)

    def test_redis_port(self, shutdown_only):
        @ray.remote
        def f():
            return 1

        info = ray.init(redis_port=1234, redis_password="testpassword")
        address = info["redis_address"]
        redis_ip, redis_port = address.split(":")
        assert redis_port == "1234"

        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password="testpassword")
        assert redis_client.ping()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
