from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import os
import pytest
import redis

import ray


@pytest.fixture
def password():
    random_bytes = os.urandom(128)
    if hasattr(random_bytes, "hex"):
        return random_bytes.hex()  # Python 3
    return random_bytes.encode("hex")  # Python 2


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


class TestRedisPassword(object):
    @pytest.mark.skipif(
        os.environ.get("RAY_USE_NEW_GCS") != "on"
        and os.environ.get("RAY_USE_XRAY"),
        reason="Redis authentication works for raylet and old GCS.")
    def test_exceptions(self, password, shutdown_only):
        with pytest.raises(Exception):
            ray.init(redis_password=password)

    @pytest.mark.skipif(
        os.environ.get("RAY_USE_NEW_GCS") == "on",
        reason="New GCS API doesn't support Redis authentication yet.")
    @pytest.mark.skipif(
        not os.environ.get("RAY_USE_XRAY"),
        reason="Redis authentication is not supported in legacy Ray.")
    def test_redis_password(self, password, shutdown_only):
        # Workaround for https://github.com/ray-project/ray/issues/3045
        @ray.remote
        def f():
            return 1

        info = ray.init(redis_password=password)
        redis_address = info["redis_address"]
        redis_ip, redis_port = redis_address.split(":")

        # Check that we can run a task
        object_id = f.remote()
        ready, running = ray.wait([object_id], timeout=5000)
        assert len(ready) > 0, "Expected task to complete"

        # Check that Redis connections require a password
        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=None)
        with pytest.raises(redis.ResponseError):
            redis_client.ping()

        # Check that we can connect to Redis using the provided password
        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=password)
        assert redis_client.ping()
