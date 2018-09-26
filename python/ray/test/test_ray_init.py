from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import redis

import ray


@pytest.fixture
def start_ray_with_password():
    ray.shutdown()

    password = "some_password"
    exception = None
    try:
        info = ray.init(redis_password=password)
    except Exception as e:
        info = ray.init(redis_password=None)
        exception = e
    use_raylet = ray.global_state.use_raylet

    return password, info, exception, use_raylet


class TestRedisPassword(object):
    def test_raylet_only(self, start_ray_with_password):
        password, info, exception, use_raylet = start_ray_with_password
        if use_raylet:
            assert exception is None
        else:
            assert exception is not None

    def test_redis_password(self, start_ray_with_password):
        password, info, exception, use_raylet = start_ray_with_password

        if not use_raylet:
            return

        redis_address = info["redis_address"]
        redis_ip, redis_port = redis_address.split(":")

        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=password)

        assert redis_client.ping()

        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=None)
        with pytest.raises(redis.ResponseError):
            redis_client.ping()
