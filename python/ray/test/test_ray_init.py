from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import redis

import ray


class TestRedisPassword(object):
    PASSWORD = "some_password"
    EXCEPTION = None
    USE_RAYLET = None
    RAY_INFO = None

    @classmethod
    def setup_class(cls):
        try:
            info = ray.init(redis_password=cls.PASSWORD)
            cls.RAY_INFO = info
        except Exception as e:
            cls.EXCEPTION = e
            ray.init(redis_password=None)
        cls.USE_RAYLET = ray.global_state.use_raylet

    @classmethod
    def teardown_class(cls):
        ray.shutdown()

    def test_raylet_only(self):
        if self.USE_RAYLET:
            assert self.EXCEPTION is None
        else:
            assert self.EXCEPTION is not None

    def test_redis_password(self):
        if not self.USE_RAYLET:
            return

        redis_address = self.RAY_INFO["redis_address"]
        redis_ip, redis_port = redis_address.split(":")

        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=self.PASSWORD)

        assert redis_client.ping()

        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=None)
        with pytest.raises(redis.ResponseError):
            redis_client.ping()
