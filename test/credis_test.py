from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import redis
import unittest

import ray


@unittest.skipIf(
    not os.environ.get('RAY_USE_NEW_GCS', False),
    "Tests functionality of the new GCS.")
class CredisTest(unittest.TestCase):
    def setUp(self):
        self.config = ray.init()

    def tearDown(self):
        ray.worker.cleanup()

    def test_credis_started(self):
        assert "credis_address" in self.config
        address, port = self.config["credis_address"].split(":")
        redis_client = redis.StrictRedis(host=address,
                                         port=port)
        assert redis_client.ping() is True


if __name__ == "__main__":
    unittest.main(verbosity=2)
