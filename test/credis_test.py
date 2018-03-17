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
        self.config = ray.init(num_workers=0)

    def tearDown(self):
        ray.worker.cleanup()

    def test_credis_started(self):
        assert "credis_address" in self.config
        credis_address, credis_port = self.config["credis_address"].split(":")
        credis_client = redis.StrictRedis(host=credis_address,
                                          port=credis_port)
        assert credis_client.ping() is True

        redis_client = ray.worker.global_state.redis_client
        addr = redis_client.get("credis_address").decode("ascii")
        assert addr == self.config["credis_address"]


if __name__ == "__main__":
    unittest.main(verbosity=2)
