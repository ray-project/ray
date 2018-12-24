from __future__ import absolute_import, division, print_function

import os
import unittest

import redis

import ray


def parse_client(addr_port_str):
    redis_address, redis_port = addr_port_str.split(":")
    return redis.StrictRedis(host=redis_address, port=redis_port)


@unittest.skipIf(not os.environ.get("RAY_USE_NEW_GCS", False),
                 "Tests functionality of the new GCS.")
class CredisTest(unittest.TestCase):
    def setUp(self):
        self.config = ray.init(num_cpus=0)

    def tearDown(self):
        ray.shutdown()

    def test_credis_started(self):
        assert "redis_address" in self.config
        primary = parse_client(self.config['redis_address'])
        assert primary.ping() is True
        member = primary.lrange('RedisShards', 0, -1)[0]
        shard = parse_client(member.decode())

        # Check that primary has loaded credis' master module.
        chain = primary.execute_command('MASTER.GET_CHAIN')
        assert len(chain) == 1

        # Check that the shard has loaded credis' member module.
        assert chain[0] == member
        assert shard.execute_command('MEMBER.SN') == -1


if __name__ == "__main__":
    unittest.main(verbosity=2)
