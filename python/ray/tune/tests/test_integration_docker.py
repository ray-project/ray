import unittest
from typing import Optional

from ray.tune.integration.docker import DockerSyncer, DockerSyncClient
from ray.tune.sync_client import SyncClient
from ray.tune.syncer import NodeSyncer


class _MockRsync:
    def __init__(self):
        self.history = []

    def __call__(self, *args, **kwargs):
        self.history.append(kwargs)


class _MockLookup:
    def __init__(self, node_ips):
        self.node_to_ip = {}
        self.ip_to_node = {}
        for node, ip in node_ips.items():
            self.node_to_ip[node] = ip
            self.ip_to_node[ip] = node

    def get_ip(self, node):
        return self.node_to_ip[node]

    def get_node(self, ip):
        return self.ip_to_node[ip]


def _create_mock_syncer(local_ip, local_dir, remote_dir):
    class _MockSyncer(DockerSyncer):
        def __init__(self,
                     local_dir: str,
                     remote_dir: str,
                     sync_client: Optional[SyncClient] = None):
            self.local_ip = local_ip
            self.worker_ip = None

            sync_client = sync_client or DockerSyncClient()
            sync_client.configure("__nofile__")

            super(NodeSyncer, self).__init__(local_dir, remote_dir,
                                             sync_client)

    return _MockSyncer(local_dir, remote_dir)


class DockerIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.lookup = _MockLookup({
            "head": "1.0.0.0",
            "w1": "1.0.0.1",
            "w2": "1.0.0.2"
        })
        self.local_dir = "/tmp/local"
        self.remote_dir = "/tmp/remote"

        self.mock_command = _MockRsync()

        from ray.tune.integration import docker
        docker.rsync = self.mock_command

    def tearDown(self):
        pass

    def testDockerRsyncUpDown(self):
        syncer = _create_mock_syncer(
            self.lookup.get_ip("head"), self.local_dir, self.remote_dir)

        syncer.set_worker_ip(self.lookup.get_ip("w1"))

        # Test sync up. Should add / to the dirs and call rsync
        syncer.sync_up()
        print(self.mock_command.history[-1])
        self.assertEqual(self.mock_command.history[-1]["source"],
                         self.local_dir + "/")
        self.assertEqual(self.mock_command.history[-1]["target"],
                         self.remote_dir + "/")
        self.assertEqual(self.mock_command.history[-1]["down"], False)
        self.assertEqual(self.mock_command.history[-1]["ip_address"],
                         self.lookup.get_ip("w1"))

        # Test sync down.
        syncer.sync_down()
        print(self.mock_command.history[-1])

        self.assertEqual(self.mock_command.history[-1]["target"],
                         self.local_dir + "/")
        self.assertEqual(self.mock_command.history[-1]["source"],
                         self.remote_dir + "/")
        self.assertEqual(self.mock_command.history[-1]["down"], True)
        self.assertEqual(self.mock_command.history[-1]["ip_address"],
                         self.lookup.get_ip("w1"))

        # Sync to same node should be ignored
        prev = len(self.mock_command.history)
        syncer.set_worker_ip(self.lookup.get_ip("head"))
        syncer.sync_up()
        self.assertEqual(len(self.mock_command.history), prev)

        syncer.sync_down()
        self.assertEqual(len(self.mock_command.history), prev)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
