import unittest
from typing import Dict, Optional

from ray.autoscaler.tags import NODE_KIND_HEAD, NODE_KIND_WORKER, \
    TAG_RAY_NODE_KIND
from ray.tune.integration.docker import DockerSyncer, \
    DockerSyncClient, _WrappedProvider
from ray.tune.sync_client import SyncClient
from ray.tune.syncer import NodeSyncer


class _MockProcessRunner:
    def __init__(self):
        self.history = []

    def check_call(self, command, **kwargs):
        self.history.append(command)
        return True


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


class _MockProvider:
    def __init__(self, lookup: _MockLookup):
        self.lookup = lookup

    def non_terminated_nodes(self, tag_filters: Dict):
        if tag_filters[TAG_RAY_NODE_KIND] == NODE_KIND_HEAD:
            return [w for w in self.lookup.node_to_ip if w.startswith("head")]
        elif tag_filters[TAG_RAY_NODE_KIND] == NODE_KIND_WORKER:
            return [w for w in self.lookup.node_to_ip if w.startswith("w")]
        else:
            return []

    def get_ip_to_node_id(self):
        return self.lookup.node_to_ip, self.lookup.ip_to_node

    def internal_ip(self, node_id: str) -> str:
        return self.lookup.get_ip(node_id)

    def external_ip(self, node_id: str) -> str:
        return self.lookup.get_ip(node_id)


class _MockWrappedProvider(_WrappedProvider):
    def __init__(self, config: Dict, lookup: _MockLookup):
        self._config = config

        self.provider = _MockProvider(lookup)

        self._ip_cache = {}
        self._node_id_cache = {}


def _create_mock_syncer(cluster_config, lookup, process_runner, local_ip,
                        local_dir, remote_dir):
    class _MockSyncer(DockerSyncer):
        def __init__(self,
                     local_dir: str,
                     remote_dir: str,
                     sync_client: Optional[SyncClient] = None):
            _provider = _MockWrappedProvider(cluster_config, lookup)
            self._provider = _provider

            self.local_ip = local_ip
            self.worker_ip = None
            self.worker_node_id = None

            sync_client = sync_client or DockerSyncClient()
            sync_client.configure(_provider, _provider.config)

            super(NodeSyncer, self).__init__(local_dir, remote_dir,
                                             sync_client)

    return _MockSyncer(
        local_dir,
        remote_dir,
        sync_client=DockerSyncClient(process_runner=process_runner))


class DockerIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.cluster_config = {
            "auth": {
                "ssh_user": "dockeruser"
            },
            "cluster_name": "docker_test",
            "docker": {
                "container_name": "dockercontainer"
            }
        }
        self.lookup = _MockLookup({
            "head": "1.0.0.0",
            "w1": "1.0.0.1",
            "w2": "1.0.0.2"
        })
        self.process_runner = _MockProcessRunner()
        self.local_dir = "/tmp/local"
        self.remote_dir = "/tmp/remote"

    def tearDown(self):
        pass

    def testDockerRsyncUpDown(self):
        syncer = _create_mock_syncer(
            self.cluster_config, self.lookup, self.process_runner,
            self.lookup.get_ip("head"), self.local_dir, self.remote_dir)

        syncer.set_worker_ip(self.lookup.get_ip("w1"))

        # Test sync up. Should add / to the dirs and call rsync
        syncer.sync_up()
        print(self.process_runner.history)
        self.assertEqual(self.process_runner.history[-1][0], "rsync")
        self.assertEqual(self.process_runner.history[-1][-2],
                         self.local_dir + "/")
        self.assertEqual(
            self.process_runner.history[-1][-1], "{}@{}:{}{}".format(
                "dockeruser", self.lookup.get_ip("w1"), "/tmp/ray_tmp_mount",
                self.remote_dir + "/"))

        # Test sync down.
        syncer.sync_down()
        self.assertEqual(self.process_runner.history[-1][0], "rsync")
        self.assertEqual(
            self.process_runner.history[-1][-2], "{}@{}:{}{}".format(
                "dockeruser", self.lookup.get_ip("w1"), "/tmp/ray_tmp_mount",
                self.remote_dir + "/"))
        self.assertEqual(self.process_runner.history[-1][-1],
                         self.local_dir + "/")

        # Sync to same node should be ignored
        prev = len(self.process_runner.history)
        syncer.set_worker_ip(self.lookup.get_ip("head"))
        syncer.sync_up()
        self.assertEqual(len(self.process_runner.history), prev)

        syncer.sync_down()
        self.assertEqual(len(self.process_runner.history), prev)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
