import unittest

from ray.autoscaler._private.command_runner import KUBECTL_RSYNC
from ray.tune.integration.kubernetes import KubernetesSyncer, KubernetesSyncClient
from ray.tune.syncer import NodeSyncer


class _MockProcessRunner:
    def __init__(self):
        self.history = []

    def check_call(self, command):
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

    def __call__(self, ip):
        return self.ip_to_node[ip]


def _create_mock_syncer(
    namespace, lookup, process_runner, local_ip, local_dir, remote_dir
):
    class _MockSyncer(KubernetesSyncer):
        _namespace = namespace
        _get_kubernetes_node_by_ip = lookup

        def __init__(self, local_dir, remote_dir, sync_client):
            self.local_ip = local_ip
            self.local_node = self._get_kubernetes_node_by_ip(self.local_ip)
            self.worker_ip = None
            self.worker_node = None

            sync_client = sync_client
            super(NodeSyncer, self).__init__(local_dir, remote_dir, sync_client)

    return _MockSyncer(
        local_dir,
        remote_dir,
        sync_client=KubernetesSyncClient(
            namespace=namespace, process_runner=process_runner
        ),
    )


class KubernetesIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.namespace = "test_ray"
        self.lookup = _MockLookup({"head": "1.0.0.0", "w1": "1.0.0.1", "w2": "1.0.0.2"})
        self.process_runner = _MockProcessRunner()
        self.local_dir = "/tmp/local"
        self.remote_dir = "/tmp/remote"

    def tearDown(self):
        pass

    def testKubernetesRsyncUpDown(self):
        syncer = _create_mock_syncer(
            self.namespace,
            self.lookup,
            self.process_runner,
            self.lookup.get_ip("head"),
            self.local_dir,
            self.remote_dir,
        )

        syncer.set_worker_ip(self.lookup.get_ip("w1"))

        # Test sync up. Should add / to the dirs and call rsync
        syncer.sync_up()
        self.assertEqual(self.process_runner.history[-1][0], KUBECTL_RSYNC)
        self.assertEqual(self.process_runner.history[-1][-2], self.local_dir + "/")
        self.assertEqual(
            self.process_runner.history[-1][-1],
            "{}@{}:{}".format("w1", self.namespace, self.remote_dir + "/"),
        )

        # Test sync down.
        syncer.sync_down()
        self.assertEqual(self.process_runner.history[-1][0], KUBECTL_RSYNC)
        self.assertEqual(
            self.process_runner.history[-1][-2],
            "{}@{}:{}".format("w1", self.namespace, self.remote_dir + "/"),
        )
        self.assertEqual(self.process_runner.history[-1][-1], self.local_dir + "/")

        # Sync to same node should be ignored
        syncer.set_worker_ip(self.lookup.get_ip("head"))
        syncer.sync_up()
        self.assertTrue(len(self.process_runner.history) == 2)

        syncer.sync_down()
        self.assertTrue(len(self.process_runner.history) == 2)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
