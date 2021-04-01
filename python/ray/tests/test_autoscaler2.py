import copy
import os
import shutil
import tempfile
import time
import unittest

import pytest
import yaml

import ray
from ray.autoscaler._private.autoscaler import StandardAutoscaler
from ray.autoscaler._private.util import prepare_config
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler.tags import STATUS_UP_TO_DATE, TAG_RAY_NODE_STATUS
from ray.test_utils import RayTestTimeoutException
from ray.tests.test_autoscaler import _NODE_PROVIDERS, _DEFAULT_CONFIGS,\
    _clear_provider_cache, SMALL_CLUSTER, MockProvider, MockProcessRunner

# TODO(Dmitri) The single test testNodeTerminatedDuringUpdate belongs in
# test_autoscaler.py
# However, apparently due to interference from the other tests there,
# I couldn't get this test to pass consistently when included in that file.


class AutoscalingTest2(unittest.TestCase):
    def setUp(self):
        _NODE_PROVIDERS["mock"] = \
            lambda config: self.create_provider
        _DEFAULT_CONFIGS["mock"] = _DEFAULT_CONFIGS["local"]
        self.provider = None
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        self.provider = None
        del _NODE_PROVIDERS["mock"]
        _clear_provider_cache()
        shutil.rmtree(self.tmpdir)
        ray.shutdown()

    def waitFor(self, condition, num_retries=50, fail_msg=None):
        for _ in range(num_retries):
            if condition():
                return
            time.sleep(.1)
        fail_msg = fail_msg or "Timed out waiting for {}".format(condition)
        raise RayTestTimeoutException(fail_msg)

    def waitForNodes(self, expected, comparison=None, tag_filters={}):
        MAX_ITER = 50
        for i in range(MAX_ITER):
            n = len(self.provider.non_terminated_nodes(tag_filters))
            if comparison is None:
                comparison = self.assertEqual
            try:
                comparison(n, expected, msg="Unexpected node quantity.")
                return
            except Exception:
                if i == MAX_ITER - 1:
                    raise
            time.sleep(.1)

    def create_provider(self, config, cluster_name):
        assert self.provider
        return self.provider

    def write_config(self, config, call_prepare_config=True):
        new_config = copy.deepcopy(config)
        if call_prepare_config:
            new_config = prepare_config(new_config)
        path = os.path.join(self.tmpdir, "simple.yaml")
        with open(path, "w") as f:
            f.write(yaml.dump(new_config))
        return path

    def testZNodeTerminatedDuringUpdate(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(3)])
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(2)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})

        # Mark both nodes as unhealthy
        for _ in range(5):
            if autoscaler.updaters:
                time.sleep(0.05)
                autoscaler.update()
        assert not autoscaler.updaters
        num_calls = len(runner.calls)
        lm.last_heartbeat_time_by_ip["172.0.0.0"] = 0
        lm.last_heartbeat_time_by_ip["172.0.0.1"] = 0

        # Set up process runner to terminate worker 0 during missed heartbeat
        # recovery and also cause the updater to fail.
        def terminate_worker_zero():
            self.provider.terminate_node(0)

        runner.fail_cmds = ["start_ray_worker"]
        runner.cmd_to_callback = {"start_ray_worker": terminate_worker_zero}

        autoscaler.update()
        events = autoscaler.event_summarizer.summary()
        # Both nodes are being recovered.
        assert ("Restarting 2 nodes of type "
                "ray-legacy-worker-node-type (lost contact with raylet)." in
                events), events
        self.waitFor(lambda: len(runner.calls) > num_calls, num_retries=150,
                     fail_msg="Did not get additional calls on first"
                     " autoscaler update.")
        # Node 0 was terminated during update.
        # Node 1's update failed, but it won't be terminated until the next
        # autoscaler update.
        self.waitFor(lambda: 0 not in autoscaler.workers(), num_retries=150,
                     fail_msg="Node zero still non-terminated")
        assert not self.provider.is_terminated(1),\
            "Node one terminated prematurely."

        autoscaler.update()
        assert autoscaler.num_failed_updates[0] == 1,\
            "Node zero update failure not registered"
        assert autoscaler.num_failed_updates[1] == 1,\
            "Node one update failure not registered"
        # Complete-update-processing logic should have terminated node 1.
        assert self.provider.is_terminated(1), "Node 1 not terminated on time."

        events = autoscaler.event_summarizer.summary()
        # Just one node (node_id 1) terminated here.
        # Validates that we didn't try to double-terminate node 0.
        assert ("Removing 1 nodes of type "
                "ray-legacy-worker-node-type (launch failed)." in
                events), events

        # Should get two new nodes after the next update.
        autoscaler.update()
        self.waitForNodes(2)
        assert set(autoscaler.workers()) == {2, 3},\
            "Unexpected node_ids"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
