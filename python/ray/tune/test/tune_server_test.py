from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import socket

import ray
from ray.rllib import _register_all
from ray.tune.trial import Trial, Resources
from ray.tune.web_server import TuneClient
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.trial_runner import TrialRunner


def get_valid_port():
    port = 4321
    while True:
        try:
            print("Trying port", port)
            port_test_socket = socket.socket()
            port_test_socket.bind(("127.0.0.1", port))
            port_test_socket.close()
            break
        except socket.error:
            port += 1
    return port


class TuneServerSuite(unittest.TestCase):
    def basicSetup(self):
        ray.init(num_cpus=4, num_gpus=1)
        port = get_valid_port()
        self.runner = TrialRunner(
            BasicVariantGenerator(), launch_web_server=True, server_port=port)
        runner = self.runner
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 3
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)
        client = TuneClient("localhost:{}".format(port))
        return runner, client

    def tearDown(self):
        print("Tearing down....")
        try:
            self.runner._server.shutdown()
            self.runner = None
        except Exception as e:
            print(e)
        ray.shutdown()
        _register_all()

    def testAddTrial(self):
        runner, client = self.basicSetup()
        for i in range(3):
            runner.step()
        spec = {
            "run": "__fake",
            "stop": {
                "training_iteration": 3
            },
            "trial_resources": {
                'cpu': 1,
                'gpu': 1
            },
        }
        client.add_trial("test", spec)
        runner.step()
        all_trials = client.get_all_trials()["trials"]
        runner.step()
        self.assertEqual(len(all_trials), 3)

    def testGetTrials(self):
        runner, client = self.basicSetup()
        for i in range(3):
            runner.step()
        all_trials = client.get_all_trials()["trials"]
        self.assertEqual(len(all_trials), 2)
        tid = all_trials[0]["id"]
        client.get_trial(tid)
        runner.step()
        self.assertEqual(len(all_trials), 2)

    def testStopTrial(self):
        """Check if Stop Trial works"""
        runner, client = self.basicSetup()
        for i in range(2):
            runner.step()
        all_trials = client.get_all_trials()["trials"]
        self.assertEqual(
            len([t for t in all_trials if t["status"] == Trial.RUNNING]), 1)

        tid = [t for t in all_trials if t["status"] == Trial.RUNNING][0]["id"]
        client.stop_trial(tid)
        runner.step()

        all_trials = client.get_all_trials()["trials"]
        self.assertEqual(
            len([t for t in all_trials if t["status"] == Trial.RUNNING]), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
