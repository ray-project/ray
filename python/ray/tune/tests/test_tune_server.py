import os
import requests
import socket
import subprocess
import unittest
import json

import ray
from ray.rllib import _register_all
from ray.tune.trial import Trial, Resources
from ray.tune.web_server import TuneClient
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
        # Wait up to five seconds for placement groups when starting a trial
        os.environ["TUNE_PLACEMENT_GROUP_WAIT_S"] = "5"
        # Block for results even when placement groups are pending
        os.environ["TUNE_TRIAL_STARTUP_GRACE_PERIOD"] = "0"

        ray.init(num_cpus=4, num_gpus=1)
        port = get_valid_port()
        self.runner = TrialRunner(server_port=port)
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
        client = TuneClient("localhost", port)
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
            "resources_per_trial": {
                "cpu": 1,
                "gpu": 1
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

    def testGetTrialsWithFunction(self):
        runner, client = self.basicSetup()
        test_trial = Trial(
            "__fake",
            trial_id="function_trial",
            stopping_criterion={"training_iteration": 3},
            config={"callbacks": {
                "on_episode_start": lambda x: None
            }})
        runner.add_trial(test_trial)

        for i in range(3):
            runner.step()
        all_trials = client.get_all_trials()["trials"]
        self.assertEqual(len(all_trials), 3)
        client.get_trial("function_trial")
        runner.step()
        self.assertEqual(len(all_trials), 3)

    def testStopTrial(self):
        """Check if Stop Trial works."""
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

    def testStopExperiment(self):
        """Check if stop_experiment works."""
        runner, client = self.basicSetup()
        for i in range(2):
            runner.step()
        all_trials = client.get_all_trials()["trials"]
        self.assertEqual(
            len([t for t in all_trials if t["status"] == Trial.RUNNING]), 1)

        client.stop_experiment()
        runner.step()
        self.assertTrue(runner.is_finished())
        self.assertRaises(
            requests.exceptions.ReadTimeout,
            lambda: client.get_all_trials(timeout=1))

    def testCurlCommand(self):
        """Check if Stop Trial works."""
        runner, client = self.basicSetup()
        for i in range(2):
            runner.step()
        stdout = subprocess.check_output(
            "curl \"http://{}:{}/trials\"".format(client.server_address,
                                                  client.server_port),
            shell=True)
        self.assertNotEqual(stdout, None)
        curl_trials = json.loads(stdout.decode())["trials"]
        client_trials = client.get_all_trials()["trials"]
        for curl_trial, client_trial in zip(curl_trials, client_trials):
            self.assertEqual(curl_trial.keys(), client_trial.keys())
            self.assertEqual(curl_trial["id"], client_trial["id"])
            self.assertEqual(curl_trial["trainable_name"],
                             client_trial["trainable_name"])
            self.assertEqual(curl_trial["status"], client_trial["status"])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
