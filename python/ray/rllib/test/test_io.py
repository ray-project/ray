from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import glob
import numpy as np
import os
import shutil
import tempfile
import time
import unittest

import ray
from ray.rllib.agents.pg import PGAgent
from ray.rllib.evaluation import SampleBatch
from ray.rllib.offline import IOContext, JsonWriter, JsonReader
from ray.rllib.offline.json_writer import _to_json

SAMPLES = SampleBatch({
    "actions": np.array([1, 2, 3]),
    "obs": np.array([4, 5, 6])
})


def make_sample_batch(i):
    return SampleBatch({
        "actions": np.array([i, i, i]),
        "obs": np.array([i, i, i])
    })


class AgentIOTest(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def writeOutputs(self, output):
        agent = PGAgent(
            env="CartPole-v0",
            config={
                "output": output,
                "sample_batch_size": 250,
            })
        agent.train()
        return agent

    def testAgentOutputOk(self):
        self.writeOutputs(self.test_dir)
        self.assertEqual(len(os.listdir(self.test_dir)), 1)
        ioctx = IOContext(self.test_dir, {}, 0, None)
        reader = JsonReader(ioctx, self.test_dir + "/*.json")
        reader.next()

    def testAgentOutputLogdir(self):
        agent = self.writeOutputs("logdir")
        self.assertEqual(len(glob.glob(agent.logdir + "/output-*.json")), 1)

    def testAgentInputDir(self):
        self.writeOutputs(self.test_dir)
        agent = PGAgent(
            env="CartPole-v0",
            config={
                "input": self.test_dir,
                "input_evaluation": None,
            })
        result = agent.train()
        self.assertEqual(result["timesteps_total"], 250)  # read from input
        self.assertTrue(np.isnan(result["episode_reward_mean"]))

    def testAgentInputEvalSim(self):
        self.writeOutputs(self.test_dir)
        agent = PGAgent(
            env="CartPole-v0",
            config={
                "input": self.test_dir,
                "input_evaluation": "simulation",
            })
        for _ in range(50):
            result = agent.train()
            if not np.isnan(result["episode_reward_mean"]):
                return  # simulation ok
            time.sleep(0.1)
        assert False, "did not see any simulation results"

    def testAgentInputList(self):
        self.writeOutputs(self.test_dir)
        agent = PGAgent(
            env="CartPole-v0",
            config={
                "input": glob.glob(self.test_dir + "/*.json"),
                "input_evaluation": None,
                "sample_batch_size": 99,
            })
        result = agent.train()
        self.assertEqual(result["timesteps_total"], 250)  # read from input
        self.assertTrue(np.isnan(result["episode_reward_mean"]))

    def testAgentInputDict(self):
        self.writeOutputs(self.test_dir)
        agent = PGAgent(
            env="CartPole-v0",
            config={
                "input": {
                    self.test_dir: 0.1,
                    "sampler": 0.9,
                },
                "train_batch_size": 2000,
                "input_evaluation": None,
            })
        result = agent.train()
        self.assertTrue(not np.isnan(result["episode_reward_mean"]))


class JsonIOTest(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def testWriteSimple(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            ioctx, self.test_dir, max_file_size=1000, compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        writer.write(SAMPLES)
        writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 1)

    def testWriteFileURI(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            ioctx,
            "file:" + self.test_dir,
            max_file_size=1000,
            compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        writer.write(SAMPLES)
        writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 1)

    def testWritePaginate(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            ioctx, self.test_dir, max_file_size=5000, compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        for _ in range(100):
            writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 12)

    def testReadWrite(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            ioctx, self.test_dir, max_file_size=5000, compress_columns=["obs"])
        for i in range(100):
            writer.write(make_sample_batch(i))
        reader = JsonReader(ioctx, self.test_dir + "/*.json")
        seen_a = set()
        seen_o = set()
        for i in range(1000):
            batch = reader.next()
            seen_a.add(batch["actions"][0])
            seen_o.add(batch["obs"][0])
        self.assertGreater(len(seen_a), 90)
        self.assertLess(len(seen_a), 101)
        self.assertGreater(len(seen_o), 90)
        self.assertLess(len(seen_o), 101)

    def testSkipsOverEmptyLinesAndFiles(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        open(self.test_dir + "/empty", "w").close()
        with open(self.test_dir + "/f1", "w") as f:
            f.write("\n")
            f.write("\n")
            f.write(_to_json(make_sample_batch(0), []))
        with open(self.test_dir + "/f2", "w") as f:
            f.write(_to_json(make_sample_batch(1), []))
            f.write("\n")
        reader = JsonReader(ioctx, [
            self.test_dir + "/empty",
            self.test_dir + "/f1",
            "file:" + self.test_dir + "/f2",
        ])
        seen_a = set()
        for i in range(100):
            batch = reader.next()
            seen_a.add(batch["actions"][0])
        self.assertEqual(len(seen_a), 2)

    def testSkipsOverCorruptedLines(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        with open(self.test_dir + "/f1", "w") as f:
            f.write(_to_json(make_sample_batch(0), []))
            f.write("\n")
            f.write(_to_json(make_sample_batch(1), []))
            f.write("\n")
            f.write(_to_json(make_sample_batch(2), []))
            f.write("\n")
            f.write(_to_json(make_sample_batch(3), []))
            f.write("\n")
            f.write("{..corrupted_json_record")
        reader = JsonReader(ioctx, [
            self.test_dir + "/f1",
        ])
        seen_a = set()
        for i in range(10):
            batch = reader.next()
            seen_a.add(batch["actions"][0])
        self.assertEqual(len(seen_a), 4)

    def testAbortOnAllEmptyInputs(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        open(self.test_dir + "/empty", "w").close()
        reader = JsonReader(ioctx, [
            self.test_dir + "/empty",
        ])
        self.assertRaises(ValueError, lambda: reader.next())
        with open(self.test_dir + "/empty1", "w") as f:
            for _ in range(100):
                f.write("\n")
        with open(self.test_dir + "/empty2", "w") as f:
            for _ in range(100):
                f.write("\n")
        reader = JsonReader(ioctx, [
            self.test_dir + "/empty1",
            self.test_dir + "/empty2",
        ])
        self.assertRaises(ValueError, lambda: reader.next())


if __name__ == "__main__":
    ray.init(num_cpus=1)
    unittest.main(verbosity=2)
