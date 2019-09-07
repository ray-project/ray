from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import glob
import gym
import json
import numpy as np
import os
import random
import shutil
import tempfile
import time
import unittest

import ray
from ray.rllib.agents.pg import PGTrainer
from ray.rllib.agents.pg.pg_policy import PGTFPolicy
from ray.rllib.evaluation import SampleBatch
from ray.rllib.offline import IOContext, JsonWriter, JsonReader
from ray.rllib.offline.json_writer import _to_json
from ray.rllib.tests.test_multi_agent_env import MultiCartpole
from ray.tune.registry import register_env

SAMPLES = SampleBatch({
    "actions": np.array([1, 2, 3, 4]),
    "obs": np.array([4, 5, 6, 7]),
    "eps_id": [1, 1, 2, 3],
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
        agent = PGTrainer(
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
        reader = JsonReader(self.test_dir + "/*.json")
        reader.next()

    def testAgentOutputLogdir(self):
        agent = self.writeOutputs("logdir")
        self.assertEqual(len(glob.glob(agent.logdir + "/output-*.json")), 1)

    def testAgentInputDir(self):
        self.writeOutputs(self.test_dir)
        agent = PGTrainer(
            env="CartPole-v0",
            config={
                "input": self.test_dir,
                "input_evaluation": [],
            })
        result = agent.train()
        self.assertEqual(result["timesteps_total"], 250)  # read from input
        self.assertTrue(np.isnan(result["episode_reward_mean"]))

    def testSplitByEpisode(self):
        splits = SAMPLES.split_by_episode()
        self.assertEqual(len(splits), 3)
        self.assertEqual(splits[0].count, 2)
        self.assertEqual(splits[1].count, 1)
        self.assertEqual(splits[2].count, 1)

    def testAgentInputPostprocessingEnabled(self):
        self.writeOutputs(self.test_dir)

        # Rewrite the files to drop advantages and value_targets for testing
        for path in glob.glob(self.test_dir + "/*.json"):
            out = []
            for line in open(path).readlines():
                data = json.loads(line)
                del data["advantages"]
                del data["value_targets"]
                out.append(data)
            with open(path, "w") as f:
                for data in out:
                    f.write(json.dumps(data))

        agent = PGTrainer(
            env="CartPole-v0",
            config={
                "input": self.test_dir,
                "input_evaluation": [],
                "postprocess_inputs": True,  # adds back 'advantages'
            })

        result = agent.train()
        self.assertEqual(result["timesteps_total"], 250)  # read from input
        self.assertTrue(np.isnan(result["episode_reward_mean"]))

    def testAgentInputEvalSim(self):
        self.writeOutputs(self.test_dir)
        agent = PGTrainer(
            env="CartPole-v0",
            config={
                "input": self.test_dir,
                "input_evaluation": ["simulation"],
            })
        for _ in range(50):
            result = agent.train()
            if not np.isnan(result["episode_reward_mean"]):
                return  # simulation ok
            time.sleep(0.1)
        assert False, "did not see any simulation results"

    def testAgentInputList(self):
        self.writeOutputs(self.test_dir)
        agent = PGTrainer(
            env="CartPole-v0",
            config={
                "input": glob.glob(self.test_dir + "/*.json"),
                "input_evaluation": [],
                "sample_batch_size": 99,
            })
        result = agent.train()
        self.assertEqual(result["timesteps_total"], 250)  # read from input
        self.assertTrue(np.isnan(result["episode_reward_mean"]))

    def testAgentInputDict(self):
        self.writeOutputs(self.test_dir)
        agent = PGTrainer(
            env="CartPole-v0",
            config={
                "input": {
                    self.test_dir: 0.1,
                    "sampler": 0.9,
                },
                "train_batch_size": 2000,
                "input_evaluation": [],
            })
        result = agent.train()
        self.assertTrue(not np.isnan(result["episode_reward_mean"]))

    def testMultiAgent(self):
        register_env("multi_cartpole", lambda _: MultiCartpole(10))
        single_env = gym.make("CartPole-v0")

        def gen_policy():
            obs_space = single_env.observation_space
            act_space = single_env.action_space
            return (PGTFPolicy, obs_space, act_space, {})

        pg = PGTrainer(
            env="multi_cartpole",
            config={
                "num_workers": 0,
                "output": self.test_dir,
                "multiagent": {
                    "policies": {
                        "policy_1": gen_policy(),
                        "policy_2": gen_policy(),
                    },
                    "policy_mapping_fn": (
                        lambda agent_id: random.choice(
                            ["policy_1", "policy_2"])),
                },
            })
        pg.train()
        self.assertEqual(len(os.listdir(self.test_dir)), 1)

        pg.stop()
        pg = PGTrainer(
            env="multi_cartpole",
            config={
                "num_workers": 0,
                "input": self.test_dir,
                "input_evaluation": ["simulation"],
                "train_batch_size": 2000,
                "multiagent": {
                    "policies": {
                        "policy_1": gen_policy(),
                        "policy_2": gen_policy(),
                    },
                    "policy_mapping_fn": (
                        lambda agent_id: random.choice(
                            ["policy_1", "policy_2"])),
                },
            })
        for _ in range(50):
            result = pg.train()
            if not np.isnan(result["episode_reward_mean"]):
                return  # simulation ok
            time.sleep(0.1)
        assert False, "did not see any simulation results"


class JsonIOTest(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def testWriteSimple(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            self.test_dir, ioctx, max_file_size=1000, compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        writer.write(SAMPLES)
        writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 1)

    def testWriteFileURI(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            "file:" + self.test_dir,
            ioctx,
            max_file_size=1000,
            compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        writer.write(SAMPLES)
        writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 1)

    def testWritePaginate(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            self.test_dir, ioctx, max_file_size=5000, compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        for _ in range(100):
            writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 12)

    def testReadWrite(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            self.test_dir, ioctx, max_file_size=5000, compress_columns=["obs"])
        for i in range(100):
            writer.write(make_sample_batch(i))
        reader = JsonReader(self.test_dir + "/*.json")
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
        open(self.test_dir + "/empty", "w").close()
        with open(self.test_dir + "/f1", "w") as f:
            f.write("\n")
            f.write("\n")
            f.write(_to_json(make_sample_batch(0), []))
        with open(self.test_dir + "/f2", "w") as f:
            f.write(_to_json(make_sample_batch(1), []))
            f.write("\n")
        reader = JsonReader([
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
        reader = JsonReader([
            self.test_dir + "/f1",
        ])
        seen_a = set()
        for i in range(10):
            batch = reader.next()
            seen_a.add(batch["actions"][0])
        self.assertEqual(len(seen_a), 4)

    def testAbortOnAllEmptyInputs(self):
        open(self.test_dir + "/empty", "w").close()
        reader = JsonReader([
            self.test_dir + "/empty",
        ])
        self.assertRaises(ValueError, lambda: reader.next())
        with open(self.test_dir + "/empty1", "w") as f:
            for _ in range(100):
                f.write("\n")
        with open(self.test_dir + "/empty2", "w") as f:
            for _ in range(100):
                f.write("\n")
        reader = JsonReader([
            self.test_dir + "/empty1",
            self.test_dir + "/empty2",
        ])
        self.assertRaises(ValueError, lambda: reader.next())


if __name__ == "__main__":
    ray.init(num_cpus=1)
    unittest.main(verbosity=2)
