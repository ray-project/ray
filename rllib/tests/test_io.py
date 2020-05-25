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
from ray.tune.registry import register_env
from ray.rllib.agents.pg import PGTrainer
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.offline import IOContext, JsonWriter, JsonReader
from ray.rllib.offline.json_writer import _to_json
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import framework_iterator

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

    def writeOutputs(self, output, fw):
        agent = PGTrainer(
            env="CartPole-v0",
            config={
                "output": output + (fw if output != "logdir" else ""),
                "rollout_fragment_length": 250,
                "framework": fw,
            })
        agent.train()
        return agent

    def testAgentOutputOk(self):
        for fw in framework_iterator(frameworks=("torch", "tf")):
            self.writeOutputs(self.test_dir, fw)
            self.assertEqual(len(os.listdir(self.test_dir + fw)), 1)
            reader = JsonReader(self.test_dir + fw + "/*.json")
            reader.next()

    def testAgentOutputLogdir(self):
        """Test special value 'logdir' as Agent's output."""
        for fw in framework_iterator():
            agent = self.writeOutputs("logdir", fw)
            self.assertEqual(
                len(glob.glob(agent.logdir + "/output-*.json")), 1)

    def testAgentInputDir(self):
        for fw in framework_iterator(frameworks=("torch", "tf")):
            self.writeOutputs(self.test_dir, fw)
            agent = PGTrainer(
                env="CartPole-v0",
                config={
                    "input": self.test_dir + fw,
                    "input_evaluation": [],
                    "framework": fw,
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
        for fw in framework_iterator(frameworks=("tf", "torch")):
            self.writeOutputs(self.test_dir, fw)

            # Rewrite the files to drop advantages and value_targets for
            # testing
            for path in glob.glob(self.test_dir + fw + "/*.json"):
                out = []
                with open(path) as f:
                    for line in f.readlines():
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
                    "input": self.test_dir + fw,
                    "input_evaluation": [],
                    "postprocess_inputs": True,  # adds back 'advantages'
                    "framework": fw,
                })

            result = agent.train()
            self.assertEqual(result["timesteps_total"], 250)  # read from input
            self.assertTrue(np.isnan(result["episode_reward_mean"]))

    def testAgentInputEvalSim(self):
        for fw in framework_iterator():
            self.writeOutputs(self.test_dir, fw)
            agent = PGTrainer(
                env="CartPole-v0",
                config={
                    "input": self.test_dir + fw,
                    "input_evaluation": ["simulation"],
                    "framework": fw,
                })
            for _ in range(50):
                result = agent.train()
                if not np.isnan(result["episode_reward_mean"]):
                    return  # simulation ok
                time.sleep(0.1)
            assert False, "did not see any simulation results"

    def testAgentInputList(self):
        for fw in framework_iterator(frameworks=("torch", "tf")):
            self.writeOutputs(self.test_dir, fw)
            agent = PGTrainer(
                env="CartPole-v0",
                config={
                    "input": glob.glob(self.test_dir + fw + "/*.json"),
                    "input_evaluation": [],
                    "rollout_fragment_length": 99,
                    "framework": fw,
                })
            result = agent.train()
            self.assertEqual(result["timesteps_total"], 250)  # read from input
            self.assertTrue(np.isnan(result["episode_reward_mean"]))

    def testAgentInputDict(self):
        for fw in framework_iterator():
            self.writeOutputs(self.test_dir, fw)
            agent = PGTrainer(
                env="CartPole-v0",
                config={
                    "input": {
                        self.test_dir + fw: 0.1,
                        "sampler": 0.9,
                    },
                    "train_batch_size": 2000,
                    "input_evaluation": [],
                    "framework": fw,
                })
            result = agent.train()
            self.assertTrue(not np.isnan(result["episode_reward_mean"]))

    def testMultiAgent(self):
        register_env("multi_agent_cartpole",
                     lambda _: MultiAgentCartPole({"num_agents": 10}))
        single_env = gym.make("CartPole-v0")

        def gen_policy():
            obs_space = single_env.observation_space
            act_space = single_env.action_space
            return (PGTFPolicy, obs_space, act_space, {})

        for fw in framework_iterator():
            pg = PGTrainer(
                env="multi_agent_cartpole",
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
                    "framework": fw,
                })
            pg.train()
            self.assertEqual(len(os.listdir(self.test_dir)), 1)

            pg.stop()
            pg = PGTrainer(
                env="multi_agent_cartpole",
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
                    "framework": fw,
                })
            for _ in range(50):
                result = pg.train()
                if not np.isnan(result["episode_reward_mean"]):
                    return  # simulation ok
                time.sleep(0.1)
            assert False, "did not see any simulation results"


class JsonIOTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1)
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        ray.shutdown()

    def test_write_simple(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            self.test_dir, ioctx, max_file_size=1000, compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        writer.write(SAMPLES)
        writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 1)

    def test_write_file_uri(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            "file://" + self.test_dir,
            ioctx,
            max_file_size=1000,
            compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        writer.write(SAMPLES)
        writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 1)

    def test_write_paginate(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            self.test_dir, ioctx, max_file_size=5000, compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        for _ in range(100):
            writer.write(SAMPLES)
        num_files = len(os.listdir(self.test_dir))
        # Magic numbers: 2: On travis, it seems to create only 2 files,
        #                   but sometimes also 7.
        #                12 or 13: Mac locally.
        # Reasons: Different compressions, file-size interpretations,
        #  json writers?
        assert num_files in [2, 7, 12, 13], \
            "Expected 2|7|12|13 files, but found {} ({})". \
            format(num_files, os.listdir(self.test_dir))

    def test_read_write(self):
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

    def test_skips_over_empty_lines_and_files(self):
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
            "file://" + self.test_dir + "/f2",
        ])
        seen_a = set()
        for i in range(100):
            batch = reader.next()
            seen_a.add(batch["actions"][0])
        self.assertEqual(len(seen_a), 2)

    def test_skips_over_corrupted_lines(self):
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

    def test_abort_on_all_empty_inputs(self):
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
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
