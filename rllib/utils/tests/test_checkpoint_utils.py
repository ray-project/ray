import os
from pathlib import Path
import tempfile
import unittest

import ray
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.checkpoints import (
    get_checkpoint_info,
    convert_to_msgpack_checkpoint,
    convert_to_msgpack_policy_checkpoint,
)
from ray.rllib.utils.test_utils import check
from ray import tune


class TestCheckpointUtils(unittest.TestCase):
    """Tests utilities helping with Checkpoint management."""

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_get_checkpoint_info_v0_1(self):
        # Create a simple (dummy) v0.1 Algorithm checkpoint.
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            # Old checkpoint-[iter] file.
            algo_state_file = os.path.join(checkpoint_dir, "checkpoint-000100")
            Path(algo_state_file).touch()

            info = get_checkpoint_info(checkpoint_dir)
            self.assertTrue(info["type"] == "Algorithm")
            self.assertTrue(str(info["checkpoint_version"]) == "0.1")
            self.assertTrue(info["checkpoint_dir"] == checkpoint_dir)
            self.assertTrue(info["state_file"] == Path(algo_state_file).name)
            self.assertTrue(info["policy_ids"] is None)

    def test_get_checkpoint_info_v1_1(self):
        for extension in ["pkl", "msgpck"]:
            # Create a simple (dummy) v1.1 Algorithm checkpoint.
            with tempfile.TemporaryDirectory() as checkpoint_dir:
                # algorithm_state.pkl
                algo_state_file = os.path.join(
                    checkpoint_dir,
                    f"algorithm_state.{extension}",
                )
                Path(algo_state_file).touch()
                # 2 policies
                pol1_dir = os.path.join(checkpoint_dir, "policies", "pol1")
                os.makedirs(pol1_dir)
                pol2_dir = os.path.join(checkpoint_dir, "policies", "pol2")
                os.makedirs(pol2_dir)
                # policy_state.pkl
                Path(os.path.join(pol1_dir, "policy_state.pkl")).touch()
                Path(os.path.join(pol2_dir, "policy_state.pkl")).touch()

                info = get_checkpoint_info(checkpoint_dir)
                self.assertTrue(info["type"] == "Algorithm")
                self.assertTrue(str(info["checkpoint_version"]) == "1.1")
                self.assertTrue(info["checkpoint_dir"] == checkpoint_dir)
                self.assertTrue(info["state_file"] == algo_state_file)
                self.assertTrue(
                    "pol1" in info["policy_ids"] and "pol2" in info["policy_ids"]
                )

    def test_get_policy_checkpoint_info_v1_1(self):
        # Create a simple (dummy) v1.0 Policy checkpoint.
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            policy_state_file = os.path.join(checkpoint_dir, "policy_state.pkl")
            Path(policy_state_file).touch()

            info = get_checkpoint_info(checkpoint_dir)
            self.assertTrue(info["type"] == "Policy")
            self.assertTrue(str(info["checkpoint_version"]) == "1.1")
            self.assertTrue(info["checkpoint_dir"] == checkpoint_dir)
            self.assertTrue(info["state_file"] == policy_state_file)
            self.assertTrue(info["policy_ids"] is None)

    def test_msgpack_checkpoint_translation(self):
        """Tests, whether a checkpoint can be translated into a msgpack-checkpoint ...

        ... and recovered back into an Algorithm, which is identical to a
        pickle-checkpoint-recovered Algorithm (given same initial config).
        """
        # Base config used for both pickle-based checkpoint and msgpack-based one.
        config = DQNConfig().environment("CartPole-v1")
        # Build algorithm object.
        algo1 = config.build()
        # Fake one result for the checkpoint saving to succeed
        algo1._last_result = {}
        # Get its state.
        pickle_state = algo1.__getstate__()
        # Create standard (pickle-based) checkpoint.
        with tempfile.TemporaryDirectory() as pickle_cp_dir:
            pickle_cp_dir = algo1.save(checkpoint_dir=pickle_cp_dir).checkpoint.path
            pickle_cp_info = get_checkpoint_info(pickle_cp_dir)
            # Now convert pickle checkpoint to msgpack using the provided
            # utility function.
            with tempfile.TemporaryDirectory() as msgpack_cp_dir:
                convert_to_msgpack_checkpoint(pickle_cp_dir, msgpack_cp_dir)
                msgpack_cp_info = get_checkpoint_info(msgpack_cp_dir)
                # Try recreating a new algorithm object from the msgpack checkpoint.
                algo2 = Algorithm.from_checkpoint(msgpack_cp_dir)
        # Get the state of the algorithm recovered from msgpack.
        msgpack_state = algo2.__getstate__()

        # Make sure JSON info files are different.
        self.assertTrue(pickle_cp_info["format"] == "cloudpickle")
        self.assertTrue(msgpack_cp_info["format"] == "msgpack")

        pickle_w = pickle_state["worker"]
        msgpack_w = msgpack_state["worker"]

        # Make sure recovered-from-pickle state is the same as recovered-from-msgpack
        # state.
        self.assertTrue(
            pickle_state["algorithm_class"] == msgpack_state["algorithm_class"]
        )
        check(pickle_state["counters"], msgpack_state["counters"])
        check(pickle_w["policy_ids"], msgpack_w["policy_ids"])
        check(pickle_w["filters"], msgpack_w["filters"])

        # Make sure the (serialized) configs match 100%. Our `check` utility
        # cannot handle comparing types/classes.
        pickle_w["policy_states"]["default_policy"]["policy_spec"][
            "config"
        ] = AlgorithmConfig._serialize_dict(
            pickle_w["policy_states"]["default_policy"]["policy_spec"]["config"]
        )
        check(pickle_w["policy_states"], msgpack_w["policy_states"])
        check(
            AlgorithmConfig._serialize_dict(pickle_state["config"]),
            AlgorithmConfig._serialize_dict(msgpack_state["config"]),
        )

        algo1.stop()
        algo2.stop()

    def test_msgpack_checkpoint_translation_multi_agent(self):
        """Tests, whether a checkpoint can be translated into a msgpack-checkpoint ...

        ... and recovered back into an Algorithm, which is identical to a
        pickle-checkpoint-recovered Algorithm (given same initial config).
        """

        # Base config used for both pickle-based checkpoint and msgpack-based one.
        def mapping_fn(aid, episode, worker, **kwargs):
            return "pol" + str(aid)

        tune.register_env("ma", lambda _: MultiAgentCartPole(config={"num_agents": 3}))

        config = (
            DQNConfig()
            .environment("ma")
            .multi_agent(
                policies=["pol0", "pol1", "pol2"],
                policy_mapping_fn=mapping_fn,
                policies_to_train={"pol0", "pol1"},
            )
        )
        # Build algorithm object.
        algo1 = config.build()
        # Fake one result
        algo1._last_result = {}
        # Get its state.
        pickle_state = algo1.__getstate__()
        # Create standard (pickle-based) checkpoint.
        with tempfile.TemporaryDirectory() as pickle_cp_dir:
            pickle_cp_dir = algo1.save(checkpoint_dir=pickle_cp_dir).checkpoint.path
            pickle_cp_info = get_checkpoint_info(pickle_cp_dir)
            # Now convert pickle checkpoint to msgpack using the provided
            # utility function.
            with tempfile.TemporaryDirectory() as msgpack_cp_dir:
                convert_to_msgpack_checkpoint(pickle_cp_dir, msgpack_cp_dir)
                msgpack_cp_info = get_checkpoint_info(msgpack_cp_dir)
                # Try recreating a new algorithm object from the msgpack checkpoint.
                algo2 = Algorithm.from_checkpoint(
                    msgpack_cp_dir,
                    # Since we have a multi-agent setup, we must provide the current
                    # policy mapping fn and list of policies to train.
                    policy_mapping_fn=mapping_fn,
                    policies_to_train=["pol0", "pol1"],
                )
        # Get the state of the algorithm recovered from msgpack.
        msgpack_state = algo2.__getstate__()

        # Make sure JSON info files are different.
        self.assertTrue(pickle_cp_info["format"] == "cloudpickle")
        self.assertTrue(msgpack_cp_info["format"] == "msgpack")

        pickle_w = pickle_state["worker"]
        msgpack_w = msgpack_state["worker"]

        # Make sure recovered-from-pickle state is the same as recovered-from-msgpack
        # state.
        self.assertTrue(
            pickle_state["algorithm_class"] == msgpack_state["algorithm_class"]
        )
        check(pickle_state["counters"], msgpack_state["counters"])
        check(pickle_w["policy_ids"], msgpack_w["policy_ids"])
        check(pickle_w["filters"], msgpack_w["filters"])

        # Make sure the (serialized) configs match 100%. Our `check` utility
        # cannot handle comparing types/classes.
        for p in ["pol0", "pol1", "pol2"]:
            pickle_w["policy_states"][p]["policy_spec"][
                "config"
            ] = AlgorithmConfig._serialize_dict(
                pickle_w["policy_states"][p]["policy_spec"]["config"]
            )
        check(pickle_w["policy_states"], msgpack_w["policy_states"])
        # Make sure the (serialized) configs match 100%. Our `check` utility cannot
        # handle comparing types/classes.
        # The only exception is the `policies` field as it might have gotten
        # regenerated from a set, thus the order of PIDs might be different.
        p = AlgorithmConfig._serialize_dict(pickle_state["config"])
        p_pols = p.pop("policies")
        m = AlgorithmConfig._serialize_dict(msgpack_state["config"])
        m_pols = m.pop("policies")
        check(p, m)
        # Compare sets of policyIDs here.
        self.assertTrue(set(p_pols) == set(m_pols))

        algo1.stop()
        algo2.stop()

    def test_msgpack_policy_checkpoint_translation(self):
        """Tests, whether a Policy checkpoint can be translated into msgpack ...

        ... and recovered back into a Policy, which is identical to a
        pickle-checkpoint-recovered Policy (given same initial config).
        """
        # Base config used for both pickle-based checkpoint and msgpack-based one.
        config = PPOConfig().environment("CartPole-v1")
        # Build algorithm/policy objects.
        algo1 = config.build()
        pol1 = algo1.get_policy()
        # Get its state.
        pickle_state = pol1.get_state()

        # Create standard (pickle-based) checkpoint.
        with tempfile.TemporaryDirectory() as pickle_cp_dir:
            pol1.export_checkpoint(pickle_cp_dir)
            # Now convert pickle checkpoint to msgpack using the provided
            # utility function.
            with tempfile.TemporaryDirectory() as msgpack_cp_dir:
                convert_to_msgpack_policy_checkpoint(pickle_cp_dir, msgpack_cp_dir)
                msgpack_cp_info = get_checkpoint_info(msgpack_cp_dir)
                self.assertTrue(msgpack_cp_info["type"] == "Policy")
                self.assertTrue(msgpack_cp_info["format"] == "msgpack")
                self.assertTrue(msgpack_cp_info["policy_ids"] is None)
                # Try recreating a new policy object from the msgpack checkpoint.
                pol2 = Policy.from_checkpoint(msgpack_cp_dir)
        # Get the state of the policy recovered from msgpack.
        msgpack_state = pol2.get_state()

        # Make sure the states? match 100%. Our `check` utility
        # cannot handle comparing types/classes, so we'll have to serialize the
        # pickle'd config (which contains types, rather than class strings).
        pickle_state["policy_spec"]["config"] = AlgorithmConfig._serialize_dict(
            pickle_state["policy_spec"]["config"]
        )
        check(pickle_state, msgpack_state)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
