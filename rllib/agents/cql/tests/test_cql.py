import numpy as np
from pathlib import Path
import os
import unittest

import ray
import ray.rllib.agents.cql as cql
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestCQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_cql_compilation(self):
        """Test whether a CQLTrainer can be built with all frameworks."""

        # Learns from a historic-data file.
        # To generate this data, first run:
        # $ ./train.py --run=SAC --env=Pendulum-v0 \
        #   --stop='{"timesteps_total": 50000}' \
        #   --config='{"output": "/tmp/out"}'
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/pendulum/small.json")
        print("data_file={} exists={}".format(data_file,
                                              os.path.isfile(data_file)))

        config = cql.CQL_DEFAULT_CONFIG.copy()
        config["env"] = "Pendulum-v0"
        config["input"] = [data_file]

        config["num_workers"] = 0  # Run locally.
        config["twin_q"] = True
        config["clip_actions"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        config["rollout_fragment_length"] = 1
        config["train_batch_size"] = 10

        # Switch on off-policy evaluation.
        config["input_evaluation"] = ["is"]

        num_iterations = 2

        # Test for tf/torch frameworks.
        for fw in framework_iterator(config):
            trainer = cql.CQLTrainer(config=config)
            for i in range(num_iterations):
                print(trainer.train())

            check_compute_single_action(trainer)

            # Get policy and model.
            pol = trainer.get_policy()
            cql_model = pol.model
            if fw == "tf":
                pol.get_session().__enter__()

            # Example on how to do evaluation on the trained Trainer
            # using the data from CQL's global replay buffer.
            # Get a sample (MultiAgentBatch -> SampleBatch).
            from ray.rllib.agents.cql.cql import replay_buffer
            batch = replay_buffer.replay().policy_batches["default_policy"]

            if fw == "torch":
                obs = torch.from_numpy(batch["obs"])
            else:
                obs = batch["obs"]
                batch["actions"] = batch["actions"].astype(np.float32)

            # Pass the observations through our model to get the
            # features, which then to pass through the Q-head.
            model_out, _ = cql_model({"obs": obs})
            # The estimated Q-values from the (historic) actions in the batch.
            if fw == "torch":
                q_values_old = cql_model.get_q_values(
                    model_out, torch.from_numpy(batch["actions"]))
            else:
                q_values_old = cql_model.get_q_values(
                    tf.convert_to_tensor(model_out), batch["actions"])

            # The estimated Q-values for the new actions computed
            # by our trainer policy.
            actions_new = pol.compute_actions_from_input_dict({"obs": obs})[0]
            if fw == "torch":
                q_values_new = cql_model.get_q_values(
                    model_out, torch.from_numpy(actions_new))
            else:
                q_values_new = cql_model.get_q_values(model_out, actions_new)

            if fw == "tf":
                q_values_old, q_values_new = pol.get_session().run(
                    [q_values_old, q_values_new])

            print(f"Q-val batch={q_values_old}")
            print(f"Q-val policy={q_values_new}")

            if fw == "tf":
                pol.get_session().__exit__(None, None, None)

            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
