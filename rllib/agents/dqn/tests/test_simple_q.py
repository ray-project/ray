import numpy as np
import unittest

import ray
import ray.rllib.agents.dqn as dqn
from ray.rllib.agents.dqn.simple_q_tf_policy import build_q_losses as loss_tf
from ray.rllib.agents.dqn.simple_q_torch_policy import build_q_losses as \
    loss_torch
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import fc, one_hot, huber_loss
from ray.rllib.utils.test_utils import check, check_compute_single_action, \
    framework_iterator

tf1, tf, tfv = try_import_tf()


class TestSimpleQ(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_simple_q_compilation(self):
        """Test whether a SimpleQTrainer can be built on all frameworks."""
        config = dqn.SIMPLE_Q_DEFAULT_CONFIG.copy()
        # Run locally.
        config["num_workers"] = 0
        # Test with compression.
        config["compress_observations"] = True

        num_iterations = 2

        for _ in framework_iterator(config):
            trainer = dqn.SimpleQTrainer(config=config, env="CartPole-v0")
            rw = trainer.workers.local_worker()
            for i in range(num_iterations):
                sb = rw.sample()
                assert sb.count == config["rollout_fragment_length"]
                results = trainer.train()
                print(results)

            check_compute_single_action(trainer)

    def test_simple_q_loss_function(self):
        """Tests the Simple-Q loss function results on all frameworks."""
        config = dqn.SIMPLE_Q_DEFAULT_CONFIG.copy()
        # Run locally.
        config["num_workers"] = 0
        # Use very simple net (layer0=10 nodes, q-layer=2 nodes (2 actions)).
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = "linear"

        for fw in framework_iterator(config):
            # Generate Trainer and get its default Policy object.
            trainer = dqn.SimpleQTrainer(config=config, env="CartPole-v0")
            policy = trainer.get_policy()
            # Batch of size=2.
            input_ = SampleBatch({
                SampleBatch.CUR_OBS: np.random.random(size=(2, 4)),
                SampleBatch.ACTIONS: np.array([0, 1]),
                SampleBatch.REWARDS: np.array([0.4, -1.23]),
                SampleBatch.DONES: np.array([False, False]),
                SampleBatch.NEXT_OBS: np.random.random(size=(2, 4)),
                SampleBatch.EPS_ID: np.array([1234, 1234]),
                SampleBatch.AGENT_INDEX: np.array([0, 0]),
                SampleBatch.ACTION_LOGP: np.array([-0.1, -0.1]),
                SampleBatch.ACTION_DIST_INPUTS: np.array([[0.1, 0.2],
                                                          [-0.1, -0.2]]),
                SampleBatch.ACTION_PROB: np.array([0.1, 0.2]),
                "q_values": np.array([[0.1, 0.2], [0.2, 0.1]]),
            })
            # Get model vars for computing expected model outs (q-vals).
            # 0=layer-kernel; 1=layer-bias; 2=q-val-kernel; 3=q-val-bias
            vars = policy.get_weights()
            if isinstance(vars, dict):
                vars = list(vars.values())

            vars_t = policy.target_model.variables()
            if fw == "tf":
                vars_t = policy.get_session().run(vars_t)

            # Q(s,a) outputs.
            q_t = np.sum(
                one_hot(input_[SampleBatch.ACTIONS], 2) * fc(
                    fc(input_[SampleBatch.CUR_OBS],
                       vars[0 if fw != "torch" else 2],
                       vars[1 if fw != "torch" else 3],
                       framework=fw),
                    vars[2 if fw != "torch" else 0],
                    vars[3 if fw != "torch" else 1],
                    framework=fw), 1)
            # max[a'](Qtarget(s',a')) outputs.
            q_target_tp1 = np.max(
                fc(fc(
                    input_[SampleBatch.NEXT_OBS],
                    vars_t[0 if fw != "torch" else 2],
                    vars_t[1 if fw != "torch" else 3],
                    framework=fw),
                   vars_t[2 if fw != "torch" else 0],
                   vars_t[3 if fw != "torch" else 1],
                   framework=fw), 1)
            # TD-errors (Bellman equation).
            td_error = q_t - config["gamma"] * input_[SampleBatch.REWARDS] + \
                q_target_tp1
            # Huber/Square loss on TD-error.
            expected_loss = huber_loss(td_error).mean()

            if fw == "torch":
                input_ = policy._lazy_tensor_dict(input_)
            # Get actual out and compare.
            if fw == "tf":
                out = policy.get_session().run(
                    policy._loss,
                    feed_dict=policy._get_loss_inputs_dict(
                        input_, shuffle=False))
            else:
                out = (loss_torch if fw == "torch" else
                       loss_tf)(policy, policy.model, None, input_)
            check(out, expected_loss, decimals=1)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
