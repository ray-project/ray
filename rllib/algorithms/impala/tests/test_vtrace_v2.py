import unittest
import numpy as np

from gymnasium.spaces import Box, Discrete

from ray.rllib.algorithms.impala.tf.vtrace_tf_v2 import (
    vtrace_tf2,
    make_time_major,
)
from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import vtrace_torch
from ray.rllib.algorithms.impala.tests.test_vtrace import (
    _ground_truth_vtrace_calculation,
)
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check

tf1, tf, _ = try_import_tf()
tf1.enable_eager_execution()


def flatten_batch_and_time_dim(t):
    if not isinstance(t, tf.Tensor):
        t = tf.convert_to_tensor(t, dtype=tf.float32)
    new_shape = [-1] + t.shape[2:].as_list()
    return tf.reshape(t, new_shape)


class TestVtraceRLModule(unittest.TestCase):
    """Tests V-trace-v2 against ground truth data calculated.

    There is a ground truth implementation that we used to test our original
    implementation against. This test checks that the new implementation still
    matches the ground truth test from our first implementation of V-Trace.
    """

    @classmethod
    def setUpClass(cls):
        """Sets up inputs for V-Trace and calculate ground truth.

        We use tf operations here to compile the inputs but convert to numpy arrays to
        calculate the ground truth (and the other v-trace outputs in the
        framework-specific tests).
        """

        # we can test against any trajectory length or batch size and it won't matter
        trajectory_len = 5
        batch_size = 10

        action_space = Discrete(10)
        action_logit_space = Box(-1.0, 1.0, (action_space.n,), np.float32)
        behavior_action_logits = tf.convert_to_tensor(
            np.array([action_logit_space.sample()]), dtype=tf.float32
        )
        target_action_logits = tf.convert_to_tensor(
            np.array([action_logit_space.sample()]), dtype=tf.float32
        )
        behavior_dist = Categorical(inputs=behavior_action_logits)
        target_dist = Categorical(inputs=target_action_logits)
        dummy_action_batch = [
            [
                tf.convert_to_tensor([action_space.sample()], dtype=tf.int8)
                for _ in range(trajectory_len)
            ]
            for _ in range(batch_size)
        ]

        behavior_log_probs = tf.stack(
            tf.nest.map_structure(
                lambda v: tf.squeeze(behavior_dist.logp(v)), dummy_action_batch
            )
        )
        target_log_probs = tf.stack(
            tf.nest.map_structure(
                lambda v: tf.squeeze(target_dist.logp(v)), dummy_action_batch
            )
        )

        value_fn_space_w_time = Box(-1.0, 1.0, (batch_size, trajectory_len), np.float32)
        value_fn_space = Box(-1.0, 1.0, (batch_size,), np.float32)

        # using randomly sampled values in lieu of actual values sampled from a value fn
        values = value_fn_space_w_time.sample()
        # this is supposed to be the value function at the last timestep of each
        # trajectory in the batch. In IMPALA its bootstrapped at training time
        cls.bootstrap_value = np.array(value_fn_space.sample() + 1.0)

        # discount factor used at all of the timesteps
        discounts = [0.9 for _ in range(trajectory_len * batch_size)]
        rewards = value_fn_space_w_time.sample()
        cls.clip_rho_threshold = 3.7
        cls.clip_pg_rho_threshold = 2.2

        # convert to time major dimension
        cls.behavior_log_probs_time_major = make_time_major(
            flatten_batch_and_time_dim(behavior_log_probs),
            trajectory_len=trajectory_len,
        ).numpy()
        cls.target_log_probs_time_major = make_time_major(
            flatten_batch_and_time_dim(target_log_probs), trajectory_len=trajectory_len
        ).numpy()
        cls.discounts_time_major = make_time_major(
            flatten_batch_and_time_dim(discounts), trajectory_len=trajectory_len
        ).numpy()
        cls.rewards_time_major = make_time_major(
            flatten_batch_and_time_dim(rewards), trajectory_len=trajectory_len
        ).numpy()
        cls.values_time_major = make_time_major(
            flatten_batch_and_time_dim(values), trajectory_len=trajectory_len
        ).numpy()

        log_rhos = cls.target_log_probs_time_major - cls.behavior_log_probs_time_major

        cls.ground_truth_v = _ground_truth_vtrace_calculation(
            discounts=cls.discounts_time_major,
            log_rhos=log_rhos,
            rewards=cls.rewards_time_major,
            values=cls.values_time_major,
            bootstrap_value=cls.bootstrap_value,
            clip_rho_threshold=cls.clip_rho_threshold,
            clip_pg_rho_threshold=cls.clip_pg_rho_threshold,
        )

    def test_vtrace_tf2(self):
        output_tf2_vtrace = vtrace_tf2(
            behaviour_action_log_probs=tf.convert_to_tensor(
                self.behavior_log_probs_time_major
            ),
            target_action_log_probs=tf.convert_to_tensor(
                self.target_log_probs_time_major
            ),
            discounts=tf.convert_to_tensor(self.discounts_time_major),
            rewards=tf.convert_to_tensor(self.rewards_time_major),
            values=tf.convert_to_tensor(self.values_time_major),
            bootstrap_value=tf.convert_to_tensor(self.bootstrap_value),
            clip_rho_threshold=self.clip_rho_threshold,
            clip_pg_rho_threshold=self.clip_pg_rho_threshold,
        )
        check(output_tf2_vtrace, self.ground_truth_v)

    def test_vtrace_torch(self):
        output_torch_vtrace = vtrace_torch(
            behaviour_action_log_probs=convert_to_torch_tensor(
                self.behavior_log_probs_time_major
            ),
            target_action_log_probs=convert_to_torch_tensor(
                self.target_log_probs_time_major
            ),
            discounts=convert_to_torch_tensor(self.discounts_time_major),
            rewards=convert_to_torch_tensor(self.rewards_time_major),
            values=convert_to_torch_tensor(self.values_time_major),
            bootstrap_value=convert_to_torch_tensor(self.bootstrap_value),
            clip_rho_threshold=self.clip_rho_threshold,
            clip_pg_rho_threshold=self.clip_pg_rho_threshold,
        )
        check(output_torch_vtrace, self.ground_truth_v)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
