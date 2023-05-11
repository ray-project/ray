import unittest
import numpy as np

from gymnasium.spaces import Box, Discrete

from ray.rllib.algorithms.impala.tf.vtrace_tf_v2 import (
    vtrace_tf2,
    make_time_major,
)
from ray.rllib.algorithms.impala.tests.test_vtrace import (
    _ground_truth_vtrace_calculation,
)
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
    def test_vtrace_tf2(self):
        """Tests V-trace-v2 against ground truth data calculated.

        The v2 vtrace is optimized for the least amount of flops. The math
        in the implementation looks nothing like the math in the paper.

        There is a ground truth implementation that we used to test our original impl
        against. This test checks that the new implementation still matches the gt.

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
        bootstrap_value = tf.convert_to_tensor(
            value_fn_space.sample() + 1.0, dtype=tf.float32
        )

        # discount factor used at all of the timesteps
        discounts = [0.9 for _ in range(trajectory_len * batch_size)]
        rewards = value_fn_space_w_time.sample()
        clip_rho_threshold = 3.7
        clip_pg_rho_threshold = 2.2

        # convert to time major dimension
        behavior_log_probs_time_major = make_time_major(
            flatten_batch_and_time_dim(behavior_log_probs),
            trajectory_len=trajectory_len,
        )
        target_log_probs_time_major = make_time_major(
            flatten_batch_and_time_dim(target_log_probs), trajectory_len=trajectory_len
        )
        discounts_time_major = make_time_major(
            flatten_batch_and_time_dim(discounts), trajectory_len=trajectory_len
        )
        rewards_time_major = make_time_major(
            flatten_batch_and_time_dim(rewards), trajectory_len=trajectory_len
        )
        values_time_major = make_time_major(
            flatten_batch_and_time_dim(values), trajectory_len=trajectory_len
        )

        output_tf2_vtrace = vtrace_tf2(
            behaviour_action_log_probs=behavior_log_probs_time_major,
            target_action_log_probs=target_log_probs_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_rho_threshold=clip_rho_threshold,
            clip_pg_rho_threshold=clip_pg_rho_threshold,
        )

        log_rhos = target_log_probs_time_major - behavior_log_probs_time_major

        ground_truth_v = _ground_truth_vtrace_calculation(
            discounts=discounts_time_major.numpy(),
            log_rhos=log_rhos.numpy(),
            rewards=rewards_time_major.numpy(),
            values=values_time_major.numpy(),
            bootstrap_value=bootstrap_value.numpy(),
            clip_rho_threshold=clip_rho_threshold,
            clip_pg_rho_threshold=clip_pg_rho_threshold,
        )

        check(output_tf2_vtrace, ground_truth_v)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
