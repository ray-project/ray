# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for V-trace.

For details and theory see:

"IMPALA: Scalable Distributed Deep-RL with
Importance Weighted Actor-Learner Architectures"
by Espeholt, Soyer, Munos et al.
"""

from gymnasium.spaces import Box
import numpy as np
import unittest

from ray.rllib.algorithms.impala import vtrace_tf as vtrace_tf
from ray.rllib.algorithms.impala import vtrace_torch as vtrace_torch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import softmax
from ray.rllib.utils.test_utils import check, framework_iterator

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


def _ground_truth_calculation(
    vtrace,
    discounts,
    log_rhos,
    rewards,
    values,
    bootstrap_value,
    clip_rho_threshold,
    clip_pg_rho_threshold,
):
    """Calculates the ground truth for V-trace in Python/Numpy."""
    vs = []
    seq_len = len(discounts)
    rhos = np.exp(log_rhos)
    cs = np.minimum(rhos, 1.0)
    clipped_rhos = rhos
    if clip_rho_threshold:
        clipped_rhos = np.minimum(rhos, clip_rho_threshold)
    clipped_pg_rhos = rhos
    if clip_pg_rho_threshold:
        clipped_pg_rhos = np.minimum(rhos, clip_pg_rho_threshold)

    # This is a very inefficient way to calculate the V-trace ground truth.
    # We calculate it this way because it is close to the mathematical notation
    # of
    # V-trace.
    # v_s = V(x_s)
    #       + \sum^{T-1}_{t=s} \gamma^{t-s}
    #         * \prod_{i=s}^{t-1} c_i
    #         * \rho_t (r_t + \gamma V(x_{t+1}) - V(x_t))
    # Note that when we take the product over c_i, we write `s:t` as the
    # notation
    # of the paper is inclusive of the `t-1`, but Python is exclusive.
    # Also note that np.prod([]) == 1.
    values_t_plus_1 = np.concatenate([values[1:], bootstrap_value[None, :]], axis=0)
    for s in range(seq_len):
        v_s = np.copy(values[s])  # Very important copy.
        for t in range(s, seq_len):
            v_s += (
                np.prod(discounts[s:t], axis=0)
                * np.prod(cs[s:t], axis=0)
                * clipped_rhos[t]
                * (rewards[t] + discounts[t] * values_t_plus_1[t] - values[t])
            )
        vs.append(v_s)
    vs = np.stack(vs, axis=0)
    pg_advantages = clipped_pg_rhos * (
        rewards
        + discounts * np.concatenate([vs[1:], bootstrap_value[None, :]], axis=0)
        - values
    )

    return vtrace.VTraceReturns(vs=vs, pg_advantages=pg_advantages)


class LogProbsFromLogitsAndActionsTest(unittest.TestCase):
    def test_log_probs_from_logits_and_actions(self):
        """Tests log_probs_from_logits_and_actions."""
        seq_len = 7
        num_actions = 3
        batch_size = 4

        for fw, sess in framework_iterator(frameworks=("torch", "tf"), session=True):
            vtrace = vtrace_tf if fw != "torch" else vtrace_torch
            policy_logits = Box(
                -1.0, 1.0, (seq_len, batch_size, num_actions), np.float32
            ).sample()
            actions = np.random.randint(
                0, num_actions - 1, size=(seq_len, batch_size), dtype=np.int32
            )

            if fw == "torch":
                action_log_probs_tensor = vtrace.log_probs_from_logits_and_actions(
                    torch.from_numpy(policy_logits), torch.from_numpy(actions)
                )
            else:
                action_log_probs_tensor = vtrace.log_probs_from_logits_and_actions(
                    policy_logits, actions
                )

            # Ground Truth
            # Using broadcasting to create a mask that indexes action logits
            action_index_mask = actions[..., None] == np.arange(num_actions)

            def index_with_mask(array, mask):
                return array[mask].reshape(*array.shape[:-1])

            # Note: Normally log(softmax) is not a good idea because it's not
            # numerically stable. However, in this test we have well-behaved
            # values.
            ground_truth_v = index_with_mask(
                np.log(softmax(policy_logits)), action_index_mask
            )

            if sess:
                action_log_probs_tensor = sess.run(action_log_probs_tensor)
            check(action_log_probs_tensor, ground_truth_v)


class VtraceTest(unittest.TestCase):
    def test_vtrace(self):
        """Tests V-trace against ground truth data calculated in python."""
        seq_len = 5
        batch_size = 10

        # Create log_rhos such that rho will span from near-zero to above the
        # clipping thresholds. In particular, calculate log_rhos in
        # [-2.5, 2.5),
        # so that rho is in approx [0.08, 12.2).
        space_w_time = Box(-1.0, 1.0, (seq_len, batch_size), np.float32)
        space_only_batch = Box(-1.0, 1.0, (batch_size,), np.float32)
        log_rhos = space_w_time.sample() / (batch_size * seq_len)
        log_rhos = 5 * (log_rhos - 0.5)  # [0.0, 1.0) -> [-2.5, 2.5).
        values = {
            "log_rhos": log_rhos,
            # T, B where B_i: [0.9 / (i+1)] * T
            "discounts": np.array(
                [[0.9 / (b + 1) for b in range(batch_size)] for _ in range(seq_len)]
            ),
            "rewards": space_w_time.sample(),
            "values": space_w_time.sample() / batch_size,
            "bootstrap_value": space_only_batch.sample() + 1.0,
            "clip_rho_threshold": 3.7,
            "clip_pg_rho_threshold": 2.2,
        }

        for fw, sess in framework_iterator(frameworks=("torch", "tf"), session=True):
            vtrace = vtrace_tf if fw != "torch" else vtrace_torch
            output = vtrace.from_importance_weights(**values)
            if sess:
                output = sess.run(output)

            ground_truth_v = _ground_truth_calculation(vtrace, **values)
            check(output, ground_truth_v)

    def test_vtrace_from_logits(self):
        """Tests V-trace calculated from logits."""
        seq_len = 5
        batch_size = 15
        num_actions = 3
        clip_rho_threshold = None  # No clipping.
        clip_pg_rho_threshold = None  # No clipping.
        space = Box(-1.0, 1.0, (seq_len, batch_size, num_actions))
        action_space = Box(
            0,
            num_actions - 1,
            (
                seq_len,
                batch_size,
            ),
            dtype=np.int32,
        )
        space_w_time = Box(
            -1.0,
            1.0,
            (
                seq_len,
                batch_size,
            ),
        )
        space_only_batch = Box(-1.0, 1.0, (batch_size,))

        for fw, sess in framework_iterator(frameworks=("torch", "tf"), session=True):
            vtrace = vtrace_tf if fw != "torch" else vtrace_torch

            if fw == "tf":
                # Intentionally leaving shapes unspecified to test if V-trace
                # can deal with that.
                inputs_ = {
                    # T, B, NUM_ACTIONS
                    "behaviour_policy_logits": tf1.placeholder(
                        dtype=tf.float32, shape=[None, None, None]
                    ),
                    # T, B, NUM_ACTIONS
                    "target_policy_logits": tf1.placeholder(
                        dtype=tf.float32, shape=[None, None, None]
                    ),
                    "actions": tf1.placeholder(dtype=tf.int32, shape=[None, None]),
                    "discounts": tf1.placeholder(dtype=tf.float32, shape=[None, None]),
                    "rewards": tf1.placeholder(dtype=tf.float32, shape=[None, None]),
                    "values": tf1.placeholder(dtype=tf.float32, shape=[None, None]),
                    "bootstrap_value": tf1.placeholder(dtype=tf.float32, shape=[None]),
                }
            else:
                inputs_ = {
                    # T, B, NUM_ACTIONS
                    "behaviour_policy_logits": space.sample(),
                    # T, B, NUM_ACTIONS
                    "target_policy_logits": space.sample(),
                    "actions": action_space.sample(),
                    "discounts": space_w_time.sample(),
                    "rewards": space_w_time.sample(),
                    "values": space_w_time.sample(),
                    "bootstrap_value": space_only_batch.sample(),
                }
            from_logits_output = vtrace.from_logits(
                clip_rho_threshold=clip_rho_threshold,
                clip_pg_rho_threshold=clip_pg_rho_threshold,
                **inputs_
            )

            if fw != "torch":
                target_log_probs = vtrace.log_probs_from_logits_and_actions(
                    inputs_["target_policy_logits"], inputs_["actions"]
                )
                behaviour_log_probs = vtrace.log_probs_from_logits_and_actions(
                    inputs_["behaviour_policy_logits"], inputs_["actions"]
                )
            else:
                target_log_probs = vtrace.log_probs_from_logits_and_actions(
                    torch.from_numpy(inputs_["target_policy_logits"]),
                    torch.from_numpy(inputs_["actions"]),
                )
                behaviour_log_probs = vtrace.log_probs_from_logits_and_actions(
                    torch.from_numpy(inputs_["behaviour_policy_logits"]),
                    torch.from_numpy(inputs_["actions"]),
                )
            log_rhos = target_log_probs - behaviour_log_probs
            ground_truth = (log_rhos, behaviour_log_probs, target_log_probs)

            if sess:
                values = {
                    "behaviour_policy_logits": space.sample(),
                    "target_policy_logits": space.sample(),
                    "actions": action_space.sample(),
                    "discounts": space_w_time.sample(),
                    "rewards": space_w_time.sample(),
                    "values": space_w_time.sample() / batch_size,
                    "bootstrap_value": space_only_batch.sample() + 1.0,
                }
                feed_dict = {inputs_[k]: v for k, v in values.items()}
                from_logits_output = sess.run(from_logits_output, feed_dict=feed_dict)
                log_rhos, behaviour_log_probs, target_log_probs = sess.run(
                    ground_truth, feed_dict=feed_dict
                )

                # Calculate V-trace using the ground truth logits.
                from_iw = vtrace.from_importance_weights(
                    log_rhos=log_rhos,
                    discounts=values["discounts"],
                    rewards=values["rewards"],
                    values=values["values"],
                    bootstrap_value=values["bootstrap_value"],
                    clip_rho_threshold=clip_rho_threshold,
                    clip_pg_rho_threshold=clip_pg_rho_threshold,
                )
                from_iw = sess.run(from_iw)
            else:
                from_iw = vtrace.from_importance_weights(
                    log_rhos=log_rhos,
                    discounts=inputs_["discounts"],
                    rewards=inputs_["rewards"],
                    values=inputs_["values"],
                    bootstrap_value=inputs_["bootstrap_value"],
                    clip_rho_threshold=clip_rho_threshold,
                    clip_pg_rho_threshold=clip_pg_rho_threshold,
                )

            check(from_iw.vs, from_logits_output.vs)
            check(from_iw.pg_advantages, from_logits_output.pg_advantages)
            check(behaviour_log_probs, from_logits_output.behaviour_action_log_probs)
            check(target_log_probs, from_logits_output.target_action_log_probs)
            check(log_rhos, from_logits_output.log_rhos)

    def test_higher_rank_inputs_for_importance_weights(self):
        """Checks support for additional dimensions in inputs."""
        for fw in framework_iterator(frameworks=("torch", "tf"), session=True):
            vtrace = vtrace_tf if fw != "torch" else vtrace_torch
            if fw == "tf":
                inputs_ = {
                    "log_rhos": tf1.placeholder(
                        dtype=tf.float32, shape=[None, None, 1]
                    ),
                    "discounts": tf1.placeholder(
                        dtype=tf.float32, shape=[None, None, 1]
                    ),
                    "rewards": tf1.placeholder(
                        dtype=tf.float32, shape=[None, None, 42]
                    ),
                    "values": tf1.placeholder(dtype=tf.float32, shape=[None, None, 42]),
                    "bootstrap_value": tf1.placeholder(
                        dtype=tf.float32, shape=[None, 42]
                    ),
                }
            else:
                inputs_ = {
                    "log_rhos": Box(-1.0, 1.0, (8, 10, 1)).sample(),
                    "discounts": Box(-1.0, 1.0, (8, 10, 1)).sample(),
                    "rewards": Box(-1.0, 1.0, (8, 10, 42)).sample(),
                    "values": Box(-1.0, 1.0, (8, 10, 42)).sample(),
                    "bootstrap_value": Box(-1.0, 1.0, (10, 42)).sample(),
                }
            output = vtrace.from_importance_weights(**inputs_)
            check(int(output.vs.shape[-1]), 42)

    def test_inconsistent_rank_inputs_for_importance_weights(self):
        """Test one of many possible errors in shape of inputs."""
        for fw in framework_iterator(frameworks=("torch", "tf"), session=True):
            vtrace = vtrace_tf if fw != "torch" else vtrace_torch
            if fw == "tf":
                inputs_ = {
                    "log_rhos": tf1.placeholder(
                        dtype=tf.float32, shape=[None, None, 1]
                    ),
                    "discounts": tf1.placeholder(
                        dtype=tf.float32, shape=[None, None, 1]
                    ),
                    "rewards": tf1.placeholder(
                        dtype=tf.float32, shape=[None, None, 42]
                    ),
                    "values": tf1.placeholder(dtype=tf.float32, shape=[None, None, 42]),
                    # Should be [None, 42].
                    "bootstrap_value": tf1.placeholder(dtype=tf.float32, shape=[None]),
                }
            else:
                inputs_ = {
                    "log_rhos": Box(-1.0, 1.0, (7, 15, 1)).sample(),
                    "discounts": Box(-1.0, 1.0, (7, 15, 1)).sample(),
                    "rewards": Box(-1.0, 1.0, (7, 15, 42)).sample(),
                    "values": Box(-1.0, 1.0, (7, 15, 42)).sample(),
                    # Should be [15, 42].
                    "bootstrap_value": Box(-1.0, 1.0, (7,)).sample(),
                }
            with self.assertRaisesRegex(
                (ValueError, AssertionError), "must have rank 2"
            ):
                vtrace.from_importance_weights(**inputs_)


if __name__ == "__main__":
    tf.test.main()
