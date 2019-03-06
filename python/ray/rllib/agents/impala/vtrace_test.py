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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from absl.testing import parameterized
import numpy as np
import tensorflow as tf
import vtrace


def _shaped_arange(*shape):
    """Runs np.arange, converts to float and reshapes."""
    return np.arange(np.prod(shape), dtype=np.float32).reshape(*shape)


def _softmax(logits):
    """Applies softmax non-linearity on inputs."""
    return np.exp(logits) / np.sum(np.exp(logits), axis=-1, keepdims=True)


def _ground_truth_calculation(discounts, log_rhos, rewards, values,
                              bootstrap_value, clip_rho_threshold,
                              clip_pg_rho_threshold):
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
    values_t_plus_1 = np.concatenate(
        [values, bootstrap_value[None, :]], axis=0)
    for s in range(seq_len):
        v_s = np.copy(values[s])  # Very important copy.
        for t in range(s, seq_len):
            v_s += (np.prod(discounts[s:t], axis=0) * np.prod(cs[s:t], axis=0)
                    * clipped_rhos[t] * (rewards[t] + discounts[t] *
                                         values_t_plus_1[t + 1] - values[t]))
        vs.append(v_s)
    vs = np.stack(vs, axis=0)
    pg_advantages = (clipped_pg_rhos * (rewards + discounts * np.concatenate(
        [vs[1:], bootstrap_value[None, :]], axis=0) - values))

    return vtrace.VTraceReturns(vs=vs, pg_advantages=pg_advantages)


class LogProbsFromLogitsAndActionsTest(tf.test.TestCase,
                                       parameterized.TestCase):
    @parameterized.named_parameters(('Batch1', 1), ('Batch2', 2))
    def test_log_probs_from_logits_and_actions(self, batch_size):
        """Tests log_probs_from_logits_and_actions."""
        seq_len = 7
        num_actions = 3

        policy_logits = _shaped_arange(seq_len, batch_size, num_actions) + 10
        actions = np.random.randint(
            0, num_actions - 1, size=(seq_len, batch_size), dtype=np.int32)

        action_log_probs_tensor = vtrace.log_probs_from_logits_and_actions(
            policy_logits, actions)

        # Ground Truth
        # Using broadcasting to create a mask that indexes action logits
        action_index_mask = actions[..., None] == np.arange(num_actions)

        def index_with_mask(array, mask):
            return array[mask].reshape(*array.shape[:-1])

        # Note: Normally log(softmax) is not a good idea because it's not
        # numerically stable. However, in this test we have well-behaved
        # values.
        ground_truth_v = index_with_mask(
            np.log(_softmax(policy_logits)), action_index_mask)

        with self.test_session() as session:
            self.assertAllClose(ground_truth_v,
                                session.run(action_log_probs_tensor))


class VtraceTest(tf.test.TestCase, parameterized.TestCase):
    @parameterized.named_parameters(('Batch1', 1), ('Batch5', 5))
    def test_vtrace(self, batch_size):
        """Tests V-trace against ground truth data calculated in python."""
        seq_len = 5

        # Create log_rhos such that rho will span from near-zero to above the
        # clipping thresholds. In particular, calculate log_rhos in
        # [-2.5, 2.5),
        # so that rho is in approx [0.08, 12.2).
        log_rhos = _shaped_arange(seq_len, batch_size) / (batch_size * seq_len)
        log_rhos = 5 * (log_rhos - 0.5)  # [0.0, 1.0) -> [-2.5, 2.5).
        values = {
            'log_rhos': log_rhos,
            # T, B where B_i: [0.9 / (i+1)] * T
            'discounts': np.array([[0.9 / (b + 1) for b in range(batch_size)]
                                   for _ in range(seq_len)]),
            'rewards': _shaped_arange(seq_len, batch_size),
            'values': _shaped_arange(seq_len, batch_size) / batch_size,
            'bootstrap_value': _shaped_arange(batch_size) + 1.0,
            'clip_rho_threshold': 3.7,
            'clip_pg_rho_threshold': 2.2,
        }

        output = vtrace.from_importance_weights(**values)

        with self.test_session() as session:
            output_v = session.run(output)

        ground_truth_v = _ground_truth_calculation(**values)
        for a, b in zip(ground_truth_v, output_v):
            self.assertAllClose(a, b)

    @parameterized.named_parameters(('Batch1', 1), ('Batch2', 2))
    def test_vtrace_from_logits(self, batch_size):
        """Tests V-trace calculated from logits."""
        seq_len = 5
        num_actions = 3
        clip_rho_threshold = None  # No clipping.
        clip_pg_rho_threshold = None  # No clipping.

        # Intentionally leaving shapes unspecified to test if V-trace can
        # deal with that.
        placeholders = {
            # T, B, NUM_ACTIONS
            'behaviour_policy_logits': tf.placeholder(
                dtype=tf.float32, shape=[None, None, None]),
            # T, B, NUM_ACTIONS
            'target_policy_logits': tf.placeholder(
                dtype=tf.float32, shape=[None, None, None]),
            'actions': tf.placeholder(dtype=tf.int32, shape=[None, None]),
            'discounts': tf.placeholder(dtype=tf.float32, shape=[None, None]),
            'rewards': tf.placeholder(dtype=tf.float32, shape=[None, None]),
            'values': tf.placeholder(dtype=tf.float32, shape=[None, None]),
            'bootstrap_value': tf.placeholder(dtype=tf.float32, shape=[None]),
        }

        from_logits_output = vtrace.from_logits(
            clip_rho_threshold=clip_rho_threshold,
            clip_pg_rho_threshold=clip_pg_rho_threshold,
            **placeholders)

        target_log_probs = vtrace.log_probs_from_logits_and_actions(
            placeholders['target_policy_logits'], placeholders['actions'])
        behaviour_log_probs = vtrace.log_probs_from_logits_and_actions(
            placeholders['behaviour_policy_logits'], placeholders['actions'])
        log_rhos = target_log_probs - behaviour_log_probs
        ground_truth = (log_rhos, behaviour_log_probs, target_log_probs)

        values = {
            'behaviour_policy_logits': _shaped_arange(seq_len, batch_size,
                                                      num_actions),
            'target_policy_logits': _shaped_arange(seq_len, batch_size,
                                                   num_actions),
            'actions': np.random.randint(
                0, num_actions - 1, size=(seq_len, batch_size)),
            'discounts': np.array(  # T, B where B_i: [0.9 / (i+1)] * T
                [[0.9 / (b + 1) for b in range(batch_size)]
                 for _ in range(seq_len)]),
            'rewards': _shaped_arange(seq_len, batch_size),
            'values': _shaped_arange(seq_len, batch_size) / batch_size,
            'bootstrap_value': _shaped_arange(batch_size) + 1.0,  # B
        }

        feed_dict = {placeholders[k]: v for k, v in values.items()}
        with self.test_session() as session:
            from_logits_output_v = session.run(
                from_logits_output, feed_dict=feed_dict)
            (ground_truth_log_rhos, ground_truth_behaviour_action_log_probs,
             ground_truth_target_action_log_probs) = session.run(
                 ground_truth, feed_dict=feed_dict)

        # Calculate V-trace using the ground truth logits.
        from_iw = vtrace.from_importance_weights(
            log_rhos=ground_truth_log_rhos,
            discounts=values['discounts'],
            rewards=values['rewards'],
            values=values['values'],
            bootstrap_value=values['bootstrap_value'],
            clip_rho_threshold=clip_rho_threshold,
            clip_pg_rho_threshold=clip_pg_rho_threshold)

        with self.test_session() as session:
            from_iw_v = session.run(from_iw)

        self.assertAllClose(from_iw_v.vs, from_logits_output_v.vs)
        self.assertAllClose(from_iw_v.pg_advantages,
                            from_logits_output_v.pg_advantages)
        self.assertAllClose(ground_truth_behaviour_action_log_probs,
                            from_logits_output_v.behaviour_action_log_probs)
        self.assertAllClose(ground_truth_target_action_log_probs,
                            from_logits_output_v.target_action_log_probs)
        self.assertAllClose(ground_truth_log_rhos,
                            from_logits_output_v.log_rhos)

    def test_higher_rank_inputs_for_importance_weights(self):
        """Checks support for additional dimensions in inputs."""
        placeholders = {
            'log_rhos': tf.placeholder(
                dtype=tf.float32, shape=[None, None, 1]),
            'discounts': tf.placeholder(
                dtype=tf.float32, shape=[None, None, 1]),
            'rewards': tf.placeholder(
                dtype=tf.float32, shape=[None, None, 42]),
            'values': tf.placeholder(dtype=tf.float32, shape=[None, None, 42]),
            'bootstrap_value': tf.placeholder(
                dtype=tf.float32, shape=[None, 42])
        }
        output = vtrace.from_importance_weights(**placeholders)
        self.assertEqual(output.vs.shape.as_list()[-1], 42)

    def test_inconsistent_rank_inputs_for_importance_weights(self):
        """Test one of many possible errors in shape of inputs."""
        placeholders = {
            'log_rhos': tf.placeholder(
                dtype=tf.float32, shape=[None, None, 1]),
            'discounts': tf.placeholder(
                dtype=tf.float32, shape=[None, None, 1]),
            'rewards': tf.placeholder(
                dtype=tf.float32, shape=[None, None, 42]),
            'values': tf.placeholder(dtype=tf.float32, shape=[None, None, 42]),
            # Should be [None, 42].
            'bootstrap_value': tf.placeholder(dtype=tf.float32, shape=[None])
        }
        with self.assertRaisesRegexp(ValueError, 'must have rank 2'):
            vtrace.from_importance_weights(**placeholders)


if __name__ == '__main__':
    tf.test.main()
