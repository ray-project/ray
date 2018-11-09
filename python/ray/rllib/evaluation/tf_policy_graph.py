from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import tensorflow as tf
import numpy as np

import ray
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.models.lstm import chop_into_sequences
from ray.rllib.utils.tf_run_builder import TFRunBuilder
from ray.rllib.utils.schedules import ConstantSchedule, PiecewiseSchedule

logger = logging.getLogger(__name__)


class TFPolicyGraph(PolicyGraph):
    """An agent policy and loss implemented in TensorFlow.

    Extending this class enables RLlib to perform TensorFlow specific
    optimizations on the policy graph, e.g., parallelization across gpus or
    fusing multiple graphs together in the multi-agent setting.

    Input tensors are typically shaped like [BATCH_SIZE, ...].

    Attributes:
        observation_space (gym.Space): observation space of the policy.
        action_space (gym.Space): action space of the policy.

    Examples:
        >>> policy = TFPolicyGraphSubclass(
            sess, obs_input, action_sampler, loss, loss_inputs, is_training)

        >>> print(policy.compute_actions([1, 0, 2]))
        (array([0, 1, 1]), [], {})

        >>> print(policy.postprocess_trajectory(SampleBatch({...})))
        SampleBatch({"action": ..., "advantages": ..., ...})
    """

    def __init__(self,
                 observation_space,
                 action_space,
                 sess,
                 obs_input,
                 action_sampler,
                 loss,
                 loss_inputs,
                 state_inputs=None,
                 state_outputs=None,
                 prev_action_input=None,
                 prev_reward_input=None,
                 seq_lens=None,
                 max_seq_len=20):
        """Initialize the policy graph.

        Arguments:
            observation_space (gym.Space): Observation space of the env.
            action_space (gym.Space): Action space of the env.
            sess (Session): TensorFlow session to use.
            obs_input (Tensor): input placeholder for observations, of shape
                [BATCH_SIZE, obs...].
            action_sampler (Tensor): Tensor for sampling an action, of shape
                [BATCH_SIZE, action...]
            loss (Tensor): scalar policy loss output tensor.
            loss_inputs (list): a (name, placeholder) tuple for each loss
                input argument. Each placeholder name must correspond to a
                SampleBatch column key returned by postprocess_trajectory(),
                and has shape [BATCH_SIZE, data...]. These keys will be read
                from postprocessed sample batches and fed into the specified
                placeholders during loss computation.
            state_inputs (list): list of RNN state input Tensors.
            state_outputs (list): list of RNN state output Tensors.
            prev_action_input (Tensor): placeholder for previous actions
            prev_reward_input (Tensor): placeholder for previous rewards
            seq_lens (Tensor): placeholder for RNN sequence lengths, of shape
                [NUM_SEQUENCES]. Note that NUM_SEQUENCES << BATCH_SIZE. See
                models/lstm.py for more information.
            max_seq_len (int): max sequence length for LSTM training.
        """

        self.observation_space = observation_space
        self.action_space = action_space
        self._sess = sess
        self._obs_input = obs_input
        self._prev_action_input = prev_action_input
        self._prev_reward_input = prev_reward_input
        self._sampler = action_sampler
        self._loss = loss
        self._loss_inputs = loss_inputs
        self._loss_input_dict = dict(self._loss_inputs)
        self._is_training = tf.placeholder_with_default(True, ())
        self._state_inputs = state_inputs or []
        self._state_outputs = state_outputs or []
        for i, ph in enumerate(self._state_inputs):
            self._loss_input_dict["state_in_{}".format(i)] = ph
        self._seq_lens = seq_lens
        self._max_seq_len = max_seq_len
        self._optimizer = self.optimizer()
        self._grads_and_vars = [(g, v)
                                for (g, v) in self.gradients(self._optimizer)
                                if g is not None]
        self._grads = [g for (g, v) in self._grads_and_vars]
        self._apply_op = self._optimizer.apply_gradients(self._grads_and_vars)
        self._variables = ray.experimental.TensorFlowVariables(
            self._loss, self._sess)

        if len(self._state_inputs) != len(self._state_outputs):
            raise ValueError(
                "Number of state input and output tensors must match, got: "
                "{} vs {}".format(self._state_inputs, self._state_outputs))
        if len(self.get_initial_state()) != len(self._state_inputs):
            raise ValueError(
                "Length of initial state must match number of state inputs, "
                "got: {} vs {}".format(self.get_initial_state(),
                                       self._state_inputs))
        if self._state_inputs and self._seq_lens is None:
            raise ValueError(
                "seq_lens tensor must be given if state inputs are defined")

        logger.debug("Created {} with loss inputs: {}".format(
            self, self._loss_input_dict))

    def build_compute_actions(self,
                              builder,
                              obs_batch,
                              state_batches=None,
                              prev_action_batch=None,
                              prev_reward_batch=None,
                              is_training=False,
                              episodes=None):
        state_batches = state_batches or []
        assert len(self._state_inputs) == len(state_batches), \
            (self._state_inputs, state_batches)
        builder.add_feed_dict(self.extra_compute_action_feed_dict())
        builder.add_feed_dict({self._obs_input: obs_batch})
        if state_batches:
            builder.add_feed_dict({self._seq_lens: np.ones(len(obs_batch))})
        if self._prev_action_input is not None and prev_action_batch:
            builder.add_feed_dict({self._prev_action_input: prev_action_batch})
        if self._prev_reward_input is not None and prev_reward_batch:
            builder.add_feed_dict({self._prev_reward_input: prev_reward_batch})
        builder.add_feed_dict({self._is_training: is_training})
        builder.add_feed_dict(dict(zip(self._state_inputs, state_batches)))
        fetches = builder.add_fetches([self._sampler] + self._state_outputs +
                                      [self.extra_compute_action_fetches()])
        return fetches[0], fetches[1:-1], fetches[-1]

    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        is_training=False,
                        episodes=None):
        builder = TFRunBuilder(self._sess, "compute_actions")
        fetches = self.build_compute_actions(builder, obs_batch, state_batches,
                                             prev_action_batch,
                                             prev_reward_batch, is_training)
        return builder.get(fetches)

    def _get_loss_inputs_dict(self, batch):
        feed_dict = {}

        # Simple case
        if not self._state_inputs:
            for k, ph in self._loss_inputs:
                feed_dict[ph] = batch[k]
            return feed_dict

        # RNN case
        feature_keys = [k for k, v in self._loss_inputs]
        state_keys = [
            "state_in_{}".format(i) for i in range(len(self._state_inputs))
        ]
        feature_sequences, initial_states, seq_lens = chop_into_sequences(
            batch["eps_id"], [batch[k] for k in feature_keys],
            [batch[k] for k in state_keys], self._max_seq_len)
        for k, v in zip(feature_keys, feature_sequences):
            feed_dict[self._loss_input_dict[k]] = v
        for k, v in zip(state_keys, initial_states):
            feed_dict[self._loss_input_dict[k]] = v
        feed_dict[self._seq_lens] = seq_lens
        return feed_dict

    def build_compute_gradients(self, builder, postprocessed_batch):
        builder.add_feed_dict(self.extra_compute_grad_feed_dict())
        builder.add_feed_dict({self._is_training: True})
        builder.add_feed_dict(self._get_loss_inputs_dict(postprocessed_batch))
        fetches = builder.add_fetches(
            [self._grads, self.extra_compute_grad_fetches()])
        return fetches[0], fetches[1]

    def compute_gradients(self, postprocessed_batch):
        builder = TFRunBuilder(self._sess, "compute_gradients")
        fetches = self.build_compute_gradients(builder, postprocessed_batch)
        return builder.get(fetches)

    def build_apply_gradients(self, builder, gradients):
        assert len(gradients) == len(self._grads), (gradients, self._grads)
        builder.add_feed_dict(self.extra_apply_grad_feed_dict())
        builder.add_feed_dict({self._is_training: True})
        builder.add_feed_dict(dict(zip(self._grads, gradients)))
        fetches = builder.add_fetches(
            [self._apply_op, self.extra_apply_grad_fetches()])
        return fetches[1]

    def apply_gradients(self, gradients):
        builder = TFRunBuilder(self._sess, "apply_gradients")
        fetches = self.build_apply_gradients(builder, gradients)
        return builder.get(fetches)

    def build_compute_apply(self, builder, postprocessed_batch):
        builder.add_feed_dict(self.extra_compute_grad_feed_dict())
        builder.add_feed_dict(self.extra_apply_grad_feed_dict())
        builder.add_feed_dict(self._get_loss_inputs_dict(postprocessed_batch))
        builder.add_feed_dict({self._is_training: True})
        fetches = builder.add_fetches([
            self._apply_op,
            self.extra_compute_grad_fetches(),
            self.extra_apply_grad_fetches()
        ])
        return fetches[1], fetches[2]

    def compute_apply(self, postprocessed_batch):
        builder = TFRunBuilder(self._sess, "compute_apply")
        fetches = self.build_compute_apply(builder, postprocessed_batch)
        return builder.get(fetches)

    def get_weights(self):
        return self._variables.get_flat()

    def set_weights(self, weights):
        return self._variables.set_flat(weights)

    def extra_compute_action_feed_dict(self):
        return {}

    def extra_compute_action_fetches(self):
        return {}  # e.g, value function

    def extra_compute_grad_feed_dict(self):
        return {}  # e.g, kl_coeff

    def extra_compute_grad_fetches(self):
        return {}  # e.g, td error

    def extra_apply_grad_feed_dict(self):
        return {}

    def extra_apply_grad_fetches(self):
        return {}  # e.g., batch norm updates

    def optimizer(self):
        return tf.train.AdamOptimizer()

    def gradients(self, optimizer):
        return optimizer.compute_gradients(self._loss)

    def loss_inputs(self):
        return self._loss_inputs


class LearningRateSchedule(object):
    """Mixin for TFPolicyGraph that adds a learning rate schedule."""

    def __init__(self, lr, lr_schedule):
        self.cur_lr = tf.get_variable("lr", initializer=lr)
        if lr_schedule is None:
            self.lr_schedule = ConstantSchedule(lr)
        else:
            self.lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1])

    def on_global_var_update(self, global_vars):
        super(LearningRateSchedule, self).on_global_var_update(global_vars)
        self.cur_lr.load(
            self.lr_schedule.value(global_vars["timestep"]),
            session=self._sess)

    def optimizer(self):
        return tf.train.AdamOptimizer(self.cur_lr)
