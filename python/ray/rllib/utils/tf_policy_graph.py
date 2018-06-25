from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

import ray
from ray.rllib.utils.policy_graph import PolicyGraph
from ray.rllib.models.lstm import chop_into_sequences


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

    def __init__(
            self, sess, obs_input, action_sampler, loss, loss_inputs,
            is_training, state_inputs=None, state_outputs=None, seq_lens=None,
            max_seq_len=20):
        """Initialize the policy graph.

        Arguments:
            obs_input (Tensor): input placeholder for observations, of shape
                [BATCH_SIZE, obs...].
            action_sampler (Tensor): Tensor for sampling an action, of shape
                [BATCH_SIZE, action...]
            loss (Tensor): scalar policy loss output tensor.
            loss_inputs (list): a (name, placeholder) tuple for each loss
                input argument. Each placeholder name must correspond to a
                SampleBatch column key returned by postprocess_trajectory(),
                and has shape [BATCH_SIZE, data...].
            is_training (Tensor): input placeholder for whether we are
                currently training the policy.
            state_inputs (list): list of RNN state output Tensors.
            state_outputs (list): list of initial state values.
            seq_lens (Tensor): placeholder for RNN sequence lengths.
            max_seq_len (int): max sequence length for LSTM training.
        """

        self._sess = sess
        self._obs_input = obs_input
        self._sampler = action_sampler
        self._loss = loss
        self._loss_inputs = loss_inputs
        self._loss_input_dict = dict(self._loss_inputs)
        self._is_training = is_training
        self._state_inputs = state_inputs or []
        self._state_outputs = state_outputs or []
        self._seq_lens = seq_lens
        self._max_seq_len = max_seq_len
        self._optimizer = self.optimizer()
        self._grads_and_vars = [
            (g, v) for (g, v) in self.gradients(self._optimizer)
            if g is not None]
        self._grads = [g for (g, v) in self._grads_and_vars]
        self._apply_op = self._optimizer.apply_gradients(self._grads_and_vars)
        self._variables = ray.experimental.TensorFlowVariables(
            self._loss, self._sess)

        assert len(self._state_inputs) == len(self._state_outputs) == \
            len(self.get_initial_state()), \
            (self._state_inputs, self._state_outputs, self.get_initial_state())
        if self._state_inputs:
            assert self._seq_lens is not None

    def compute_actions(
            self, obs_batch, state_batches=None, is_training=False):
        state_batches = state_batches or []
        assert len(self._state_inputs) == len(state_batches), \
            (self._state_inputs, state_batches)
        feed_dict = self.extra_compute_action_feed_dict()
        feed_dict[self._obs_input] = obs_batch
        feed_dict[self._is_training] = is_training
        for ph, value in zip(self._state_inputs, state_batches):
            feed_dict[ph] = value
        fetches = self._sess.run(
            ([self._sampler] + self._state_outputs +
             [self.extra_compute_action_fetches()]), feed_dict=feed_dict)
        return fetches[0], fetches[1:-1], fetches[-1]

    def _get_loss_inputs(self, batch):
        feed_dict = {}

        # Simple case
        if not self._state_inputs:
            for k, ph in self._loss_inputs:
                feed_dict[ph] = batch[k]
            return feed_dict

        # RNN case
        feature_keys = [
            k for k, v in self._loss_inputs if not k.startswith("state_in_")]
        state_keys = [
            k for k, v in self._loss_inputs if k.startswith("state_in_")]
        feature_sequences, initial_states, seq_lens = chop_into_sequences(
            batch["t"],
            [batch[k] for k in feature_keys],
            [batch[k] for k in state_keys],
            self._max_seq_len)
        for k, v in zip(feature_keys, feature_sequences):
            feed_dict[self._loss_input_dict[k]] = v
        for k, v in zip(state_keys, initial_states):
            feed_dict[self._loss_input_dict[k]] = v
        feed_dict[self._seq_lens] = seq_lens
        return feed_dict

    def compute_gradients(self, postprocessed_batch):
        feed_dict = self.extra_compute_grad_feed_dict()
        feed_dict[self._is_training] = True
        feed_dict.update(self._get_loss_inputs(postprocessed_batch))
        fetches = self._sess.run(
            [self._grads, self.extra_compute_grad_fetches()],
            feed_dict=feed_dict)
        return fetches[0], fetches[1]

    def apply_gradients(self, gradients):
        assert len(gradients) == len(self._grads), (gradients, self._grads)
        feed_dict = self.extra_apply_grad_feed_dict()
        feed_dict[self._is_training] = True
        for ph, value in zip(self._grads, gradients):
            feed_dict[ph] = value
        fetches = self._sess.run(
            [self._apply_op, self.extra_apply_grad_fetches()],
            feed_dict=feed_dict)
        return fetches[1]

    def compute_apply(self, postprocessed_batch):
        feed_dict = self.extra_compute_grad_feed_dict()
        feed_dict.update(self.extra_apply_grad_feed_dict())
        feed_dict.update(self._get_loss_inputs(postprocessed_batch))
        feed_dict[self._is_training] = True
        fetches = self._sess.run(
            [self._apply_op, self.extra_compute_grad_fetches(),
             self.extra_apply_grad_fetches()],
            feed_dict=feed_dict)
        return fetches[1], fetches[2]

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
