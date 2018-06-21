from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

import ray
from ray.rllib.utils.policy_graph import PolicyGraph
from ray.rllib.utils.tf_run_builder import TFRunBuilder


class TFPolicyGraph(PolicyGraph):
    """An agent policy and loss implemented in TensorFlow.

    Extending this class enables RLlib to perform TensorFlow specific
    optimizations on the policy graph, e.g., parallelization across gpus or
    fusing multiple graphs together in the multi-agent setting.

    All input and output tensors are of shape [BATCH_DIM, ...].

    Examples:
        >>> policy = TFPolicyGraphSubclass(
            sess, obs_input, action_sampler, loss, loss_inputs, is_training)

        >>> print(policy.compute_actions([1, 0, 2]))
        (array([0, 1, 1]), [], {})

        >>> print(policy.postprocess_trajectory(SampleBatch({...})))
        SampleBatch({"action": ..., "advantages": ..., ...})
    """

    def __init__(
            self, observation_space, action_space, sess, obs_input,
            action_sampler, loss, loss_inputs,
            is_training, state_inputs=None, state_outputs=None):
        """Initialize the policy.

        Arguments:
            observation_space (gym.Space): Observation space of the env.
            action_space (gym.Space): Action space of the env.
            sess (Session): TensorFlow session to use.
            obs_input (Tensor): input placeholder for observations.
            action_sampler (Tensor): Tensor for sampling an action.
            loss (Tensor): scalar policy loss output tensor.
            loss_inputs (list): a (name, placeholder) tuple for each loss
                input argument. Each placeholder name must correspond to a
                SampleBatch column key returned by postprocess_trajectory().
            is_training (Tensor): input placeholder for whether we are
                currently training the policy.
            state_inputs (list): list of RNN state output Tensors.
            state_outputs (list): list of initial state values.
        """

        self.observation_space = observation_space
        self.action_space = action_space
        self._sess = sess
        self._obs_input = obs_input
        self._sampler = action_sampler
        self._loss = loss
        self._loss_inputs = loss_inputs
        self._is_training = is_training
        self._state_inputs = state_inputs or []
        self._state_outputs = state_outputs or []
        self._optimizer = self.optimizer()
        self._grads_and_vars = [
            (g, v) for (g, v) in self.gradients(self._optimizer)
            if g is not None]
        self._grads = [g for (g, v) in self._grads_and_vars]
        self._apply_op = self._optimizer.apply_gradients(self._grads_and_vars)
        self._variables = ray.experimental.TensorFlowVariables(
            self._loss, self._sess)

        assert len(self._state_inputs) == len(self._state_outputs) == \
            len(self.get_initial_state())

    def build_compute_actions(
            self, builder, obs_batch, state_batches=None, is_training=False):
        state_batches = state_batches or []
        assert len(self._state_inputs) == len(state_batches), \
            (self._state_inputs, state_batches)
        builder.add_feed_dict(self.extra_compute_action_feed_dict())
        builder.add_feed_dict({self._obs_input: obs_batch})
        builder.add_feed_dict({self._is_training: is_training})
        builder.add_feed_dict(dict(zip(self._state_inputs, state_batches)))
        fetches = builder.add_fetches(
            [self._sampler] + self._state_outputs +
            [self.extra_compute_action_fetches()])
        return fetches[0], fetches[1:-1], fetches[-1]

    def compute_actions(
            self, obs_batch, state_batches=None, is_training=False):
        builder = TFRunBuilder(self._sess)
        fetches = self.build_compute_actions(
            builder, obs_batch, state_batches, is_training)
        return builder.get(fetches)

    def _get_loss_inputs_dict(self, postprocessed_batch):
        feed_dict = {}
        for key, ph in self._loss_inputs:
            # TODO(ekl) fix up handling of RNN inputs so that we can batch
            # across multiple rollouts
            if key.startswith("state_in_"):
                feed_dict[ph] = postprocessed_batch[key][:1]  # in state only
            else:
                feed_dict[ph] = postprocessed_batch[key]
        return feed_dict

    def build_compute_gradients(self, builder, postprocessed_batch):
        builder.add_feed_dict(self.extra_compute_grad_feed_dict())
        builder.add_feed_dict({self._is_training: True})
        builder.add_feed_dict(self._get_loss_inputs_dict(postprocessed_batch))
        fetches = builder.add_fetches(
            [self._grads, self.extra_compute_grad_fetches()])
        return fetches[0], fetches[1]

    def compute_gradients(self, postprocessed_batch):
        builder = TFRunBuilder(self._sess)
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
        builder = TFRunBuilder(self._sess)
        fetches = self.build_apply_gradients(builder, gradients)
        return builder.get(fetches)

    def build_compute_apply(self, builder, postprocessed_batch):
        builder.add_feed_dict(self.extra_compute_grad_feed_dict())
        builder.add_feed_dict(self.extra_apply_grad_feed_dict())
        builder.add_feed_dict(self._get_loss_inputs_dict(postprocessed_batch))
        builder.add_feed_dict({self._is_training: True})
        fetches = builder.add_fetches(
            [self._apply_op, self.extra_compute_grad_fetches(),
             self.extra_apply_grad_fetches()])
        return fetches[1], fetches[2]

    def compute_apply(self, postprocessed_batch):
        builder = TFRunBuilder(self._sess)
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
