import ray
from ray.rllib.v2.policy import Policy


class TFPolicy(Policy):
    """An agent policy that supports TF-specific optimizations.

    All input and output tensors are of shape [BATCH_DIM, ...].

    Required attributes:
        action_dist (ActionDistribution): output policy action dist.
        loss (Tensor): scalar policy loss output tensor.
        loss_in (list): a (name, placeholder) tuple for each loss
            input argument. Each placeholder name must correspond to one of the
            SampleBatch column keys returned by self.postprocess_trajectory().

    Optional attributes:
        state_out (list): list of recurrent state output Tensors.
            Non-recurrent policies can omit this attribute.
        state_init (list): list of initial state values.
            Non-recurrent policies can omit this attribute.

    Examples:
        >>> policy = TFPolicySubclass(sess, obs_ph, is_train_ph, [s0, s1])
        >>> print(policy.action_dist)
        <ActionDistribution object>

        >>> print(policy.loss)
        <tf.Tensor shape=(?)>

        >>> print(policy.loss_in)
        [("action", action_ph), ("advantages", advantages_ph), ...]

        >>> print(policy.postprocess_trajectory(SampleBatch({...})))
        SampleBatch({"action": ..., "advantages": ..., ...})

        >>> print(policy.state_out)
        [<tf.Tensor shape=(?, ...)>, <tf.Tensor shape=(?, ...)>]

        >>> print(policy.state_init)
        [np.array([...]), np.array([...])]
    """

    def __init__(self, sess, obs_input, is_training, state_inputs=[]):
        """Initialize the policy.

        This assumes that subclass init has already completed.
        """

        for attr in ["action_dist", "loss", "loss_in"]:
            if not hasattr(self, attr):
                raise NotImplementedError(
                    "TFPolicy implementations must define `{}`".format(attr))

        if not hasattr(self, "state_init"):
            self.state_init = []
        if not hasattr(self, "state_out"):
            self.state_out = []

        assert len(state_inputs) == len(self.state_init) == \
            len(self.state_out), (state_inputs, state_init, state_out)

        self._sess = sess
        self._obs_input = obs_input
        self._state_inputs = state_inputs
        self._is_training = is_training
        self._optimizer = self.optimizer()
        self._grads_and_vars = self.gradients(self._optimizer)
        self._grads = [g for (g, v) in self._grads_and_vars]
        self._apply_op = self._optimizer.apply_gradients(self._grads_and_vars)
        self._variables = ray.experimental.TensorFlowVariables(
            self.loss, self._sess)

    def compute_actions(self, obs_batch, state_batches, is_training=False):
        assert len(self._state_inputs) == len(state_batches), \
            (self._state_inputs, state_batches)
        feed_dict = self.extra_compute_action_feed_dict()
        feed_dict[self._obs_input] = obs_batch
        feed_dict[self._is_training] = is_training
        for ph, value in zip(self._state_inputs, state_batches):
            feed_dict[ph] = value
        fetches = self.sess.run(
            ([self.action_dist.sample] + self.state_out +
             [self.extra_compute_action_fetches]), feed_dict=feed_dict)
        return fetches[:-1], fetches[-1]

    def compute_gradients(self, postprocessed_batch):
        feed_dict = self.extra_compute_grad_feed_dict()
        for key, ph in self.loss_in:
            feed_dict[ph] = postprocessed_batch[key]
        fetches = self.sess.run(
            [self._grad_op, self.extra_compute_grad_fetches()],
            feed_dict=feed_dict)
        return fetches[0], fetches[1]

    def apply_gradients(self, gradients):
        assert len(gradients) == len(self._grads), (gradients, self._grads)
        feed_dict = self.extra_apply_grad_feed_dict()
        for ph, value in zip(self._grads, gradients):
            feed_dict[ph] = value
        fetches = self.sess.run(
            [self._apply_op, self.extra_apply_grad_fetches],
            feed_dict=feed_dict)
        return fetches[1]

    def get_weights(self):
        return self._variables.get_flat()

    def set_weights(self, weights):
        return self._variables.set_flat(weights)

    def extra_compute_action_feed_dict(self):
        raise {}

    def extra_compute_action_fetches(self):
        raise {}  # e.g, value function

    def extra_compute_grad_feed_dict(self):
        raise {}  # e.g, kl_coeff

    def extra_compute_grad_fetches(self):
        raise {}  # e.g, td error

    def extra_apply_grad_feed_dict(self):
        raise {}

    def extra_apply_grad_fetches(self):
        raise {} # e.g., batch norm updates

    def optimizer(self):
        return tf.train.AdamOptimizer()

    def gradients(self, optimizer):
        return optimizer.compute_gradients(self.loss)
