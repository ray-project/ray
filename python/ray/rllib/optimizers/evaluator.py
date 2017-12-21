from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Evaluator(object):
    """RL Algorithms extend this base class to leverage RLlib optimizers.

    Any algorithm that implements Evaluator can plug in any RLLib optimizer,
    e.g. async SGD, local multi-GPU SGD, etc.

    Attributes:
        obs_filter (Filter):
        rew_filter (Filter):
    """

    def sample(self):
        """Returns experience samples from this Evaluator.

        Returns:
            SampleBatch: A columnar batch of experiences.
            info (dict): Extra return values from evaluator

        Examples:
            >>> print(ev.sample())
            SampleBatch({"a": [1, 2, 3], "b": [4, 5, 6]})
        """

        raise NotImplementedError

    def compute_gradients(self, samples):
        """Returns a gradient computed w.r.t the specified samples.

        Returns:
            object: A gradient that can be applied on a compatible evaluator.
            info (dict): Extra return values from evaluator.
        """

        raise NotImplementedError

    def apply_gradients(self, grads):
        """Applies the given gradients to this Evaluator's weights.

        Examples:
            >>> samples = ev1.sample()
            >>> grads = ev2.compute_gradients(samples)
            >>> ev1.apply_gradients(grads)
        """

        raise NotImplementedError

    def get_weights(self):
        """Returns the model weights of this Evaluator.

        Returns:
            object: weights that can be set on a compatible evaluator.
        """

        raise NotImplementedError

    def set_weights(self, weights):
        """Sets the model weights of this Evaluator.

        Examples:
            >>> weights = ev1.get_weights()
            >>> ev2.set_weights(weights)
        """

        raise NotImplementedError

    def merge_filters(self, obs_filter=None, rew_filter=None):
        """
        Examples:
            >>> ... TODO(rliaw)"""
        if obs_filter:
            self.obs_filter.apply_changes(obs_filter, with_buffer=False)
        if rew_filter:
            self.rew_filter.apply_changes(rew_filter, with_buffer=False)

    def sync_filters(self, obs_filter=None, rew_filter=None):
        """Updates local filters with copies from master and rebases
        the accumulated delta to it, as if the accumulated delta was acquired
        using the new obs_filter

        Examples:
            >>> ... TODO(rliaw)"""
        if rew_filter:
            new_rew_filter = rew_filter.copy()
            new_rew_filter.apply_changes(self.rew_filter, with_buffer=True)
            self.rew_filter.sync(new_rew_filter)
        if obs_filter:
            new_obs_filter = obs_filter.copy()
            new_obs_filter.apply_changes(self.obs_filter, with_buffer=True)
            self.obs_filter.sync(new_obs_filter)

    def get_filters(self, flush_after=False):
        """Clears buffer while making a copy of the filter.
        Examples:
            >>> ... TODO(rliaw)"""
        obs_filter = self.obs_filter.copy()
        if hasattr(self.obs_filter, "lockless"):
            obs_filter = obs_filter.lockless()
        rew_filter = self.rew_filter.copy()
        if flush_after:
            self.obs_filter.clear_buffer(), self.rew_filter.clear_buffer()
        return obs_filter, rew_filter



class TFMultiGPUSupport(Evaluator):
    """The multi-GPU TF optimizer requires additional TF-specific supportt.

    Attributes:
        sess (Session) the tensorflow session associated with this evaluator
    """

    def tf_loss_inputs(self):
        """Returns a list of the input placeholders required for the loss.

        For example, the following calls should work:

        Returns:
            list: a (name, placeholder) tuple for each loss input argument.
                Each placeholder name must correspond to one of the SampleBatch
                column keys returned by sample().

        Examples:
            >>> print(ev.tf_loss_inputs())
            [("action", action_placeholder), ("reward", reward_placeholder)]

            >>> print(ev.sample().data.keys())
            ["action", "reward"]
        """

        raise NotImplementedError

    def build_tf_loss(self, input_placeholders):
        """Returns a new loss tensor graph for the specified inputs.

        The graph must share vars with this Evaluator's policy model, so that
        the multi-gpu optimizer can update the weights.

        Examples:
            >>> loss_inputs = ev.tf_loss_inputs()
            >>> ev.build_tf_loss([ph for _, ph in loss_inputs])
        """

        raise NotImplementedError
