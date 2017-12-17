from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Evaluator(object):
    """RLlib optimizers require RL algorithms to implement this interface.

    Any algorithm that implements Evaluator can plug in any RLLib optimizer,
    e.g. async SGD, local multi-GPU SGD, etc.
    """

    def sample(self):
        """Returns experience samples from this Evaluator."""

        raise NotImplementedError

    def compute_gradients(self, samples):
        """Returns a gradient computed w.r.t the specified samples."""

        raise NotImplementedError

    def apply_gradients(self, grads):
        """Applies the given gradients to this Evaluator's weights."""

        raise NotImplementedError

    def get_weights(self):
        """Returns the model weights of this Evaluator."""

        raise NotImplementedError

    def set_weights(self, weights):
        """Sets the model weights of this Evaluator."""

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
    """The multi-GPU TF optimizer requires this additional interface."""

    def tf_loss_inputs(self):
        """Returns a list of the input placeholders required for the loss."""

        raise NotImplementedError

    def build_tf_loss(self, input_placeholders):
        """Returns a new loss tensor graph for the specified inputs."""

        raise NotImplementedError
