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


class TFMultiGpuSupport(Evaluator):
    """The multi-GPU TF optimizer requires this additional interface."""

    def tf_loss_inputs(self):
        """Returns a list of the input placeholders required for the loss."""

        raise NotImplementedError

    def build_tf_loss(self, input_placeholders):
        """Returns a new loss tensor graph for the specified inputs."""

        raise NotImplementedError
