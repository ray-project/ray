from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os


class Evaluator(object):
    """Algorithms implement this interface to leverage RLlib optimizers.

    Any algorithm that implements Evaluator can plug in any RLLib optimizer,
    e.g. async SGD, local multi-GPU SGD, etc.
    """

    def sample(self):
        """Returns experience samples from this Evaluator.

        Returns:
            SampleBatch: A columnar batch of experiences.

        Examples:
            >>> print(ev.sample())
            SampleBatch({"a": [1, 2, 3], "b": [4, 5, 6]})
        """

        raise NotImplementedError

    def compute_gradients(self, samples):
        """Returns a gradient computed w.r.t the specified samples.

        Returns:
            object: A gradient that can be applied on a compatible evaluator.
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

    def compute_apply(self, samples):
        """Fused compute and apply gradients on given samples.

        Returns:
            The result of calling compute_gradients(samples)
        """

        grads = self.compute_gradients(samples)
        self.apply_gradients(grads)
        return grads

    def get_host(self):
        """Returns hostname of actor."""

        return os.uname()[1]


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
