from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
class EvaluatorInterface(object):
    """This is the interface between policy optimizers and policy evaluation.

    See also: PolicyEvaluator
    """

    @DeveloperAPI
    def sample(self):
        """Returns a batch of experience sampled from this evaluator.

        This method must be implemented by subclasses.

        Returns:
            SampleBatch|MultiAgentBatch: A columnar batch of experiences
            (e.g., tensors), or a multi-agent batch.

        Examples:
            >>> print(ev.sample())
            SampleBatch({"obs": [1, 2, 3], "action": [0, 1, 0], ...})
        """

        raise NotImplementedError

    @DeveloperAPI
    def learn_on_batch(self, samples):
        """Update policies based on the given batch.

        This is the equivalent to apply_gradients(compute_gradients(samples)),
        but can be optimized to avoid pulling gradients into CPU memory.

        Either this or the combination of compute/apply grads must be
        implemented by subclasses.

        Returns:
            info: dictionary of extra metadata from compute_gradients().

        Examples:
            >>> batch = ev.sample()
            >>> ev.learn_on_batch(samples)
        """

        return self.compute_apply(samples)

    @DeveloperAPI
    def compute_gradients(self, samples):
        """Returns a gradient computed w.r.t the specified samples.

        Either this or learn_on_batch() must be implemented by subclasses.

        Returns:
            (grads, info): A list of gradients that can be applied on a
            compatible evaluator. In the multi-agent case, returns a dict
            of gradients keyed by policy graph ids. An info dictionary of
            extra metadata is also returned.

        Examples:
            >>> batch = ev.sample()
            >>> grads, info = ev2.compute_gradients(samples)
        """

        raise NotImplementedError

    @DeveloperAPI
    def apply_gradients(self, grads):
        """Applies the given gradients to this evaluator's weights.

        Either this or learn_on_batch() must be implemented by subclasses.

        Examples:
            >>> samples = ev1.sample()
            >>> grads, info = ev2.compute_gradients(samples)
            >>> ev1.apply_gradients(grads)
        """

        raise NotImplementedError

    @DeveloperAPI
    def get_weights(self):
        """Returns the model weights of this Evaluator.

        This method must be implemented by subclasses.

        Returns:
            object: weights that can be set on a compatible evaluator.
            info: dictionary of extra metadata.

        Examples:
            >>> weights = ev1.get_weights()
        """

        raise NotImplementedError

    @DeveloperAPI
    def set_weights(self, weights):
        """Sets the model weights of this Evaluator.

        This method must be implemented by subclasses.

        Examples:
            >>> weights = ev1.get_weights()
            >>> ev2.set_weights(weights)
        """

        raise NotImplementedError

    @DeveloperAPI
    def compute_apply(self, samples):
        """Deprecated: override learn_on_batch instead."""

        grads, info = self.compute_gradients(samples)
        self.apply_gradients(grads)
        return info

    @DeveloperAPI
    def get_host(self):
        """Returns the hostname of the process running this evaluator."""

        return os.uname()[1]

    @DeveloperAPI
    def apply(self, func, *args):
        """Apply the given function to this evaluator instance."""

        return func(self, *args)
