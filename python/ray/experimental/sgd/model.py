from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Model(object):
    """Your class must implement this interface to be used with Ray SGD.

    This supports any form of input pipeline: it is up to you to define it
    using TensorFlow.
    For an example implementation, see tfbench/test_model.py
    """

    def get_loss(self):
        """Return loss of the model

        Returns:
            loss
        """
        raise NotImplementedError(
            "get_loss of %s is not implemented" % self.__class__.__name__)

    # TODO support complex way of updating gradient,
    # e.g. using different optimizers
    def get_optimizer(self):
        """Return optimizer for the model

        Returns:
            optimizer
        """
        raise NotImplementedError(
            "get_optimizer of %s is not implemented" % self.__class__.__name__)

    def get_metrics(self):
        """Return metrics of the model

        Returns:
            metrics(dict): e.g. {"accuracy": accuracy(numpy data)}
        """
        return {}

    def get_feed_dict(self):
        """Extra values to pass in when computing gradients for the loss.

        Returns:
            TensorFlow feed_dict to add to the gradient operation.
        """
        return {}

    def get_weights(self):
        """Return weights from the model.

        Implementing `get_weights` is required for checkpointing and fault
        tolerance.

        Returns:
            Numpy array of weights from the model.
        """
        raise NotImplementedError(
            "get_weights of %s is not implemented" % self.__class__.__name__)

    def set_weights(self, weights):
        """Sets the model weights.

        Implementing `set_weights` is required for checkpointing and fault
        tolerance.

        Args:
            weights: numpy array of weights for the model.
        """
        raise NotImplementedError(
            "set_weights of %s is not implemented" % self.__class__.__name__)
