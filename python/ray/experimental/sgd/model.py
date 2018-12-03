from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Model(object):
    """Your class must implement this interface to be used with Ray SGD.

    This supports any form of input pipeline: it is up to you to define it
    using TensorFlow.
    For an example implementation, see tfbench/test_model.py
    """

    def build(self):
        """Build customized tensorflow model

        Users should setup variables such as loss or in get_loss function
        """
        raise NotImplementedError('build of %s is not implemented' % self.__class__.__name__)

    def get_loss(self):
        """Return loss of the model

        Returns:
            loss
        """
        raise NotImplementedError('get_loss of %s is not implemented' % self.__class__.__name__)

    #TODO support complex way of updating gradient, e.g. using different optimizers
    def get_optimizer(self):
        """Return optimizer for the model

        Returns:
            optimizer
        """
        raise NotImplementedError('get_optimizer of %s is not implemented' \
                % self.__class__.__name__)

    def get_metrics(self):
        """Return metrics of the model

        Returns:
            metrics(dict): e.g. {'accuray': accuracy_tensor}
        """
        return {}

    def get_predict_score(self):
        """Return tensor for predict score

        Returns:
            score: tensor for predicted probability
        """
        raise NotImplementedError('get_predict_score of %s is not implemented' \
                % self.__class__.__name__)

    def get_predict_label(self):
        """Return tensor for predict label if any

        Returns:
            predict_label: tensor for predicted label
        """
        raise NotImplementedError('get_predict_label of %s is not implemented' \
                % self.__class__.__name__)

    def get_feed_dict(self):
        """Extra values to pass in when computing gradients for the loss.

        Returns:
            TensorFlow feed_dict to add to the gradient operation.
        """
        return {}
