from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Trainable(object):
    """Interface for utilizing Tune's full functionality"""

    def train(self):
        """
        Should return TrainingResult
        """
        raise NotImplementedError

    def save(self):
        """Saves the data.

        Should return a checkpoint path"""
        raise NotImplementedError

    def restore(self, checkpoint_path):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    @classmethod
    def identifier(self, config):
        return "Default"

