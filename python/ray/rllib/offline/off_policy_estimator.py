from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


# TODO(ekl): implement model-based estimators from literature, e.g., doubly
# robust, MAGIC. This will require adding some way of training a model
# (we should probably piggyback this on RLlib model API).
class OffPolicyEstimator(object):
    """Interface for an off policy reward estimator (experimental)."""

    def __init__(self, ioctx):
        self.ioctx = ioctx

    def process(self, batch):
        """Process a new batch of experiences.

        The batch will only contain data from one episode, but it may only be
        a fragment of an episode.
        """
        raise NotImplementedError

    def get_metrics(self):
        """Return a list of new episode metric estimates since the last call.

        Returns:
            list of RolloutMetrics objects.
        """
        raise NotImplementedError


class ImportanceSamplingEstimator(OffPolicyEstimator):
    """The step-wise IS estimator.

    Step-wise IS estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    def __init__(self, ioctx):
        OffPolicyEstimator.__init__(self, ioctx)

    def process(self, batch):
        pass

    def get_metrics(self):
        pass


class WeightedImportanceSamplingEstimator(object):
    """The weighted step-wise IS estimator.

    Step-wise WIS estimator in https://arxiv.org/pdf/1511.03722.pdf"""

    def __init__(self, ioctx):
        OffPolicyEstimator.__init__(self, ioctx)

    def process(self, batch):
        pass

    def get_metrics(self):
        pass
