from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class OutputWriter(object):
    """Writer object for saving experiences from policy evaluation."""

    def write(self, sample_batch):
        """Save a batch of experiences.

        Arguments:
            sample_batch: SampleBatch or MultiAgentBatch to save.
        """
        raise NotImplementedError


class NoopOutput(object):
    """Output writer that discards its outputs."""

    def write(self, sample_batch):
        pass
