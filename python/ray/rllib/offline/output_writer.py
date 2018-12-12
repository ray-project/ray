from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils.annotations import override


class OutputWriter(object):
    """Writer object for saving experiences from policy evaluation."""

    def write(self, sample_batch):
        """Save a batch of experiences.

        Arguments:
            sample_batch: SampleBatch or MultiAgentBatch to save.
        """
        raise NotImplementedError


class NoopOutput(OutputWriter):
    """Output writer that discards its outputs."""

    @override(OutputWriter)
    def write(self, sample_batch):
        pass
