from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils.annotations import override


class InputReader(object):
    """Input object for loading experiences in policy evaluation."""

    def next(self):
        """Return the next batch of experiences read."""

        raise NotImplementedError


class SamplerInput(InputReader):
    """Reads input experiences from an existing sampler."""

    def __init__(self, sampler):
        self.sampler = sampler
        self.pending = []

    @override(InputReader)
    def next(self):
        if self.pending:
            batch = self.pending.pop(0)
        else:
            batch = self.sampler.get_data()
            self.pending.extend(self.sampler.get_extra_batches())
        return batch
