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

    @override(InputReader)
    def next(self):
        batches = [self.sampler.get_data()]
        batches.extend(self.sampler.get_extra_batches())
        if len(batches) > 1:
            return batches[0].concat_samples(batches)
        else:
            return batches[0]
