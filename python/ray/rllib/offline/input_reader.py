from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class InputReader(object):
    """Input object for loading experiences in policy evaluation."""

    @PublicAPI
    def next(self):
        """Return the next batch of experiences read.

        Returns:
            SampleBatch or MultiAgentBatch read.
        """
        raise NotImplementedError
