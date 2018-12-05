from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class InputReader(object):
    """Input object for loading experiences in policy evaluation."""

    def next(self):
        """Return the next batch of experiences read."""

        raise NotImplementedError
