from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class UnsupportedSpaceException(Exception):
    """Error for an unsupported action or observation space."""
    pass
