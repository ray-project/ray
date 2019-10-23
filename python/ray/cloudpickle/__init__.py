from __future__ import absolute_import
import sys

if sys.version_info[:2] >= (3, 8):
    from ray.cloudpickle.cloudpickle_fast import *
    FAST_CLOUDPICKLE_USED = True
else:
    from ray.cloudpickle.cloudpickle import *
    FAST_CLOUDPICKLE_USED = False

__version__ = '1.2.2.dev0'
