from __future__ import absolute_import
import sys

HAS_PICKLE5 = True

try:
    import pickle5
except ImportError:
    HAS_PICKLE5 = False

if sys.version_info[:2] >= (3, 8) or HAS_PICKLE5:
    from ray.cloudpickle.cloudpickle_fast import *
    FAST_CLOUDPICKLE_USED = True
else:
    from ray.cloudpickle.cloudpickle import *
    FAST_CLOUDPICKLE_USED = False

__version__ = '1.2.2.dev0'
