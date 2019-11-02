from __future__ import absolute_import
import sys

if sys.version_info[:2] >= (3, 6):
    if sys.version_info[:2] < (3, 8):
        try:
            import pickle5
        except ImportError:
            raise AssertionError("'pickle5' should have been "
                                 "installed for Python 3.6 and 3.7")
    from ray.cloudpickle.cloudpickle_fast import *
    FAST_CLOUDPICKLE_USED = True
else:
    from ray.cloudpickle.cloudpickle import *
    FAST_CLOUDPICKLE_USED = False

__version__ = '1.2.2.dev0'
