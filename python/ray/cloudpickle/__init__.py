from __future__ import absolute_import
import sys

# TODO(suquark): This is a temporary flag for
# the new serialization implementation.
# Remove it when the old one is deprecated.
USE_NEW_SERIALIZER = False

if USE_NEW_SERIALIZER and sys.version_info[:2] >= (3, 8):
    from ray.cloudpickle.cloudpickle_fast import *
    FAST_CLOUDPICKLE_USED = True
else:
    try:
        import pickle5
    except ImportError:
        # We need pickle5 backport support for the new serializer.
        USE_NEW_SERIALIZER = False
    from ray.cloudpickle.cloudpickle import *
    FAST_CLOUDPICKLE_USED = False

__version__ = '1.2.2.dev0'
