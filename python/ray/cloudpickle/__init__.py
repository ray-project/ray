from __future__ import absolute_import

# TODO(suquark): This is a temporary flag for the new serialization implementation.
# Remove it when the old one is deprecated.
USE_NEW_SERIALIZER = False

if USE_NEW_SERIALIZER:
    from ray.cloudpickle.cloudpickle_fast import *
else:
    from ray.cloudpickle.cloudpickle import *

__version__ = '1.2.2.dev0'
