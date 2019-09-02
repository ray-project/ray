from __future__ import absolute_import


if ray.USE_NEW_SERIALIZER:
    from ray.cloudpickle.cloudpickle_fast import *
else:
    from ray.cloudpickle.cloudpickle import *

__version__ = '1.2.2.dev0'
