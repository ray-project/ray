from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import pyarrow

try:
    import snappy
    SNAPPY_ENABLED = True
except ImportError:
    print("WARNING: python-snappy not available, disabling sample compression")
    SNAPPY_ENABLED = False


def pack(data):
    if SNAPPY_ENABLED:
        data = snappy.compress(
            pyarrow.serialize(data).to_buffer().to_pybytes())
        # TODO(ekl) we shouldn't need to base64 encode this data, but this
        # seems to not survive a transfer through the object store if we don't.
        return base64.b64encode(data)
    else:
        return data


def unpack(data):
    if SNAPPY_ENABLED:
        data = base64.b64decode(data)
        return pyarrow.deserialize(snappy.decompress(data))
    else:
        return data
