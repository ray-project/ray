from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import pyarrow

import snappy


def pack(data):
    data = snappy.compress(
        pyarrow.serialize(data).to_buffer().to_pybytes())
    # TODO(ekl) we shouldn't need to base64 encode this data, but this
    # seems to not survive a transfer through the object store if we don't.
    return base64.b64encode(data)


def unpack(data):
    data = base64.b64decode(data)
    return pyarrow.deserialize(snappy.decompress(data))
