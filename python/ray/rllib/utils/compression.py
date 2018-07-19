from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import base64
import numpy as np
import pyarrow

try:
    import lz4.frame
    LZ4_ENABLED = True
except ImportError:
    print("WARNING: lz4 not available, disabling sample compression. "
          "This will significantly impact RLlib performance. "
          "To install lz4, run `pip install lz4`.")
    LZ4_ENABLED = False


def pack(data):
    if LZ4_ENABLED:
        data = pyarrow.serialize(data).to_buffer().to_pybytes()
        data = lz4.frame.compress(data)
        # TODO(ekl) we shouldn't need to base64 encode this data, but this
        # seems to not survive a transfer through the object store if we don't.
        data = base64.b64encode(data)
    return data


def pack_if_needed(data):
    if isinstance(data, np.ndarray):
        data = pack(data)
    return data


def unpack(data):
    if LZ4_ENABLED:
        data = base64.b64decode(data)
        data = lz4.frame.decompress(data)
        data = pyarrow.deserialize(data)
    return data


def unpack_if_needed(data):
    if isinstance(data, bytes):
        data = unpack(data)
    return data


# Intel(R) Core(TM) i7-4600U CPU @ 2.10GHz
# Compression speed: 753.664 MB/s
# Compression ratio: 87.4839812046
# Decompression speed: 910.9504 MB/s
if __name__ == "__main__":
    size = 32 * 80 * 80 * 4
    data = np.ones(size).reshape((32, 80, 80, 4))

    count = 0
    start = time.time()
    while time.time() - start < 1:
        pack(data)
        count += 1
    compressed = pack(data)
    print("Compression speed: {} MB/s".format(count * size * 4 / 1e6))
    print("Compression ratio: {}".format(round(size * 4 / len(compressed), 2)))

    count = 0
    start = time.time()
    while time.time() - start < 1:
        unpack(compressed)
        count += 1
    print("Decompression speed: {} MB/s".format(count * size * 4 / 1e6))
