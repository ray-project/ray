from ray.rllib.utils.annotations import DeveloperAPI

import logging
import time
import base64
import numpy as np
from ray import cloudpickle as pickle

logger = logging.getLogger(__name__)

try:
    import lz4.frame

    LZ4_ENABLED = True
except ImportError:
    logger.warning(
        "lz4 not available, disabling sample compression. "
        "This will significantly impact RLlib performance. "
        "To install lz4, run `pip install lz4`."
    )
    LZ4_ENABLED = False


@DeveloperAPI
def compression_supported():
    return LZ4_ENABLED


@DeveloperAPI
def pack(data):
    if LZ4_ENABLED:
        data = pickle.dumps(data)
        data = lz4.frame.compress(data)
        # TODO(ekl) we shouldn't need to base64 encode this data, but this
        # seems to not survive a transfer through the object store if we don't.
        data = base64.b64encode(data)
    return data


@DeveloperAPI
def pack_if_needed(data):
    if isinstance(data, np.ndarray):
        data = pack(data)
    return data


@DeveloperAPI
def unpack(data):
    if LZ4_ENABLED:
        data = base64.b64decode(data)
        data = lz4.frame.decompress(data)
        data = pickle.loads(data)
    return data


@DeveloperAPI
def unpack_if_needed(data):
    if is_compressed(data):
        data = unpack(data)
    return data


@DeveloperAPI
def is_compressed(data):
    return isinstance(data, bytes) or isinstance(data, str)


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
