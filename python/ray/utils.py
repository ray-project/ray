from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import collections
import numpy as np
import sys

import ray.local_scheduler

ERROR_KEY_PREFIX = b"Error:"
DRIVER_ID_LENGTH = 20


def _random_string():
    return np.random.bytes(20)


def push_error_to_driver(redis_client, error_type, message, driver_id=None,
                         data=None):
    """Push an error message to the driver to be printed in the background.

    Args:
        redis_client: The redis client to use.
        error_type (str): The type of the error.
        message (str): The message that will be printed in the background
            on the driver.
        driver_id: The ID of the driver to push the error message to. If this
            is None, then the message will be pushed to all drivers.
        data: This should be a dictionary mapping strings to strings. It
            will be serialized with json and stored in Redis.
    """
    if driver_id is None:
        driver_id = DRIVER_ID_LENGTH * b"\x00"
    error_key = ERROR_KEY_PREFIX + driver_id + b":" + _random_string()
    data = {} if data is None else data
    redis_client.hmset(error_key, {"type": error_type,
                                   "message": message,
                                   "data": data})
    redis_client.rpush("ErrorKeys", error_key)


def is_cython(obj):
    """Check if an object is a Cython function or method"""

    # TODO(suo): We could split these into two functions, one for Cython
    # functions and another for Cython methods.
    # TODO(suo): There doesn't appear to be a Cython function 'type' we can
    # check against via isinstance. Please correct me if I'm wrong.
    def check_cython(x):
        return type(x).__name__ == "cython_function_or_method"

    # Check if function or method, respectively
    return check_cython(obj) or \
        (hasattr(obj, "__func__") and check_cython(obj.__func__))


def random_string():
    """Generate a random string to use as an ID.

    Note that users may seed numpy, which could cause this function to generate
    duplicate IDs. Therefore, we need to seed numpy ourselves, but we can't
    interfere with the state of the user's random number generator, so we
    extract the state of the random number generator and reset it after we are
    done.

    TODO(rkn): If we want to later guarantee that these are generated in a
    deterministic manner, then we will need to make some changes here.

    Returns:
        A random byte string of length 20.
    """
    # Get the state of the numpy random number generator.
    numpy_state = np.random.get_state()
    # Try to use true randomness.
    np.random.seed(None)
    # Generate the random ID.
    random_id = np.random.bytes(20)
    # Reset the state of the numpy random number generator.
    np.random.set_state(numpy_state)
    return random_id


def decode(byte_str):
    """Make this unicode in Python 3, otherwise leave it as bytes."""
    if sys.version_info >= (3, 0):
        return byte_str.decode("ascii")
    else:
        return byte_str


def binary_to_object_id(binary_object_id):
    return ray.local_scheduler.ObjectID(binary_object_id)


def binary_to_hex(identifier):
    hex_identifier = binascii.hexlify(identifier)
    if sys.version_info >= (3, 0):
        hex_identifier = hex_identifier.decode()
    return hex_identifier


def hex_to_binary(hex_identifier):
    return binascii.unhexlify(hex_identifier)


FunctionProperties = collections.namedtuple("FunctionProperties",
                                            ["num_return_vals",
                                             "resources",
                                             "max_calls"])
"""FunctionProperties: A named tuple storing remote functions information."""
