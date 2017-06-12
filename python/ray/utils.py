from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import collections
import numpy as np
import sys

import ray.local_scheduler


def random_string():
  """Generate a random string to use as an ID.

  Note that users may seed numpy, which could cause this function to generate
  duplicate IDs. Therefore, we need to seed numpy ourselves, but we can't
  interfere with the state of the user's random number generator, so we extract
  the state of the random number generator and reset it after we are done.

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
                                             "num_cpus",
                                             "num_gpus",
                                             "max_calls"])
"""FunctionProperties: A named tuple storing remote functions information."""
