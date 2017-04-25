from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import sys

import ray.local_scheduler


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
