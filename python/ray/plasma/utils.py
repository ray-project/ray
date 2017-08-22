from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import random

import pyarrow.plasma as plasma


def random_object_id():
    return plasma.ObjectID(np.random.bytes(20))


def generate_metadata(length):
    metadata_buffer = bytearray(length)
    if length > 0:
        metadata_buffer[0] = random.randint(0, 255)
        metadata_buffer[-1] = random.randint(0, 255)
        for _ in range(100):
            metadata_buffer[random.randint(0, length - 1)] = (
                random.randint(0, 255))
    return metadata_buffer


def write_to_data_buffer(buff, length):
    array = np.frombuffer(buff, dtype="uint8")
    if length > 0:
        array[0] = random.randint(0, 255)
        array[-1] = random.randint(0, 255)
        for _ in range(100):
            array[random.randint(0, length - 1)] = random.randint(0, 255)


def create_object_with_id(client, object_id, data_size, metadata_size,
                          seal=True):
    metadata = generate_metadata(metadata_size)
    memory_buffer = client.create(object_id, data_size, metadata)
    write_to_data_buffer(memory_buffer, data_size)
    if seal:
        client.seal(object_id)
    return memory_buffer, metadata


def create_object(client, data_size, metadata_size, seal=True):
    object_id = random_object_id()
    memory_buffer, metadata = create_object_with_id(client, object_id,
                                                    data_size, metadata_size,
                                                    seal=seal)
    return object_id, memory_buffer, metadata
