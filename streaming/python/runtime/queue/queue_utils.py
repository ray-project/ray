import logging
import random

import ray.streaming.runtime.queue.queue_constants as qc

logger = logging.getLogger(__name__)


def qid_str_to_bytes(qid_str):
    """
    Args:
        qid_str: string representation of queue id

    Returns:
        bytes representation of queue id
    """
    assert type(qid_str) in [str, bytes]
    if isinstance(qid_str, bytes):
        return qid_str
    qid_bytes = bytes.fromhex(qid_str)
    assert len(qid_bytes) == qc.QUEUE_ID_LEN
    return qid_bytes


def qid_bytes_to_str(qid_bytes):
    """
    Args:
        qid_bytes: bytes representation of queue id

    Returns:
        string representation of queue id
    """
    assert type(qid_bytes) in [str, bytes]
    if isinstance(qid_bytes, str):
        return qid_bytes
    return bytes.hex(qid_bytes)


def gen_random_qid():
    """Generate a random queue id string
    """
    res = ""
    for i in range(qc.QUEUE_ID_LEN * 2):
        res += str(chr(random.randint(0, 5) + ord('A')))
    return res


def generate_qid(from_index, to_index, ts):
    """Generate queue id, which is 20 character"""
    queue_id = bytearray(20)
    for i in range(11, 7, -1):
        queue_id[i] = ts & 0xff
        ts >>= 8
    queue_id[16] = (from_index & 0xffff) >> 8
    queue_id[17] = (from_index & 0xff)
    queue_id[18] = (to_index & 0xffff) >> 8
    queue_id[19] = (to_index & 0xff)
    return qid_bytes_to_str(bytes(queue_id))
