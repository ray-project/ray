from enum import IntEnum


class TaskContext(IntEnum):
    """TaskContext constants for queue.enqueue method"""
    Web = 1
    Python = 2
