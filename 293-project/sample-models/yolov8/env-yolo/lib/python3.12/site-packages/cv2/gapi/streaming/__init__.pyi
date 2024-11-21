__all__: list[str] = []

import cv2
import typing as _typing


# Enumerations
sync_policy_dont_sync: int
SYNC_POLICY_DONT_SYNC: int
sync_policy_drop: int
SYNC_POLICY_DROP: int
sync_policy = int
"""One of [sync_policy_dont_sync, SYNC_POLICY_DONT_SYNC, sync_policy_drop, SYNC_POLICY_DROP]"""



# Classes
class queue_capacity:
    capacity: int

    # Functions
    def __init__(self, cap: int = ...) -> None: ...



# Functions
def desync(g: cv2.GMat) -> cv2.GMat: ...

def seqNo(arg1: cv2.GMat) -> cv2.GOpaqueT: ...

def seq_id(arg1: cv2.GMat) -> cv2.GOpaqueT: ...

@_typing.overload
def size(src: cv2.GMat) -> cv2.GOpaqueT: ...
@_typing.overload
def size(r: cv2.GOpaqueT) -> cv2.GOpaqueT: ...
@_typing.overload
def size(src: cv2.GFrame) -> cv2.GOpaqueT: ...

def timestamp(arg1: cv2.GMat) -> cv2.GOpaqueT: ...


