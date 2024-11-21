__all__: list[str] = []

import cv2
import typing as _typing


from cv2.gapi.ot import cpu as cpu


# Enumerations
NEW: int
TRACKED: int
LOST: int
TrackingStatus = int
"""One of [NEW, TRACKED, LOST]"""



# Classes
class ObjectTrackerParams:
    max_num_objects: int
    input_image_format: int
    tracking_per_class: bool


# Functions
@_typing.overload
def track(mat: cv2.GMat, detected_rects: cv2.GArrayT, detected_class_labels: cv2.GArrayT, delta: float) -> tuple[cv2.GArrayT, cv2.GArrayT, cv2.GArrayT, cv2.GArrayT]: ...
@_typing.overload
def track(frame: cv2.GFrame, detected_rects: cv2.GArrayT, detected_class_labels: cv2.GArrayT, delta: float) -> tuple[cv2.GArrayT, cv2.GArrayT, cv2.GArrayT, cv2.GArrayT]: ...


