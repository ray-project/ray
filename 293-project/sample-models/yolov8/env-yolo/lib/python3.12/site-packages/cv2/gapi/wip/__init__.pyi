__all__: list[str] = []

import cv2
import cv2.gapi
import cv2.gapi.wip.gst
import cv2.typing
import typing as _typing


from cv2.gapi.wip import draw as draw
from cv2.gapi.wip import gst as gst
from cv2.gapi.wip import onevpl as onevpl


# Classes
class GOutputs:
    # Functions
    def getGMat(self) -> cv2.GMat: ...

    def getGScalar(self) -> cv2.GScalar: ...

    def getGArray(self, type: cv2.gapi.ArgType) -> cv2.GArrayT: ...

    def getGOpaque(self, type: cv2.gapi.ArgType) -> cv2.GOpaqueT: ...


class IStreamSource:
    ...


# Functions
def get_streaming_source(pipeline: cv2.gapi.wip.gst.GStreamerPipeline, appsinkName: str, outputType: cv2.gapi.wip.gst.GStreamerSource_OutputType = ...) -> IStreamSource: ...

@_typing.overload
def make_capture_src(path: str, properties: cv2.typing.map_int_and_double = ...) -> IStreamSource: ...
@_typing.overload
def make_capture_src(id: int, properties: cv2.typing.map_int_and_double = ...) -> IStreamSource: ...

def make_gst_src(pipeline: str, outputType: cv2.gapi.wip.gst.GStreamerSource_OutputType = ...) -> IStreamSource: ...


