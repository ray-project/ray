__all__: list[str] = []

GStreamerSource_OutputType_FRAME: int
GSTREAMER_SOURCE_OUTPUT_TYPE_FRAME: int
GStreamerSource_OutputType_MAT: int
GSTREAMER_SOURCE_OUTPUT_TYPE_MAT: int
GStreamerSource_OutputType = int
"""One of [GStreamerSource_OutputType_FRAME, GSTREAMER_SOURCE_OUTPUT_TYPE_FRAME, GStreamerSource_OutputType_MAT, GSTREAMER_SOURCE_OUTPUT_TYPE_MAT]"""


# Classes
class GStreamerPipeline:
    # Functions
    def __init__(self, pipeline: str) -> None: ...



