from typing import Any, Dict, Iterator

import torch

from ..utils import _log_api_usage_once

try:
    from ._load_gpu_decoder import _HAS_GPU_VIDEO_DECODER
except ModuleNotFoundError:
    _HAS_GPU_VIDEO_DECODER = False

from ._video_opt import (
    _HAS_CPU_VIDEO_DECODER,
    _HAS_VIDEO_OPT,
    _probe_video_from_file,
    _probe_video_from_memory,
    _read_video_from_file,
    _read_video_from_memory,
    _read_video_timestamps_from_file,
    _read_video_timestamps_from_memory,
    Timebase,
    VideoMetaData,
)
from .image import (
    decode_gif,
    decode_image,
    decode_jpeg,
    decode_png,
    decode_webp,
    encode_jpeg,
    encode_png,
    ImageReadMode,
    read_file,
    read_image,
    write_file,
    write_jpeg,
    write_png,
)
from .video import read_video, read_video_timestamps, write_video
from .video_reader import VideoReader


__all__ = [
    "write_video",
    "read_video",
    "read_video_timestamps",
    "_read_video_from_file",
    "_read_video_timestamps_from_file",
    "_probe_video_from_file",
    "_read_video_from_memory",
    "_read_video_timestamps_from_memory",
    "_probe_video_from_memory",
    "_HAS_CPU_VIDEO_DECODER",
    "_HAS_VIDEO_OPT",
    "_HAS_GPU_VIDEO_DECODER",
    "_read_video_clip_from_memory",
    "_read_video_meta_data",
    "VideoMetaData",
    "Timebase",
    "ImageReadMode",
    "decode_image",
    "decode_jpeg",
    "decode_png",
    "decode_heic",
    "decode_webp",
    "decode_gif",
    "encode_jpeg",
    "encode_png",
    "read_file",
    "read_image",
    "write_file",
    "write_jpeg",
    "write_png",
    "Video",
    "VideoReader",
]
