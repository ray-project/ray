from ..extension import _load_library


try:
    _load_library("gpu_decoder")
    _HAS_GPU_VIDEO_DECODER = True
except (ImportError, OSError):
    _HAS_GPU_VIDEO_DECODER = False
