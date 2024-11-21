import io
import warnings

from typing import Any, Dict, Iterator

import torch

from ..utils import _log_api_usage_once

from ._video_opt import _HAS_CPU_VIDEO_DECODER

if _HAS_CPU_VIDEO_DECODER:

    def _has_video_opt() -> bool:
        return True

else:

    def _has_video_opt() -> bool:
        return False


try:
    import av

    av.logging.set_level(av.logging.ERROR)
    if not hasattr(av.video.frame.VideoFrame, "pict_type"):
        av = ImportError(
            """\
Your version of PyAV is too old for the necessary video operations in torchvision.
If you are on Python 3.5, you will have to build from source (the conda-forge
packages are not up-to-date).  See
https://github.com/mikeboers/PyAV#installation for instructions on how to
install PyAV on your system.
"""
        )
except ImportError:
    av = ImportError(
        """\
PyAV is not installed, and is necessary for the video operations in torchvision.
See https://github.com/mikeboers/PyAV#installation for instructions on how to
install PyAV on your system.
"""
    )


class VideoReader:
    """
    Fine-grained video-reading API.
    Supports frame-by-frame reading of various streams from a single video
    container. Much like previous video_reader API it supports the following
    backends: video_reader, pyav, and cuda.
    Backends can be set via `torchvision.set_video_backend` function.

    .. warning::

        In the near future, we intend to centralize PyTorch's video decoding
        capabilities within the `torchcodec
        <https://github.com/pytorch/torchcodec>`_ project. We encourage you to
        try it out and share your feedback, as the torchvision video decoders
        will eventually be deprecated.

    .. betastatus:: VideoReader class

    Example:
        The following examples creates a :mod:`VideoReader` object, seeks into 2s
        point, and returns a single frame::

            import torchvision
            video_path = "path_to_a_test_video"
            reader = torchvision.io.VideoReader(video_path, "video")
            reader.seek(2.0)
            frame = next(reader)

        :mod:`VideoReader` implements the iterable API, which makes it suitable to
        using it in conjunction with :mod:`itertools` for more advanced reading.
        As such, we can use a :mod:`VideoReader` instance inside for loops::

            reader.seek(2)
            for frame in reader:
                frames.append(frame['data'])
            # additionally, `seek` implements a fluent API, so we can do
            for frame in reader.seek(2):
                frames.append(frame['data'])

        With :mod:`itertools`, we can read all frames between 2 and 5 seconds with the
        following code::

            for frame in itertools.takewhile(lambda x: x['pts'] <= 5, reader.seek(2)):
                frames.append(frame['data'])

        and similarly, reading 10 frames after the 2s timestamp can be achieved
        as follows::

            for frame in itertools.islice(reader.seek(2), 10):
                frames.append(frame['data'])

    .. note::

        Each stream descriptor consists of two parts: stream type (e.g. 'video') and
        a unique stream id (which are determined by the video encoding).
        In this way, if the video container contains multiple
        streams of the same type, users can access the one they want.
        If only stream type is passed, the decoder auto-detects first stream of that type.

    Args:
        src (string, bytes object, or tensor): The media source.
            If string-type, it must be a file path supported by FFMPEG.
            If bytes, should be an in-memory representation of a file supported by FFMPEG.
            If Tensor, it is interpreted internally as byte buffer.
            It must be one-dimensional, of type ``torch.uint8``.

        stream (string, optional): descriptor of the required stream, followed by the stream id,
            in the format ``{stream_type}:{stream_id}``. Defaults to ``"video:0"``.
            Currently available options include ``['video', 'audio']``

        num_threads (int, optional): number of threads used by the codec to decode video.
            Default value (0) enables multithreading with codec-dependent heuristic. The performance
            will depend on the version of FFMPEG codecs supported.
    """

    def __init__(
        self,
        src: str,
        stream: str = "video",
        num_threads: int = 0,
    ) -> None:
        _log_api_usage_once(self)
        from .. import get_video_backend

        self.backend = get_video_backend()
        if isinstance(src, str):
            if not src:
                raise ValueError("src cannot be empty")
        elif isinstance(src, bytes):
            if self.backend in ["cuda"]:
                raise RuntimeError(
                    "VideoReader cannot be initialized from bytes object when using cuda or pyav backend."
                )
            elif self.backend == "pyav":
                src = io.BytesIO(src)
            else:
                with warnings.catch_warnings():
                    # Ignore the warning because we actually don't modify the buffer in this function
                    warnings.filterwarnings("ignore", message="The given buffer is not writable")
                    src = torch.frombuffer(src, dtype=torch.uint8)
        elif isinstance(src, torch.Tensor):
            if self.backend in ["cuda", "pyav"]:
                raise RuntimeError(
                    "VideoReader cannot be initialized from Tensor object when using cuda or pyav backend."
                )
        else:
            raise ValueError(f"src must be either string, Tensor or bytes object. Got {type(src)}")

        if self.backend == "cuda":
            device = torch.device("cuda")
            self._c = torch.classes.torchvision.GPUDecoder(src, device)

        elif self.backend == "video_reader":
            if isinstance(src, str):
                self._c = torch.classes.torchvision.Video(src, stream, num_threads)
            elif isinstance(src, torch.Tensor):
                self._c = torch.classes.torchvision.Video("", "", 0)
                self._c.init_from_memory(src, stream, num_threads)

        elif self.backend == "pyav":
            self.container = av.open(src, metadata_errors="ignore")
            # TODO: load metadata
            stream_type = stream.split(":")[0]
            stream_id = 0 if len(stream.split(":")) == 1 else int(stream.split(":")[1])
            self.pyav_stream = {stream_type: stream_id}
            self._c = self.container.decode(**self.pyav_stream)

            # TODO: add extradata exception

        else:
            raise RuntimeError("Unknown video backend: {}".format(self.backend))

    def __next__(self) -> Dict[str, Any]:
        """Decodes and returns the next frame of the current stream.
        Frames are encoded as a dict with mandatory
        data and pts fields, where data is a tensor, and pts is a
        presentation timestamp of the frame expressed in seconds
        as a float.

        Returns:
            (dict): a dictionary and containing decoded frame (``data``)
            and corresponding timestamp (``pts``) in seconds

        """
        if self.backend == "cuda":
            frame = self._c.next()
            if frame.numel() == 0:
                raise StopIteration
            return {"data": frame, "pts": None}
        elif self.backend == "video_reader":
            frame, pts = self._c.next()
        else:
            try:
                frame = next(self._c)
                pts = float(frame.pts * frame.time_base)
                if "video" in self.pyav_stream:
                    frame = torch.as_tensor(frame.to_rgb().to_ndarray()).permute(2, 0, 1)
                elif "audio" in self.pyav_stream:
                    frame = torch.as_tensor(frame.to_ndarray()).permute(1, 0)
                else:
                    frame = None
            except av.error.EOFError:
                raise StopIteration

        if frame.numel() == 0:
            raise StopIteration

        return {"data": frame, "pts": pts}

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return self

    def seek(self, time_s: float, keyframes_only: bool = False) -> "VideoReader":
        """Seek within current stream.

        Args:
            time_s (float): seek time in seconds
            keyframes_only (bool): allow to seek only to keyframes

        .. note::
            Current implementation is the so-called precise seek. This
            means following seek, call to :mod:`next()` will return the
            frame with the exact timestamp if it exists or
            the first frame with timestamp larger than ``time_s``.
        """
        if self.backend in ["cuda", "video_reader"]:
            self._c.seek(time_s, keyframes_only)
        else:
            # handle special case as pyav doesn't catch it
            if time_s < 0:
                time_s = 0
            temp_str = self.container.streams.get(**self.pyav_stream)[0]
            offset = int(round(time_s / temp_str.time_base))
            if not keyframes_only:
                warnings.warn("Accurate seek is not implemented for pyav backend")
            self.container.seek(offset, backward=True, any_frame=False, stream=temp_str)
            self._c = self.container.decode(**self.pyav_stream)
        return self

    def get_metadata(self) -> Dict[str, Any]:
        """Returns video metadata

        Returns:
            (dict): dictionary containing duration and frame rate for every stream
        """
        if self.backend == "pyav":
            metadata = {}  # type:  Dict[str, Any]
            for stream in self.container.streams:
                if stream.type not in metadata:
                    if stream.type == "video":
                        rate_n = "fps"
                    else:
                        rate_n = "framerate"
                    metadata[stream.type] = {rate_n: [], "duration": []}

                rate = getattr(stream, "average_rate", None) or stream.sample_rate

                metadata[stream.type]["duration"].append(float(stream.duration * stream.time_base))
                metadata[stream.type][rate_n].append(float(rate))
            return metadata
        return self._c.get_metadata()

    def set_current_stream(self, stream: str) -> bool:
        """Set current stream.
        Explicitly define the stream we are operating on.

        Args:
            stream (string): descriptor of the required stream. Defaults to ``"video:0"``
                Currently available stream types include ``['video', 'audio']``.
                Each descriptor consists of two parts: stream type (e.g. 'video') and
                a unique stream id (which are determined by video encoding).
                In this way, if the video container contains multiple
                streams of the same type, users can access the one they want.
                If only stream type is passed, the decoder auto-detects first stream
                of that type and returns it.

        Returns:
            (bool): True on success, False otherwise
        """
        if self.backend == "cuda":
            warnings.warn("GPU decoding only works with video stream.")
        if self.backend == "pyav":
            stream_type = stream.split(":")[0]
            stream_id = 0 if len(stream.split(":")) == 1 else int(stream.split(":")[1])
            self.pyav_stream = {stream_type: stream_id}
            self._c = self.container.decode(**self.pyav_stream)
            return True
        return self._c.set_current_stream(stream)
