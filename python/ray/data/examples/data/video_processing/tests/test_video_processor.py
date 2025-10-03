import asyncio
import io
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from ray.data.examples.data.video_processing.video_processor import VideoProcessor


@pytest.fixture
def mock_http_connection_bytes():
    with patch(
        "ray.data.examples.data.video_processing.video_processor.HTTPConnection",
    ) as mock:
        conn = MagicMock()
        conn.download_bytes_chunked = MagicMock(return_value=b"FAKE-MP4")
        conn.download_file = MagicMock()
        mock.return_value = conn
        yield conn


@pytest.fixture
def mock_pyav_open():
    class _Stream:
        type = "video"
        index = 0
        time_base = 1 / 1000
        duration = 1000

    class _Frame:
        def __init__(self, pts):
            self.pts = pts

        def to_image(self):
            class _Img:
                width = 64
                height = 48

                def resize(self, *args, **kwargs):
                    return self

                def crop(self, *args, **kwargs):
                    return self

                def convert(self, *args, **kwargs):
                    return self

            return _Img()

        def to_ndarray(self, format="rgb24"):
            return np.zeros((48, 64, 3), dtype=np.uint8)

    class _Container:
        def __init__(self):
            self.streams = [_Stream()]
            self.duration = 2_000_000

        def decode(self, video=0):
            for pts in [0, 333, 666, 1000, 1333, 1666]:
                yield _Frame(pts)

        def close(self):
            pass

    class _AV:
        time_base = 1 / 1_000_000

        @staticmethod
        def open(resolved, format=None):
            return _Container()

    class _PILImage:
        BILINEAR = 2

        class Resampling:
            BILINEAR = 2

    with (
        patch("ray.data.examples.data.video_processing.video_processor._av_mod", _AV),
        patch(
            "ray.data.examples.data.video_processing.video_processor._PIL_Image",
            _PILImage,
        ),
    ):
        yield


@pytest.mark.asyncio
async def test_process_http_source_basic(mock_http_connection_bytes, mock_pyav_open):
    processor = VideoProcessor(
        sampling={"fps": 3}, output_format="pil", cache_mode="memory"
    )
    results = await processor.process(["http://example.com/video.mp4"])
    assert len(results) == 1
    item = results[0]
    assert item["frames"]
    assert item["meta"]["video_num_frames"] >= 1
    assert item["meta"]["failed"] is False


@pytest.mark.asyncio
async def test_num_frames_sampling_exact(mock_http_connection_bytes, mock_pyav_open):
    processor = VideoProcessor(
        sampling={"num_frames": 4}, output_format="pil", cache_mode="memory"
    )
    results = await processor.process(["http://example.com/v.mp4"])
    frames = results[0]["frames"]
    assert len(frames) == 4


@pytest.mark.asyncio
async def test_data_uri_handling(mock_pyav_open):
    processor = VideoProcessor(
        sampling={"fps": 1}, output_format="pil", cache_mode="memory"
    )
    data_uri = "data:video/mp4;base64,AAAA"
    res = await processor.process([data_uri])
    assert res[0]["frames"]


@pytest.mark.asyncio
async def test_local_file_path_handling(mock_pyav_open, tmp_path):
    local = tmp_path / "x.mp4"
    local.write_bytes(b"fake")
    processor = VideoProcessor(sampling={"fps": 1}, output_format="pil")
    res = await processor.process([str(local)])
    assert res[0]["meta"]["failed"] is False


@pytest.mark.asyncio
async def test_auto_cache_to_disk_when_num_frames(
    mock_http_connection_bytes, mock_pyav_open, tmp_path
):
    processor = VideoProcessor(
        sampling={"num_frames": 3},
        cache_dir=str(tmp_path),
        cache_mode="auto",
        output_format="pil",
    )
    await processor.process(["http://example.com/v.mp4"])
    assert tmp_path.exists()
    assert mock_http_connection_bytes.download_file.call_count >= 1


@pytest.mark.asyncio
async def test_av_missing_import_error_metadata(mock_http_connection_bytes):
    with (
        patch(
            "ray.data.examples.data.video_processing.video_processor._av_mod",
            new=None,
        ),
        patch(
            "ray.data.examples.data.video_processing.video_processor._PIL_Image",
            new=object(),
        ),
    ):
        processor = VideoProcessor(sampling={"fps": 1}, output_format="pil")
        res = await processor.process(["http://example.com/v.mp4"])
        meta = res[0]["meta"]
        assert meta["failed"] is True
        assert meta.get("error_type") in {"ImportError", "Exception"}
        assert meta.get("attempts", 0) >= 1


@pytest.mark.asyncio
async def test_retry_success_and_counts(mock_pyav_open):
    processor = VideoProcessor(sampling={"fps": 1}, output_format="pil", retries=1)
    calls = {"n": 0}

    def _sync_ok(_src):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("temp")
        return {"frames": [object()], "meta": {"failed": False}}

    processor._process_one_sync = _sync_ok  # type: ignore

    res = await processor.process(["placeholder"])
    assert calls["n"] == 2
    assert res[0]["meta"]["failed"] is False


@pytest.mark.asyncio
async def test_non_retriable_no_retry(mock_pyav_open):
    processor = VideoProcessor(
        sampling={"fps": 1}, output_format="pil", retries=3, retry_backoff_base=0.0
    )

    def _sync_fail(_src):
        raise ValueError("bad config")

    processor._process_one_sync = _sync_fail  # type: ignore

    res = await processor.process(["placeholder"])
    meta = res[0]["meta"]
    assert meta["failed"] is True
    assert meta.get("attempts") == 1
    assert meta.get("retried") is False


@pytest.mark.asyncio
async def test_preprocess_convert_numpy_consistency(mock_http_connection_bytes):
    class _S:
        type = "video"
        index = 0
        time_base = 1 / 1000
        duration = 1000

    class _F:
        def __init__(self, pts):
            self.pts = pts

        def to_image(self):
            class _I:
                def __init__(self):
                    self.width = 10
                    self.height = 10

                def resize(self, size, *args, **kwargs):
                    w, h = size
                    self.width, self.height = int(w), int(h)
                    return self

                def crop(self, *a, **k):
                    return self

                def convert(self, *a, **k):
                    return self

                def __array__(self, dtype=None):
                    h, w = int(self.height), int(self.width)
                    arr = np.zeros((h, w, 3), dtype=np.uint8)
                    if dtype is not None:
                        return arr.astype(dtype)
                    return arr

            return _I()

        def to_ndarray(self, format="rgb24"):
            return np.zeros((10, 10, 3), dtype=np.uint8)

    class _C:
        def __init__(self):
            self.streams = [_S()]
            self.duration = 1_000_000

        def decode(self, video=0):
            yield _F(0)

        def close(self):
            pass

    class _AV:
        time_base = 1 / 1_000_000

        @staticmethod
        def open(resolved, format=None):
            return _C()

    class _P:
        BILINEAR = 2

        class Resampling:
            BILINEAR = 2

    with (
        patch("ray.data.examples.data.video_processing.video_processor._av_mod", _AV),
        patch("ray.data.examples.data.video_processing.video_processor._PIL_Image", _P),
    ):
        processor = VideoProcessor(
            sampling={"fps": 1},
            output_format="numpy",
            channels_first=False,
            preprocess={"resize": {"size": [8, 8]}, "convert": "RGB"},
        )
        res = await processor.process(["http://example.com/v.mp4"])
        arr = res[0]["frames"][0]
        assert isinstance(arr, np.ndarray)
        assert arr.shape[:2] == (8, 8)


@pytest.mark.asyncio
async def test_bytesio_format_guess_fallback(mock_http_connection_bytes):
    class _ErrOnAuto:
        time_base = 1 / 1_000_000

        @staticmethod
        def open(resolved, format=None):
            if isinstance(resolved, io.BytesIO) and format is None:
                raise RuntimeError("need format")

            class _S:
                type = "video"
                index = 0
                time_base = 1 / 1000
                duration = 1000

            class _F:
                def __init__(self, pts):
                    self.pts = pts

                def to_image(self):
                    class _I:
                        width = 4
                        height = 4

                        def resize(self, *a, **k):
                            return self

                        def crop(self, *a, **k):
                            return self

                        def convert(self, *a, **k):
                            return self

                    return _I()

            class _C:
                def __init__(self):
                    self.streams = [_S()]
                    self.duration = 500000

                def decode(self, video=0):
                    yield _F(0)

                def close(self):
                    pass

            return _C()

    class _P:
        BILINEAR = 2

        class Resampling:
            BILINEAR = 2

    with (
        patch(
            "ray.data.examples.data.video_processing.video_processor._av_mod",
            _ErrOnAuto,
        ),
        patch("ray.data.examples.data.video_processing.video_processor._PIL_Image", _P),
    ):
        processor = VideoProcessor(
            sampling={"fps": 1}, output_format="pil", cache_mode="memory"
        )
        data_uri = "data:video/mp4;base64,AAAA"
        res = await processor.process([data_uri])
        assert res and res[0]["meta"]["failed"] is False


@pytest.mark.asyncio
async def test_target_cap_limits_frames(mock_http_connection_bytes):
    class _S:
        type = "video"
        index = 0
        time_base = 1 / 1000
        duration = 2000

    class _F:
        def __init__(self, pts):
            self.pts = pts

        def to_image(self):
            class _I:
                width = 10
                height = 10

                def resize(self, *a, **k):
                    return self

                def crop(self, *a, **k):
                    return self

                def convert(self, *a, **k):
                    return self

            return _I()

    class _C:
        def __init__(self):
            self.streams = [_S()]
            self.duration = 2_000_000

        def decode(self, video=0):
            for pts in range(0, 2000, 50):
                yield _F(pts)

        def close(self):
            pass

    class _AV:
        time_base = 1 / 1_000_000

        @staticmethod
        def open(resolved, format=None):
            return _C()

    class _P:
        BILINEAR = 2

        class Resampling:
            BILINEAR = 2

    with (
        patch("ray.data.examples.data.video_processing.video_processor._av_mod", _AV),
        patch("ray.data.examples.data.video_processing.video_processor._PIL_Image", _P),
    ):
        processor = VideoProcessor(
            sampling={"fps": 30},
            output_format="pil",
            max_sampled_frames=2,
        )
        res = await processor.process(["http://example.com/v.mp4"])
        assert res and res[0]["meta"]["video_num_frames"] <= 2


@pytest.mark.asyncio
async def test_numpy_output_channels_first(mock_http_connection_bytes, mock_pyav_open):
    processor = VideoProcessor(
        sampling={"fps": 2},
        output_format="numpy",
        channels_first=True,
        cache_mode="memory",
    )
    res = await processor.process(["http://example.com/video.mp4"])
    frames = res[0]["frames"]
    assert isinstance(frames, list)
    if frames:
        f0 = frames[0]
        assert isinstance(f0, np.ndarray)
        assert f0.shape[0] in (3, 48)


@pytest.mark.asyncio
async def test_strict_no_fallback_when_no_frames(mock_http_connection_bytes):
    class _S:
        type = "video"
        index = 0
        time_base = 1 / 1000
        duration = 1000

    class _C:
        def __init__(self):
            self.streams = [_S()]
            self.duration = 1_000_000

        def decode(self, video=0):
            if False:
                yield
            return

        def close(self):
            pass

    class _AV:
        time_base = 1 / 1_000_000

        @staticmethod
        def open(resolved, format=None):
            return _C()

    class _P:
        BILINEAR = 2

        class Resampling:
            BILINEAR = 2

    with (
        patch("ray.data.examples.data.video_processing.video_processor._av_mod", _AV),
        patch("ray.data.examples.data.video_processing.video_processor._PIL_Image", _P),
    ):
        processor = VideoProcessor(sampling={"fps": 10}, output_format="pil")
        res = await processor.process(["http://example.com/v.mp4"])
        meta = res[0]["meta"]
        assert meta["failed"] is True
        assert meta["error_type"] in ("ValueError",)


@pytest.mark.asyncio
async def test_e2e_with_pyav_synth(tmp_path):
    pytest.importorskip(
        "av",
        reason="PyAV not installed; skipping video E2E tests (requires FFmpeg).",
    )
    import importlib

    av = importlib.import_module("av")

    path = tmp_path / "synth.mp4"
    out = av.open(str(path), mode="w")
    stream = out.add_stream("libx264", rate=24)
    stream.width = 64
    stream.height = 48
    stream.pix_fmt = "yuv420p"
    for i in range(10):
        img = np.full((48, 64, 3), fill_value=(i * 25) % 255, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(img, format="rgb24")
        for packet in stream.encode(frame):
            out.mux(packet)
    for packet in stream.encode(None):
        out.mux(packet)
    out.close()

    vp = VideoProcessor(sampling={"num_frames": 4}, output_format="numpy")
    res = await vp.process([str(path)])
    assert not res[0]["meta"]["failed"]
    assert res[0]["meta"]["video_num_frames"] == 4
    assert all(isinstance(f, np.ndarray) for f in res[0]["frames"])  # type: ignore


@pytest.mark.asyncio
async def test_e2e_num_frames_pil(tmp_path):
    pytest.importorskip(
        "av",
        reason="PyAV not installed; skipping video E2E tests (requires FFmpeg).",
    )
    import importlib

    av = importlib.import_module("av")

    path = tmp_path / "synth2.mp4"
    out = av.open(str(path), mode="w")
    stream = out.add_stream("libx264", rate=24)
    stream.width = 64
    stream.height = 48
    stream.pix_fmt = "yuv420p"
    for i in range(12):
        img = np.full((48, 64, 3), fill_value=(i * 20) % 255, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(img, format="rgb24")
        for packet in stream.encode(frame):
            out.mux(packet)
    for packet in stream.encode(None):
        out.mux(packet)
    out.close()

    vp = VideoProcessor(sampling={"num_frames": 4}, output_format="pil")
    res = await vp.process([str(path)])
    meta = res[0]["meta"]
    assert not meta["failed"]
    assert meta["video_num_frames"] == 4
    sz = meta.get("video_size")
    assert sz is None or sz == [64, 48]


@pytest.mark.asyncio
async def test_e2e_fps_sampling(tmp_path):
    pytest.importorskip(
        "av",
        reason="PyAV not installed; skipping video E2E tests (requires FFmpeg).",
    )
    import importlib

    av = importlib.import_module("av")

    path = tmp_path / "synth_fps.mp4"
    out = av.open(str(path), mode="w")
    stream = out.add_stream("libx264", rate=24)
    stream.width = 64
    stream.height = 48
    stream.pix_fmt = "yuv420p"
    for i in range(24):
        img = np.full((48, 64, 3), fill_value=(i * 10) % 255, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(img, format="rgb24")
        for packet in stream.encode(frame):
            out.mux(packet)
    for packet in stream.encode(None):
        out.mux(packet)
    out.close()

    vp = VideoProcessor(sampling={"fps": 6}, output_format="numpy")
    res = await vp.process([str(path)])
    meta = res[0]["meta"]
    assert not meta["failed"]
    assert 3 <= meta["video_num_frames"] <= 7
    ts = meta.get("frame_timestamps") or []
    assert all(ts[i] <= ts[i + 1] for i in range(len(ts) - 1))


@pytest.mark.asyncio
async def test_e2e_preprocess_resize_numpy_channels_first(tmp_path):
    pytest.importorskip(
        "av",
        reason="PyAV not installed; skipping video E2E tests (requires FFmpeg).",
    )
    import importlib

    av = importlib.import_module("av")

    path = tmp_path / "synth_resize.mp4"
    out = av.open(str(path), mode="w")
    stream = out.add_stream("libx264", rate=24)
    stream.width = 64
    stream.height = 48
    stream.pix_fmt = "yuv420p"
    for i in range(8):
        img = np.full((48, 64, 3), fill_value=(i * 30) % 255, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(img, format="rgb24")
        for packet in stream.encode(frame):
            out.mux(packet)
    for packet in stream.encode(None):
        out.mux(packet)
    out.close()

    vp = VideoProcessor(
        sampling={"num_frames": 3},
        output_format="numpy",
        channels_first=True,
        preprocess={"resize": {"size": [32, 24]}, "convert": "RGB"},
    )
    res = await vp.process([str(path)])
    meta = res[0]["meta"]
    frames = res[0]["frames"]
    assert not meta["failed"]
    assert meta["video_num_frames"] == 3
    assert all(isinstance(f, np.ndarray) for f in frames)
    assert frames[0].shape == (3, 24, 32)


@pytest.mark.asyncio
async def test_e2e_max_sampled_frames_cap(tmp_path):
    pytest.importorskip(
        "av",
        reason="PyAV not installed; skipping video E2E tests (requires FFmpeg).",
    )
    import importlib

    av = importlib.import_module("av")

    path = tmp_path / "synth_cap.mp4"
    out = av.open(str(path), mode="w")
    stream = out.add_stream("libx264", rate=24)
    stream.width = 64
    stream.height = 48
    stream.pix_fmt = "yuv420p"
    for i in range(30):
        img = np.full((48, 64, 3), fill_value=(i * 5) % 255, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(img, format="rgb24")
        for packet in stream.encode(frame):
            out.mux(packet)
    for packet in stream.encode(None):
        out.mux(packet)
    out.close()

    vp_fps = VideoProcessor(
        sampling={"fps": 30}, output_format="pil", max_sampled_frames=2
    )
    vp_n = VideoProcessor(
        sampling={"num_frames": 5}, output_format="pil", max_sampled_frames=2
    )
    res_fps, res_n = await asyncio.gather(
        vp_fps.process([str(path)]),
        vp_n.process([str(path)]),
    )
    assert res_fps[0]["meta"]["video_num_frames"] <= 2
    assert res_n[0]["meta"]["video_num_frames"] == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
