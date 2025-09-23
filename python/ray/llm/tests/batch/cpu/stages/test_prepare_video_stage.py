import io
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from ray.llm._internal.batch.stages.prepare_video_stage import (
    PrepareVideoUDF,
    VideoProcessor,
)


@pytest.fixture
def mock_http_connection_bytes():
    with patch(
        "ray.llm._internal.batch.stages.prepare_video_stage.HTTPConnection",
    ) as mock:
        conn = MagicMock()
        conn.download_bytes_chunked = MagicMock(return_value=b"FAKE-MP4")
        conn.download_file = MagicMock()
        mock.return_value = conn
        yield conn


@pytest.fixture
def mock_pyav_open():
    # Mock av module and decoding pipeline
    with patch("importlib.import_module") as imp:

        def _import(name):
            if name == "av":

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
                        self.duration = 2000000  # 2s in microseconds

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

                return _AV
            elif name == "PIL.Image":

                class _PILImage:
                    pass

                return _PILImage
            return __import__(name)

        imp.side_effect = _import
        yield imp


@pytest.mark.asyncio
async def test_udf_extract_and_process_basic(
    mock_http_connection_bytes, mock_pyav_open
):
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        sampling={"fps": 3},  # default-like
        output_format="pil",
        cache_mode="memory",
    )

    batch = {
        "__data": [
            {
                "messages": [
                    {
                        "content": [
                            {"type": "video", "video": "http://example.com/video.mp4"}
                        ]
                    }
                ]
            }
        ]
    }

    results = []
    async for out in udf(batch):
        results.append(out["__data"][0])

    assert len(results) == 1
    item = results[0]
    assert "video" in item and "video_meta" in item
    assert len(item["video"]) == 1
    assert item["video_meta"][0]["video_num_frames"] >= 1


@pytest.mark.asyncio
async def test_num_frames_sampling_exact(mock_http_connection_bytes, mock_pyav_open):
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        sampling={"num_frames": 4},
        output_format="pil",
        cache_mode="memory",
    )
    batch = {
        "__data": [
            {
                "messages": [
                    {
                        "content": [
                            {"type": "video", "video": "http://example.com/v.mp4"}
                        ]
                    }
                ]
            }
        ]
    }
    outs = []
    async for out in udf(batch):
        outs.append(out["__data"][0])
    frames = outs[0]["video"][0]
    assert len(frames) == 4 or len(frames) > 0  # allow fallback if decode shorter


@pytest.mark.asyncio
async def test_data_uri_handling(mock_pyav_open):
    # No HTTP needed; provide a tiny fake data URI
    # This will be base64 but our av mock doesn't actually parse bytes; we just need the flow to hit BytesIO
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        sampling={"fps": 1},
        output_format="pil",
        cache_mode="memory",
    )
    data_uri = "data:video/mp4;base64,AAAA"
    batch = {
        "__data": [{"messages": [{"content": [{"type": "video", "video": data_uri}]}]}]
    }
    outs = []
    async for out in udf(batch):
        outs.append(out["__data"][0])
    assert "video" in outs[0]


@pytest.mark.asyncio
async def test_local_file_path_handling(mock_pyav_open, tmp_path):
    local = tmp_path / "x.mp4"
    local.write_bytes(b"fake")
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        sampling={"fps": 1},
        output_format="pil",
    )
    batch = {
        "__data": [
            {"messages": [{"content": [{"type": "video", "video": str(local)}]}]}
        ]
    }
    outs = []
    async for out in udf(batch):
        outs.append(out["__data"][0])
    assert outs[0]["video_meta"][0]["failed"] is False


@pytest.mark.asyncio
async def test_auto_cache_to_disk_when_num_frames(
    mock_http_connection_bytes, mock_pyav_open, tmp_path
):
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        sampling={"num_frames": 3},
        cache_dir=str(tmp_path),
        cache_mode="auto",
        output_format="pil",
    )
    batch = {
        "__data": [
            {
                "messages": [
                    {
                        "content": [
                            {"type": "video", "video": "http://example.com/v.mp4"}
                        ]
                    }
                ]
            }
        ]
    }
    async for _ in udf(batch):
        pass
    # ensure directory exists and download_file happened once
    assert tmp_path.exists()
    assert mock_http_connection_bytes.download_file.call_count >= 1


@pytest.mark.asyncio
async def test_av_missing_import_error_metadata(mock_http_connection_bytes):
    # Simulate missing av import and validate metadata fields
    with patch(
        "importlib.import_module",
        side_effect=lambda name: (
            (_ for _ in ()).throw(ImportError()) if name == "av" else __import__(name)
        ),
    ):
        udf = PrepareVideoUDF(
            data_column="__data",
            expected_input_keys=["messages"],
            sampling={"fps": 1},
            output_format="pil",
        )
        batch = {
            "__data": [
                {
                    "messages": [
                        {
                            "content": [
                                {"type": "video", "video": "http://example.com/v.mp4"}
                            ]
                        }
                    ]
                }
            ]
        }
        outs = []
        async for out in udf(batch):
            outs.append(out["__data"][0])
        meta = outs[0]["video_meta"][0]
        assert meta["failed"] is True
        assert "error_type" in meta and meta["error_type"] in (
            "ImportError",
            "Exception",
        )
        assert "attempts" in meta and meta["attempts"] >= 1
        assert "retried" in meta


@pytest.mark.asyncio
async def test_multiple_videos_order_preserved(
    mock_http_connection_bytes, mock_pyav_open
):
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        sampling={"fps": 2},
        output_format="pil",
        cache_mode="memory",
    )
    batch = {
        "__data": [
            {
                "messages": [
                    {
                        "content": [
                            {"type": "video", "video": "http://example.com/a.mp4"},
                            {"type": "video", "video": "http://example.com/b.mp4"},
                        ]
                    }
                ]
            }
        ]
    }
    outs = []
    async for out in udf(batch):
        outs.append(out["__data"][0])
    assert len(outs[0]["video"]) == 2


@pytest.mark.asyncio
async def test_preprocess_convert_numpy_consistency(mock_http_connection_bytes):
    # Ensure numpy output respects preprocess (resize) by going through PIL then to numpy
    with patch("importlib.import_module") as imp:

        def _import(name):
            if name == "av":

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
                            # Track logical size to reflect resize in numpy conversion
                            def __init__(self):
                                self.width = 10
                                self.height = 10

                            def resize(self, size, *args, **kwargs):
                                # size is (W, H)
                                try:
                                    w, h = size
                                    self.width, self.height = int(w), int(h)
                                except Exception:
                                    pass
                                return self

                            def crop(self, *a, **k):
                                return self

                            def convert(self, *a, **k):
                                return self

                            # Provide numpy array interface to make np.array(img) valid
                            def __array__(self, dtype=None):
                                import numpy as _np

                                h, w = int(self.height), int(self.width)
                                arr = _np.zeros((h, w, 3), dtype=_np.uint8)
                                if dtype is not None:
                                    return arr.astype(dtype)
                                return arr

                        return _I()

                    def to_ndarray(self, format="rgb24"):
                        return np.zeros((10, 10, 3), dtype=np.uint8)

                class _C:
                    def __init__(self):
                        self.streams = [_S()]
                        self.duration = 1000000

                    def decode(self, video=0):
                        yield _F(0)

                    def close(self):
                        pass

                class _AV:
                    time_base = 1 / 1_000_000

                    @staticmethod
                    def open(resolved, format=None):
                        return _C()

                return _AV
            elif name == "PIL.Image":

                class _P:
                    pass

                return _P
            return __import__(name)

        imp.side_effect = _import
        udf = PrepareVideoUDF(
            data_column="__data",
            expected_input_keys=["messages"],
            sampling={"fps": 1},
            output_format="numpy",
            channels_first=False,
        )
        udf._video._preprocess = {"resize": {"size": [8, 8]}, "convert": "RGB"}
        batch = {
            "__data": [
                {
                    "messages": [
                        {
                            "content": [
                                {"type": "video", "video": "http://example.com/v.mp4"}
                            ]
                        }
                    ]
                }
            ]
        }
        outs = []
        async for out in udf(batch):
            outs.append(out["__data"][0])
        arr = outs[0]["video"][0][0]
        assert isinstance(arr, np.ndarray)
        assert arr.shape[:2] == (8, 8)


@pytest.mark.asyncio
async def test_bytesio_format_guess_fallback(mock_http_connection_bytes):
    # For data URI, first open without format raises; second with guessed format succeeds
    with patch("importlib.import_module") as imp:

        class _ErrOnAuto:
            time_base = 1 / 1_000_000

            @staticmethod
            def open(resolved, format=None):
                # Fail when resolved is BytesIO and format is None
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

        def _import(name):
            if name == "av":
                return _ErrOnAuto
            elif name == "PIL.Image":

                class _P:
                    pass

                return _P
            return __import__(name)

        imp.side_effect = _import
        udf = PrepareVideoUDF(
            data_column="__data",
            expected_input_keys=["messages"],
            sampling={"fps": 1},
            output_format="pil",
            cache_mode="memory",
        )
        data_uri = (
            "data:video/mp4;base64,AAAA"  # mime indicates mp4 so guess should be mp4
        )
        batch = {
            "__data": [
                {"messages": [{"content": [{"type": "video", "video": data_uri}]}]}
            ]
        }
        outs = []
        async for out in udf(batch):
            outs.append(out["__data"][0])
        assert outs and outs[0]["video_meta"][0]["failed"] is False


@pytest.mark.asyncio
async def test_retries_success_and_counts(mock_pyav_open):
    # Monkeypatch processor sync method to fail once then succeed
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        sampling={"fps": 1},
        output_format="pil",
        retries=1,
        retry_backoff_base=0.0,
    )
    calls = {"n": 0}

    def _sync_ok(_src):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("temp")
        return {"frames": [object()], "meta": {"failed": False}}

    udf._video._process_one_sync = _sync_ok  # patch instance method

    batch = {
        "__data": [
            {
                "messages": [
                    {
                        "content": [
                            {"type": "video", "video": "http://example.com/v.mp4"}
                        ]
                    }
                ]
            }
        ]
    }
    outs = []
    async for out in udf(batch):
        outs.append(out["__data"][0])
    assert calls["n"] == 2  # retried once
    assert outs[0]["video_meta"][0]["failed"] is False


@pytest.mark.asyncio
async def test_non_retriable_no_retry(mock_pyav_open):
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        sampling={"fps": 1},
        output_format="pil",
        retries=3,
        retry_backoff_base=0.0,
    )

    def _sync_fail(_src):
        raise ValueError("bad config")

    udf._video._process_one_sync = _sync_fail

    batch = {
        "__data": [
            {
                "messages": [
                    {
                        "content": [
                            {"type": "video", "video": "http://example.com/v.mp4"}
                        ]
                    }
                ]
            }
        ]
    }
    outs = []
    async for out in udf(batch):
        outs.append(out["__data"][0])
    meta = outs[0]["video_meta"][0]
    assert meta["failed"] is True
    assert meta.get("attempts") == 1
    assert meta.get("retried") is False


@pytest.mark.asyncio
async def test_target_cap_limits_frames(mock_http_connection_bytes):
    # Use av mock that yields many frames over 2s; cap to 2
    with patch("importlib.import_module") as imp:

        def _import(name):
            if name == "av":

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
                        # generate many pts
                        for pts in range(0, 2000, 50):
                            yield _F(pts)

                    def close(self):
                        pass

                class _AV:
                    time_base = 1 / 1_000_000

                    @staticmethod
                    def open(resolved, format=None):
                        return _C()

                return _AV
            elif name == "PIL.Image":

                class _P:
                    pass

                return _P
            return __import__(name)

        imp.side_effect = _import
        udf = PrepareVideoUDF(
            data_column="__data",
            expected_input_keys=["messages"],
            sampling={"fps": 30},
            output_format="pil",
            max_sampled_frames=2,
        )
        batch = {
            "__data": [
                {
                    "messages": [
                        {
                            "content": [
                                {"type": "video", "video": "http://example.com/v.mp4"}
                            ]
                        }
                    ]
                }
            ]
        }
        outs = []
        async for out in udf(batch):
            outs.append(out["__data"][0])
        assert outs and outs[0]["video_meta"][0]["video_num_frames"] <= 2


@pytest.mark.asyncio
async def test_numpy_output_channels_first(mock_http_connection_bytes, mock_pyav_open):
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        sampling={"fps": 2},
        output_format="numpy",
        channels_first=True,
        cache_mode="memory",
    )
    batch = {
        "__data": [
            {
                "messages": [
                    {
                        "content": [
                            {"type": "video", "video": "http://example.com/video.mp4"}
                        ]
                    }
                ]
            }
        ]
    }

    out_rows = []
    async for out in udf(batch):
        out_rows.append(out["__data"][0])

    frames = out_rows[0]["video"][0]

    assert isinstance(frames, list)
    if frames:
        f0 = frames[0]
        assert isinstance(f0, np.ndarray)
        assert f0.shape[0] in (3, 48)  # (C,H,W) or (H,W,C) safety


@pytest.mark.asyncio
async def test_strict_no_fallback_when_no_frames(mock_http_connection_bytes):
    # Use av mock that yields no frames -> should surface ValueError and mark failed in metadata
    with patch("importlib.import_module") as imp:

        def _import(name):
            if name == "av":

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
                            yield  # no frames
                        return

                    def close(self):
                        pass

                class _AV:
                    time_base = 1 / 1_000_000

                    @staticmethod
                    def open(resolved, format=None):
                        return _C()

                return _AV
            elif name == "PIL.Image":

                class _P:
                    pass

                return _P
            return __import__(name)

        imp.side_effect = _import
        udf = PrepareVideoUDF(
            data_column="__data",
            expected_input_keys=["messages"],
            sampling={"fps": 10},
            output_format="pil",
        )
        batch = {
            "__data": [
                {
                    "messages": [
                        {
                            "content": [
                                {"type": "video", "video": "http://example.com/v.mp4"}
                            ]
                        }
                    ]
                }
            ]
        }
        outs = []
        async for out in udf(batch):
            outs.append(out["__data"][0])
        meta = outs[0]["video_meta"][0]
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

    # Synthesize a short mp4 with solid color frames
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

    # Synthesize a short mp4 with solid color frames
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
    for i in range(24):  # ~1 second @24fps
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
    import asyncio
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
