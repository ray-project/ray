import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest  # type: ignore

from ray.llm._internal.batch.stages.prepare_video_stage import (
    PrepareVideoUDF,
    VideoProcessor,
)


@pytest.fixture
def mock_http(monkeypatch, tmp_path):
    conn = MagicMock()
    conn.download_bytes_chunked.return_value = b"FAKE-MP4"

    def _download_file(url: str, target: Path, timeout: float) -> None:
        Path(target).write_bytes(b"FAKE-MP4")

    conn.download_file.side_effect = _download_file
    monkeypatch.setattr(
        "ray.llm._internal.batch.stages.prepare_video_stage.HTTPConnection",
        lambda: conn,
    )
    return conn


@pytest.fixture
def mock_av(monkeypatch):
    class DummyImage:
        def __init__(self) -> None:
            self.width = 64
            self.height = 48

    class DummyFrame:
        def __init__(self, pts: int) -> None:
            self.pts = pts

        def to_image(self) -> DummyImage:
            return DummyImage()

    class DummyStream:
        type = "video"
        index = 0
        time_base = 1 / 1000
        duration = 1000

    class DummyContainer:
        def __init__(self) -> None:
            self.streams = [DummyStream()]
            self.duration = 2_000_000

        def decode(self, video: int = 0):
            for pts in [0, 333, 666, 1000]:
                yield DummyFrame(pts)

        def close(self) -> None:
            pass

    class DummyAV:
        time_base = 1 / 1_000_000

        @staticmethod
        def open(resolved, format: str | None = None):
            return DummyContainer()

    monkeypatch.setattr(
        "ray.llm._internal.batch.stages.prepare_video_stage._av_mod", DummyAV
    )
    monkeypatch.setattr(
        "ray.llm._internal.batch.stages.prepare_video_stage._PIL_Image",
        DummyImage,
    )
    return DummyAV


@pytest.mark.asyncio
async def test_video_processor_processes_http_urls(mock_http, mock_av):
    processor = VideoProcessor()
    results = await processor.process(["http://example.com/video.mp4"])
    meta = results[0]["meta"]
    assert meta["failed"] is False
    assert meta["video_num_frames"] > 0
    assert meta["frame_timestamps"][0] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_video_processor_uses_disk_cache(mock_http, mock_av, tmp_path):
    processor = VideoProcessor(cache_dir=str(tmp_path))
    await processor.process(["http://example.com/video.mp4"])
    assert mock_http.download_file.call_count == 1


@pytest.mark.asyncio
async def test_video_processor_only_allows_http(mock_http, mock_av):
    processor = VideoProcessor()
    results = await processor.process(["file:///tmp/video.mp4"])
    meta = results[0]["meta"]
    assert meta["failed"] is True
    assert meta["error_type"] == "ValueError"


@pytest.mark.asyncio
async def test_video_processor_reports_missing_pyav(mock_http, monkeypatch):
    monkeypatch.setattr(
        "ray.llm._internal.batch.stages.prepare_video_stage._av_mod", None
    )
    monkeypatch.setattr(
        "ray.llm._internal.batch.stages.prepare_video_stage._PIL_Image", object()
    )
    processor = VideoProcessor()
    results = await processor.process(["http://example.com/video.mp4"])
    meta = results[0]["meta"]
    assert meta["failed"] is True
    assert meta["error_type"] == "ImportError"


@pytest.mark.asyncio
async def test_udf_batches_rows(mock_http, mock_av):
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["video_urls"],
        sampling_fps=2.0,
    )
    batch = {
        "__data": [
            {"video_urls": ["http://example.com/a.mp4", "http://example.com/b.mp4"]},
            {"video_urls": []},
        ]
    }
    outputs = []
    async for out in udf(batch):
        outputs.append(out["__data"])
    assert len(outputs) == 1
    rows = outputs[0]
    assert len(rows[0]["video"]) == 2
    assert "video" not in rows[1]


@pytest.mark.asyncio
async def test_udf_requires_video_urls(mock_http, mock_av):
    udf = PrepareVideoUDF(
        data_column="__data",
        expected_input_keys=["video_urls"],
    )
    batch = {"__data": [{}]}
    with pytest.raises(ValueError):
        async for _ in udf(batch):
            pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
