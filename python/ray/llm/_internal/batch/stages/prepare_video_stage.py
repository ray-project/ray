"""Prepare Video Stage"""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from ray.llm._internal.batch.stages._util import HTTPConnection
from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF

try:  # pragma: no cover - availability depends on environment
    import av as _av_mod  # type: ignore
except Exception:  # pragma: no cover
    _av_mod = None  # type: ignore

try:  # pragma: no cover
    from PIL import Image as _PIL_Image  # type: ignore
except Exception:  # pragma: no cover
    _PIL_Image = None  # type: ignore

FrameType = Any


def _is_http(url: str) -> bool:
    try:
        return urlparse(url).scheme in ("http", "https")
    except Exception:
        return False


def _sha256_16(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


class VideoProcessor:
    def __init__(
        self, *, sampling_fps: float = 3.0, cache_dir: Optional[str] = None
    ) -> None:
        if sampling_fps <= 0:
            raise ValueError("sampling_fps must be positive")
        self._fps = float(sampling_fps)
        self._cache_dir = Path(cache_dir) if cache_dir else None
        self._http = HTTPConnection()
        self._sem = asyncio.Semaphore(4)

    async def process(self, sources: List[str]) -> List[Dict[str, Any]]:
        if not sources:
            return []
        tasks = [self._process_one_safe(url) for url in sources]
        return await asyncio.gather(*tasks)

    async def _process_one_safe(self, url: str) -> Dict[str, Any]:
        async with self._sem:
            attempt = 0
            backoff = 0.5
            while attempt <= 2:
                try:
                    return await asyncio.to_thread(self._process_one_sync, url)
                except Exception as exc:
                    if attempt == 2 or isinstance(exc, (ImportError, ValueError)):
                        return {
                            "frames": [],
                            "meta": {
                                "failed": True,
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                                "attempts": attempt + 1,
                                "retried": attempt > 0,
                                "source": url,
                                "video_num_frames": 0,
                                "frame_timestamps": [],
                            },
                        }
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    attempt += 1

    def _process_one_sync(self, url: str) -> Dict[str, Any]:
        if _av_mod is None:
            raise ImportError("PyAV is required. Install with `pip install av`.")
        if _PIL_Image is None:
            raise ImportError("Pillow is required. Install with `pip install pillow`.")
        if not _is_http(url):
            raise ValueError("VideoProcessor only supports HTTP(S) URLs.")

        resolved, cleanup_path = self._resolve_source(url)
        container = None
        try:
            container = self._open_container(resolved)
            stream = self._first_video_stream(container)
            frames, timestamps = self._decode_frames(container, stream)
        finally:
            if container is not None:
                container.close()
            if cleanup_path is not None and os.path.exists(cleanup_path):
                os.remove(cleanup_path)

        if not frames:
            raise ValueError("No frames sampled")

        size = self._extract_size(frames)
        return {
            "frames": frames,
            "meta": {
                "failed": False,
                "source": url,
                "video_num_frames": len(frames),
                "frame_timestamps": timestamps,
                "video_size": size,
            },
        }

    def _resolve_source(self, url: str) -> Tuple[Any, Optional[str]]:
        if self._cache_dir is None:
            data = self._http.download_bytes_chunked(url, timeout=30.0)
            return io.BytesIO(data), None
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        target = self._cache_dir / f"video-{_sha256_16(url)}.mp4"
        if target.exists():
            return str(target), None
        tmp_path = target.with_suffix(".tmp")
        self._http.download_file(url, tmp_path, timeout=30.0)
        os.replace(tmp_path, target)
        return str(target), str(target)

    def _open_container(self, resolved: Any) -> Any:
        try:
            return _av_mod.open(resolved)
        except Exception:
            return _av_mod.open(resolved, format="mp4")

    def _first_video_stream(self, container: Any) -> Any:
        for stream in container.streams:
            if getattr(stream, "type", None) == "video":
                return stream
        raise ValueError("No video stream found")

    def _decode_frames(
        self, container: Any, stream: Any
    ) -> Tuple[List[FrameType], List[float]]:
        targets = self._build_targets(container, stream)
        if not targets:
            return [], []
        frames: List[FrameType] = []
        timestamps: List[float] = []
        target_idx = 0
        next_target = targets[target_idx]
        for frame in container.decode(video=stream.index):
            current_ts = self._timestamp_from_frame(frame, stream, len(timestamps))
            if current_ts + 1e-6 < next_target:
                continue
            image = frame.to_image()
            frames.append(image)
            timestamps.append(current_ts)
            target_idx += 1
            if target_idx >= len(targets):
                break
            next_target = targets[target_idx]
        return frames, timestamps

    def _build_targets(self, container: Any, stream: Any) -> List[float]:
        duration_s: Optional[float] = None
        duration_attr = getattr(container, "duration", None)
        if (
            duration_attr is not None
            and getattr(_av_mod, "time_base", None) is not None
        ):
            duration_s = float(duration_attr * _av_mod.time_base)
        if duration_s is None and getattr(stream, "duration", None) is not None:
            duration_s = float(stream.duration * float(stream.time_base))
        if duration_s is None:
            limit = max(int(self._fps * 2), 1)
            return [i / self._fps for i in range(limit)]
        total = max(int(duration_s * self._fps) + 1, 1)
        return [i / self._fps for i in range(total)]

    def _timestamp_from_frame(self, frame: Any, stream: Any, index: int) -> float:
        if getattr(frame, "pts", None) is None:
            return index / self._fps
        return float(frame.pts * stream.time_base)

    def _extract_size(self, frames: List[FrameType]) -> Optional[List[int]]:
        if not frames:
            return None
        first = frames[0]
        width = getattr(first, "width", None)
        height = getattr(first, "height", None)
        if width is None or height is None:
            return None
        return [width, height]


class PrepareVideoUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        *,
        sampling_fps: float = 3.0,
        cache_dir: Optional[str] = None,
    ) -> None:
        super().__init__(data_column, expected_input_keys)
        self._video = VideoProcessor(sampling_fps=sampling_fps, cache_dir=cache_dir)

    def extract_video_sources(self, urls: List[str]) -> List[str]:
        return urls

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        async def handle_row(index: int, row: Dict[str, Any]) -> Dict[str, Any]:
            sources = self.extract_video_sources(row["video_urls"])
            if not sources:
                return {self.IDX_IN_BATCH_COLUMN: index}
            results = await self._video.process(sources)
            return {
                self.IDX_IN_BATCH_COLUMN: index,
                "video": [item["frames"] for item in results],
                "video_meta": [item["meta"] for item in results],
            }

        tasks = [handle_row(i, row) for i, row in enumerate(batch)]
        for coro in asyncio.as_completed(tasks):
            yield await coro


class PrepareVideoStage(StatefulStage):
    fn: StatefulStageUDF = PrepareVideoUDF

    def get_required_input_keys(self) -> Dict[str, str]:
        return {"video_urls": "List[str] of HTTP(S) video URLs."}
