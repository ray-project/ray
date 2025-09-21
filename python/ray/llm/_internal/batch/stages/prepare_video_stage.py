"""Prepare Video Stage

Minimal video preparation stage aligned with PrepareImageStage.

Features (MVP):
- Parse video sources from OpenAI chat-style messages ("video", "video_url").
- Resolve sources via streaming (default) or optional caching (disk/memory).
- Decode via PyAV (FFmpeg) and sample frames by fps or uniform num_frames.
- Return frames (PIL by default, or numpy arrays) and metadata.

Notes:
- PyAV is imported dynamically; if missing, a clear ImportError is raised when used.
- Heavy decode work runs in a thread to avoid blocking the event loop.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
import io
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF
from ray.llm._internal.batch.stages._util import HTTPConnection


# Types
FrameType = Any  # PIL.Image.Image or numpy.ndarray


def _is_http(url: str) -> bool:
    try:
        scheme = urlparse(url).scheme
        return scheme in ("http", "https")
    except Exception:
        return False


def _is_data_uri(url: str) -> bool:
    return isinstance(url, str) and url.startswith("data:")


def _sha256_16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


@dataclass
class Sampling:
    fps: Optional[float] = None  # e.g., 3.0 frames/sec
    num_frames: Optional[int] = None  # e.g., 8 uniformly spaced frames

    @classmethod
    def from_user(cls, cfg: Optional[Dict[str, Any]]) -> "Sampling":
        if not cfg:
            return cls(fps=3.0)
        if "fps" in cfg:
            return cls(fps=float(cfg["fps"]))
        if "num_frames" in cfg:
            return cls(num_frames=int(cfg["num_frames"]))
        # Default fallback
        return cls(fps=3.0)


class VideoProcessor:
    """Decode and sample frames from video sources.

    - Uses PyAV for decoding (imported via importlib).
    - Network fetch/caching via HTTPConnection (shared with image stage).
    - CPU-heavy work done in a thread to avoid blocking the event loop.
    """

    def __init__(
        self,
        *,
        sampling: Optional[Dict[str, Any]] = None,  # {"fps": k} or {"num_frames": n}
        cache_dir: Optional[str] = None,
        cache_mode: str = "auto",  # "auto" | "disk" | "memory"
        output_format: str = "pil",  # "pil" | "numpy"
        channels_first: bool = False,
        timeout_s: float = 30.0,
        max_concurrency: int = 8,
        retries: int = 2,
        bypass_if_frames_present: bool = False,
        pack_for_model: bool = False,  # reserved for future use
        keep_downloaded: bool = False,  # when using disk cache, persist after use (default False)
        preprocess: Optional[Dict[str, Any]] = None,  # {resize:{}, crop:{}, convert:"RGB"}; default off
    ) -> None:
        self._sampling = Sampling.from_user(sampling)
        self._cache_dir = Path(cache_dir) if cache_dir else None
        self._cache_mode = cache_mode
        self._output_format = output_format
        self._channels_first = channels_first
        self._timeout_s = timeout_s
        self._retries = retries
        self._bypass_if_frames_present = bypass_if_frames_present
        self._pack_for_model = pack_for_model
        self._keep_downloaded = keep_downloaded
        self._preprocess = preprocess or {}

        # Lazy imports for optional dependencies
        self._av = None  # set on first use
        self._Image = None  # PIL.Image

        self._http = HTTPConnection()
        self._sem = asyncio.Semaphore(max_concurrency)

    # ------------------------ public async API ------------------------
    async def process(self, sources: List[str]) -> List[Dict[str, Any]]:
        tasks = [self._process_one_safe(src) for src in sources]
        return await asyncio.gather(*tasks)

    # ------------------------ internal helpers ------------------------
    async def _process_one_safe(self, source: str) -> Dict[str, Any]:
        async with self._sem:
            try:
                return await asyncio.to_thread(self._process_one_sync, source)
            except Exception as e:
                return {
                    "frames": [],
                    "meta": {"failed": True, "error": str(e), "source": str(source)},
                }

    def _ensure_deps(self) -> None:
        if self._av is None:
            try:
                self._av = importlib.import_module("av")
            except Exception as e:
                raise ImportError(
                    "PyAV is required for PrepareVideoStage. Install with `pip install av`."
                ) from e
        if self._Image is None:
            try:
                self._Image = importlib.import_module("PIL.Image")
            except Exception as e:
                raise ImportError(
                    "Pillow is required for PrepareVideoStage. Install with `pip install pillow`."
                ) from e

    # Core sync path (runs in thread)
    def _process_one_sync(self, source: str) -> Dict[str, Any]:
        self._ensure_deps()

        resolved, is_memory, cleanup_path = self._resolve_source_for_decode(source)

        container = self._av.open(resolved) if not is_memory else self._av.open(resolved, format="mp4")
        try:
            vstream = next(s for s in container.streams if s.type == "video")
        except StopIteration:
            container.close()
            raise ValueError("No video stream found in source")

        # Build target timestamps based on sampling config
        targets = self._build_targets(container, vstream)

        frames: List[FrameType] = []
        timestamps: List[float] = []

        # Iterate frames once and pick nearest to targets
        # For fps sampling, targets are spaced by 1/fps; for num_frames, uniform across duration
        target_idx = 0
        next_target = targets[target_idx] if targets else None

        for frame in container.decode(video=vstream.index):
            # frame.pts * time_base -> seconds
            if frame.pts is None:
                # Some streams may lack pts; approximate by count
                current_ts = len(timestamps) / (self._sampling.fps or 30.0)
            else:
                current_ts = float(frame.pts * vstream.time_base)

            if next_target is None:
                break

            if current_ts + 1e-6 >= next_target:
                frames.append(self._format_frame(frame))
                timestamps.append(current_ts)
                target_idx += 1
                if target_idx >= len(targets):
                    break
                next_target = targets[target_idx]

        container.close()

        # If we failed to collect any frames (e.g., very short video), try first frame fallback
        if not frames:
            container = self._av.open(resolved) if not is_memory else self._av.open(resolved, format="mp4")
            # re-detect video stream
            try:
                vstream_fb = next(s for s in container.streams if s.type == "video")
            except StopIteration:
                container.close()
                frames = []
                timestamps = []
            else:
                for frame in container.decode(video=vstream_fb.index):
                    frames.append(self._format_frame(frame))
                    ts = (
                        float(frame.pts * vstream_fb.time_base)
                        if frame.pts is not None
                        else 0.0
                    )
                    timestamps.append(ts)
                    break
                container.close()

        w = h = None
        if frames:
            if self._output_format == "pil":
                w, h = frames[0].width, frames[0].height  # PIL.Image
            else:
                # numpy array (H, W, C) or (C, H, W) depending on channels_first
                arr = frames[0]
                if self._channels_first:
                    _, h, w = arr.shape  # (C, H, W)
                else:
                    h, w, _ = arr.shape  # (H, W, C)

        result = {
            "frames": frames,
            "meta": {
                "video_size": [w, h] if (w and h) else None,
                "video_num_frames": len(frames),
                "frame_timestamps": timestamps,
                "source": self._source_repr(source, resolved, is_memory),
                "failed": False,
            },
        }

        # Best-effort cleanup if using disk and not keeping the file
        if cleanup_path is not None and not self._keep_downloaded:
            try:
                os.remove(cleanup_path)
            except Exception:
                pass

        return result

    def _source_repr(self, original: str, resolved: Any, is_memory: bool) -> str:
        try:
            if is_memory:
                return f"memory://{len(resolved.getbuffer())}b"
            return str(resolved)
        except Exception:
            return str(original)

    def _build_targets(self, container: Any, vstream: Any) -> List[float]:
        # Estimate duration in seconds
        duration_s: Optional[float] = None
        try:
            # container.duration is in AV_TIME_BASE units (microseconds)
            # Convert to seconds by multiplying by av.time_base (1/1_000_000)
            if container.duration is not None:
                duration_s = float(container.duration * self._av.time_base)
        except Exception:
            duration_s = None

        if duration_s is None:
            # Fallback: approximate by using stream duration if available
            try:
                if getattr(vstream, "duration", None) is not None:
                    duration_s = float(vstream.duration * float(vstream.time_base))
            except Exception:
                duration_s = None

        # Build targets
        s = self._sampling
        targets: List[float] = []
        if s.fps is not None:
            # Sample at fixed fps until duration if known, else sample up to first N frames (2 seconds heuristic)
            if duration_s is None:
                # Fallback: sample up to ~2 seconds worth
                limit = max(int(s.fps * 2), 1)
                targets = [i / s.fps for i in range(limit)]
            else:
                i = 0
                while True:
                    t = i / s.fps
                    if t > duration_s + 1e-6:
                        break
                    targets.append(t)
                    i += 1
        elif s.num_frames is not None:
            n = max(int(s.num_frames), 1)
            if duration_s is None:
                # Without duration, aim for the first n frames at t=0 (best-effort)
                targets = [0.0 for _ in range(n)]
            else:
                if n == 1:
                    targets = [0.0]
                else:
                    step = duration_s / (n - 1)
                    targets = [i * step for i in range(n)]
        return targets

    def _apply_preprocess_pil(self, img: Any) -> Any:
        # preprocess is optional and off by default
        if not self._preprocess:
            return img
        # resize
        r = self._preprocess.get("resize")
        if r and isinstance(r, dict) and "size" in r:
            resample = r.get("mode", "bilinear")
            method = getattr(self._Image, r.get("resample", "BILINEAR"), self._Image.BILINEAR)
            img = img.resize(tuple(r["size"]), method)
        # crop
        c = self._preprocess.get("crop")
        if c and isinstance(c, dict) and "box" in c:
            img = img.crop(tuple(c["box"]))
        # convert
        conv = self._preprocess.get("convert")
        if isinstance(conv, str):
            img = img.convert(conv)
        return img

    def _format_frame(self, frame: Any) -> FrameType:
        if self._output_format == "pil":
            img = frame.to_image()
            img = self._apply_preprocess_pil(img)
            return img
        else:
            # numpy ndarray in RGB
            arr = frame.to_ndarray(format="rgb24")
            # Note: resize/crop/convert for numpy path is intentionally omitted in MVP
            if self._channels_first:
                # (H, W, C) -> (C, H, W)
                return arr.transpose(2, 0, 1)
            return arr

    def _resolve_source_for_decode(
        self, source: str
    ) -> Tuple[Union[str, io.BytesIO], bool, Optional[str]]:
        """Return (resolved, is_memory, cleanup_path).

        cache_mode:
          - "auto": download to disk if uniform sampling (num_frames) likely needs seek; otherwise stream.
          - "disk": always download to disk when http/https.
          - "memory": fetch into BytesIO when http/https or data URI.
        """
        # data URI: decode
        if _is_data_uri(source):
            try:
                header, b64 = source.split(",", 1)
                raw = base64.b64decode(b64)
                return io.BytesIO(raw), True, None
            except Exception as e:
                raise ValueError(f"Invalid data URI for video: {e}")

        # local file path
        parsed = urlparse(source)
        if parsed.scheme in ("file", "") and os.path.exists(parsed.path or source):
            return parsed.path or source, False, None

        # http/https
        if _is_http(source):
            # Decide caching mode
            use_disk = (
                self._cache_dir is not None
                and (
                    self._cache_mode == "disk"
                    or (
                        self._cache_mode == "auto"
                        and self._sampling.num_frames is not None
                    )
                )
            )
            use_memory = self._cache_mode == "memory"

            if use_memory:
                # Fetch into memory
                # Chunked in-memory download by default to reduce peak memory
                data = self._http.download_bytes_chunked(
                    source, timeout=self._timeout_s
                )
                return io.BytesIO(data), True, None

            if use_disk:
                assert self._cache_dir is not None
                self._cache_dir.mkdir(parents=True, exist_ok=True)
                # Determine extension if any
                ext = os.path.splitext(parsed.path)[1] or ".mp4"
                name = f"{_sha256_16(source)}{ext}"
                final = self._cache_dir / name
                if not final.exists():
                    tmp = self._cache_dir / f".{name}.tmp"
                    self._http.download_file(
                        source, tmp, timeout=self._timeout_s
                    )
                    tmp.replace(final)
                # Return cleanup path so caller can delete if needed
                return str(final), False, str(final)

            # Default: stream directly from URL
            return source, False, None

        # Fallback: treat as path string
        return source, False, None


class PrepareVideoUDF(StatefulStageUDF):
    """UDF for PrepareVideoStage.

    Extract video sources from messages, process via VideoProcessor,
    and emit frames and metadata aligned with PrepareImageStage conventions.
    """

    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        *,
        sampling: Optional[Dict[str, Any]] = None,  # {"fps": 3} or {"num_frames": 8}
        cache_dir: Optional[str] = None,
        cache_mode: str = "auto",
        output_format: str = "pil",
        channels_first: bool = False,
        timeout_s: float = 30.0,
        max_concurrency: int = 8,
        retries: int = 2,
        bypass_if_frames_present: bool = False,
        pack_for_model: bool = False,
    ) -> None:
        super().__init__(data_column, expected_input_keys)
        self._video = VideoProcessor(
            sampling=sampling,
            cache_dir=cache_dir,
            cache_mode=cache_mode,
            output_format=output_format,
            channels_first=channels_first,
            timeout_s=timeout_s,
            max_concurrency=max_concurrency,
            retries=retries,
            bypass_if_frames_present=bypass_if_frames_present,
            pack_for_model=pack_for_model,
        )

    def extract_video_info(self, messages: List[Dict]) -> List[str]:
        sources: List[str] = []
        for message in messages:
            content = message.get("content")
            # Handle PyArrow list-like (e.g., tolist()) as in PrepareImageUDF
            if hasattr(content, "tolist"):
                content = content.tolist()
            if not isinstance(content, list):
                continue
            for item in content:
                t = item.get("type")
                if t == "video":
                    src = item.get("video")
                elif t == "video_url":
                    vurl = item.get("video_url") or {}
                    src = vurl.get("url")
                else:
                    continue
                if isinstance(src, str):
                    sources.append(src)
                else:
                    raise ValueError(f"Cannot handle video type {type(src)}")
        return sources

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        messages_batch = [row["messages"] for row in batch]

        all_video_info = [self.extract_video_info(msgs) for msgs in messages_batch]
        flat_sources = [s for lst in all_video_info for s in lst]

        processed = await self._video.process(flat_sources)

        start = 0
        for idx_in_batch, per_req in enumerate(all_video_info):
            n = len(per_req)
            ret: Dict[str, Any] = {self.IDX_IN_BATCH_COLUMN: idx_in_batch}
            if n > 0:
                chunk = processed[start : start + n]
                start += n
                ret.update(
                    {
                        "video": [c["frames"] for c in chunk],
                        "video_meta": [c["meta"] for c in chunk],
                    }
                )
            yield ret


class PrepareVideoStage(StatefulStage):
    """A stage to prepare videos from OpenAI chat template messages."""

    fn: StatefulStageUDF = PrepareVideoUDF

    def get_required_input_keys(self) -> Dict[str, str]:
        return {
            "messages": "A list of messages in OpenAI chat format. See https://platform.openai.com/docs/api-reference/chat/create",
        }
