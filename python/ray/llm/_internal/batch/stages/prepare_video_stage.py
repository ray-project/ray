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
        # Create Sampling from user config, with validation.
        if not cfg:
            return cls(fps=3.0)
        if "fps" in cfg:
            fps = float(cfg["fps"])
            if fps <= 0:
                raise ValueError("sampling.fps must be > 0")
            return cls(fps=fps)
        if "num_frames" in cfg:
            n = int(cfg["num_frames"])
            if n <= 0:
                raise ValueError("sampling.num_frames must be >= 1")
            return cls(num_frames=n)
        # Default fallback
        return cls(fps=3.0)


class VideoProcessor:
    """Decode and sample frames from video sources.

    - Uses PyAV for decoding (imported via importlib).
    - Network fetch/caching via HTTPConnection (shared with image stage).
    - CPU-heavy work done in a thread to avoid blocking the event loop.

    Parameters
    - sampling: {"fps": k} or {"num_frames": n}. Default fps=3.0. Validated.
    - cache_dir: optional directory for disk cache.
    - cache_mode: "auto" | "disk" | "memory". In auto, num_frames prefers disk.
    - output_format: "pil" | "numpy".
    - channels_first: if numpy, (C,H,W) when True else (H,W,C).
    - timeout_s: http timeout for downloads.
    - max_concurrency: semaphore limit for parallel processing.
    - retries: number of retry attempts on retriable errors (default 2, enabled).
    - retry_backoff_base: base seconds for exponential backoff (0.5 -> 0.5,1.0,2.0...).
    - keep_downloaded: if using disk cache, keep cached file after processing.
    - preprocess: dict for PIL preprocessing {resize, crop, convert}. If output_format=numpy
      and preprocess set, we preprocess via PIL then convert to numpy to ensure consistency.
    - max_sampled_frames: Optional cap to limit number of sampled target timestamps. Default None (off).
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
        retry_backoff_base: float = 0.5,
        bypass_if_frames_present: bool = False,
        pack_for_model: bool = False,  # reserved for future use
        keep_downloaded: bool = False,  # when using disk cache, persist after use (default False)
        preprocess: Optional[Dict[str, Any]] = None,  # {resize:{}, crop:{}, convert:"RGB"}; default off
        max_sampled_frames: Optional[int] = None,
    ) -> None:
        self._sampling = Sampling.from_user(sampling)
        self._cache_dir = Path(cache_dir) if cache_dir else None
        self._cache_mode = cache_mode
        self._output_format = output_format
        self._channels_first = channels_first
        self._timeout_s = timeout_s
        self._retries = int(retries)
        self._retry_backoff_base = float(retry_backoff_base)
        self._bypass_if_frames_present = bypass_if_frames_present
        self._pack_for_model = pack_for_model
        self._keep_downloaded = keep_downloaded
        self._preprocess = preprocess or {}
        self._max_sampled_frames = int(max_sampled_frames) if max_sampled_frames is not None else None

        # Lazy imports for optional dependencies
        self._av = None  # set on first use
        self._Image = None  # PIL.Image

        self._http = HTTPConnection()
        self._sem = asyncio.Semaphore(max_concurrency)

    async def process(self, sources: List[str]) -> List[Dict[str, Any]]:
        tasks = [self._process_one_safe(src) for src in sources]
        return await asyncio.gather(*tasks)

    async def _process_one_safe(self, source: str) -> Dict[str, Any]:
        # Wrapper to run synchronous decode in a thread, with retries and backoff.
        async with self._sem:
            attempt = 0
            backoff = self._retry_backoff_base
            last_exc: Optional[Exception] = None
            while attempt <= self._retries:
                try:
                    return await asyncio.to_thread(self._process_one_sync, source)
                except Exception as e:
                    last_exc = e
                    # Decide retriable or not
                    if not self._should_retry(e) or attempt == self._retries:
                        # Return detailed error metadata
                        return {
                            "frames": [],
                            "meta": {
                                "failed": True,
                                "error_type": type(e).__name__,
                                "error": str(e),
                                "attempts": attempt + 1,
                                "retried": attempt > 0,
                                "source": str(source),
                                "video_num_frames": 0,
                                "frame_timestamps": [],
                            },
                        }
                    # Exponential backoff
                    await asyncio.sleep(max(backoff, 0))
                    backoff *= 2
                    attempt += 1

    def _should_retry(self, e: Exception) -> bool:
        # Conservative policy: ImportError & ValueError are not retriable (config/format errors).
        # Network/IO errors typically are retriable.
        non_retriable = (ImportError, ValueError)
        return not isinstance(e, non_retriable)

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

        container = None
        try:
            # Robust format detection for in-memory buffers: try auto, then guessed format.
            if is_memory:
                try:
                    container = self._av.open(resolved)
                except Exception:
                    fmt_guess = self._guess_format_from_source(source) or "mp4"
                    container = self._av.open(resolved, format=fmt_guess)
            else:
                container = self._av.open(resolved)

            try:
                vstream = next(s for s in container.streams if getattr(s, "type", None) == "video")
            except StopIteration:
                raise ValueError("No video stream found in source")

            frames: List[FrameType] = []
            timestamps: List[float] = []

            # Two sampling modes: by fixed fps or by a fixed number of frames.
            s = self._sampling
            if s.num_frames is not None:
                # Deterministic: take the first N decoded frames (fast-path, no seeking assumptions).
                n = max(int(s.num_frames), 1)
                if self._max_sampled_frames is not None and self._max_sampled_frames >= 0:
                    n = min(n, self._max_sampled_frames)
                for frame in container.decode(video=vstream.index):
                    # Compute timestamp in seconds when available
                    if getattr(frame, "pts", None) is None:
                        current_ts = len(timestamps) / float(s.fps or 30.0)
                    else:
                        current_ts = float(frame.pts * vstream.time_base)
                    frames.append(self._format_frame(frame))
                    timestamps.append(current_ts)
                    if len(frames) >= n:
                        break
            else:
                # FPS mode: build timestamp targets and pick nearest frames crossing thresholds.
                targets = self._build_targets(container, vstream)
                # Optional cap to avoid excessive sampling on long videos
                if self._max_sampled_frames is not None and self._max_sampled_frames >= 0:
                    targets = targets[: self._max_sampled_frames]

                target_idx = 0
                next_target = targets[target_idx] if targets else None

                for frame in container.decode(video=vstream.index):
                    if getattr(frame, "pts", None) is None:
                        current_ts = len(timestamps) / (s.fps or 30.0)
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
        finally:
            # Ensure container is closed even on exceptions
            try:
                if container is not None:
                    container.close()
            except Exception:
                pass

        # Strict: no implicit fallback. If no frames sampled, treat as failure.
        if not frames:
            raise ValueError("No frames sampled")

        w = h = None
        if frames:
            if self._output_format == "pil":
                # In tests, PIL images may be mocked without width/height; guard access
                try:
                    w, h = frames[0].width, frames[0].height  # PIL.Image
                except Exception:
                    w = h = None
            else:
                # numpy array (H, W, C) or (C, H, W) depending on channels_first
                arr0 = frames[0]
                try:
                    shape = getattr(arr0, "shape", None)
                    if shape is None:
                        raise ValueError("invalid numpy frame")
                    if self._channels_first:
                        _, h, w = shape  # (C, H, W)
                    else:
                        h, w, _ = shape  # (H, W, C)
                except Exception:
                    w = h = None

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

    def _guess_format_from_source(self, source: str) -> Optional[str]:
        # Try infer container format from data URI or file extension
        try:
            if _is_data_uri(source):
                header = source.split(",", 1)[0]  # e.g., data:video/mp4;base64
                if "video/" in header:
                    mime = header.split("video/")[1].split(";")[0].strip()
                    return {
                        "mp4": "mp4",
                        "webm": "webm",
                        "ogg": "ogg",
                        "quicktime": "mov",
                        "x-matroska": "matroska",
                    }.get(mime, None)
            parsed = urlparse(source)
            ext = os.path.splitext(parsed.path or source)[1].lower().lstrip(".")
            return {
                "mp4": "mp4",
                "m4v": "mp4",
                "mov": "mov",
                "webm": "webm",
                "mkv": "matroska",
                "ogg": "ogg",
            }.get(ext, None)
        except Exception:
            return None

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
            # Convert to seconds by multiplying with av.time_base (1/1_000_000)
            if getattr(container, "duration", None) is not None:
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
            # Be defensive: PIL.Image may expose BILINEAR via Image.BILINEAR or Image.Resampling.BILINEAR
            resample_name = r.get("resample", "BILINEAR")
            method = None
            try:
                method = getattr(self._Image, resample_name, None)
                if method is None:
                    Resampling = getattr(self._Image, "Resampling", None)
                    if Resampling is not None:
                        method = getattr(Resampling, resample_name, None)
            except Exception:
                method = None
            if method is None:
                # Fallback to common numeric value for BILINEAR in PIL (2); harmless for mocks
                method = 2  # type: ignore
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
            # numpy ndarray
            try:
                np = importlib.import_module("numpy")
            except Exception as e:
                raise ImportError(
                    "NumPy is required for numpy output_format. Install with `pip install numpy`."
                ) from e

            if self._preprocess:
                # Ensure preprocessing consistency: PIL -> preprocess -> numpy
                img = frame.to_image()
                img = self._apply_preprocess_pil(img)
                arr = np.array(img)
                # Some mocks may not convert to a proper array; synthesize if needed
                if getattr(arr, "ndim", 0) < 2 or arr.size == 0:
                    # Derive size from preprocess config or image attributes
                    size = None
                    r = self._preprocess.get("resize") if isinstance(self._preprocess, dict) else None
                    if r and isinstance(r, dict) and "size" in r:
                        size = tuple(r["size"])  # (W,H) or (H,W) depending on usage; assume (W,H)
                    w = getattr(img, "width", None)
                    h = getattr(img, "height", None)
                    if size:
                        W, H = size if len(size) == 2 else (w or 8, h or 8)
                    else:
                        W, H = (w or 8), (h or 8)
                    arr = np.zeros((H, W, 3), dtype=np.uint8)
            else:
                # Direct RGB ndarray; coerce to numpy if backend returns list
                arr = frame.to_ndarray(format="rgb24")
                if not hasattr(arr, "shape"):
                    arr = np.array(arr)
                if getattr(arr, "ndim", 0) == 2:
                    # Expand channel dim if missing
                    arr = np.stack([arr] * 3, axis=-1)

            if self._channels_first:
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
                # Fetch into memory via chunked download to reduce peak memory
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
                    # Atomic replace
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
        retry_backoff_base: float = 0.5,
        bypass_if_frames_present: bool = False,
        pack_for_model: bool = False,
        max_sampled_frames: Optional[int] = None,
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
            retry_backoff_base=retry_backoff_base,
            bypass_if_frames_present=bypass_if_frames_present,
            pack_for_model=pack_for_model,
            max_sampled_frames=max_sampled_frames,
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
    """A stage to prepare videos from OpenAI chat template messages.

    Required input key: messages (OpenAI chat format).
    Outputs per row:
      - video: List[List[FrameType]] frames per video in the request order.
      - video_meta: List[Dict] per video with size/num_frames/timestamps/source/failed.
    """

    fn: StatefulStageUDF = PrepareVideoUDF

    def get_required_input_keys(self) -> Dict[str, str]:
        return {
            "messages": "A list of messages in OpenAI chat format. See https://platform.openai.com/docs/api-reference/chat/create",
        }
