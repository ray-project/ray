"""Video loading and frame sampling utilities using ffmpeg."""

import asyncio
import json
import subprocess
from dataclasses import dataclass
from typing import Optional

import numpy as np
from PIL import Image

from constants import NUM_WORKERS


@dataclass
class VideoMetadata:
    """Video metadata extracted from ffprobe."""
    duration: float  # seconds
    fps: float
    width: int
    height: int
    num_frames: int


def get_video_metadata(video_path: str) -> VideoMetadata:
    """Get video metadata using ffprobe. Works with local files and URLs."""
    # Use JSON output for reliable field parsing (CSV order is unpredictable)
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height,r_frame_rate,nb_frames,duration",
        "-of", "json",
        video_path,
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)
    stream = data["streams"][0]
    
    width = int(stream["width"])
    height = int(stream["height"])
    
    # Parse frame rate (can be "30/1" or "29.97")
    fps_str = stream["r_frame_rate"]
    if "/" in fps_str:
        num, den = fps_str.split("/")
        fps = float(num) / float(den)
    else:
        fps = float(fps_str)
    
    # nb_frames might be N/A for some formats
    try:
        num_frames = int(stream.get("nb_frames", 0))
    except (ValueError, TypeError):
        num_frames = 0
    
    # Duration might be in stream or need to be fetched from format
    try:
        duration = float(stream.get("duration", 0))
    except (ValueError, TypeError):
        duration = 0
    
    if duration == 0:
        # Fallback: get duration from format
        cmd2 = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "json",
            video_path,
        ]
        result2 = subprocess.run(cmd2, capture_output=True, text=True, check=True)
        data2 = json.loads(result2.stdout)
        duration = float(data2["format"]["duration"])
    
    if num_frames == 0:
        num_frames = int(duration * fps)
    
    return VideoMetadata(
        duration=duration,
        fps=fps,
        width=width,
        height=height,
        num_frames=num_frames,
    )


def extract_frames_ffmpeg(
    video_path: str,
    start_time: float,
    duration: float,
    num_frames: int,
    target_size: int = 384,
    ffmpeg_threads: int = 0,
) -> np.ndarray:
    """
    Extract frames from a video segment using ffmpeg.
    
    Works with local files and URLs (including presigned S3 URLs).
    
    Args:
        video_path: Path to video file or URL
        start_time: Start time in seconds
        duration: Duration to extract in seconds
        num_frames: Number of frames to extract (uniformly sampled)
        target_size: Output frame size (square)
        ffmpeg_threads: Number of threads for FFmpeg (0 = auto)
    
    Returns:
        np.ndarray of shape (num_frames, target_size, target_size, 3) uint8 RGB
    """
    # Calculate output fps to get exactly num_frames
    output_fps = num_frames / duration if duration > 0 else num_frames
    
    cmd = [
        "ffmpeg",
        "-threads", str(ffmpeg_threads),
        "-ss", str(start_time),
        "-t", str(duration),
        "-i", video_path,
        "-vf", f"fps={output_fps},scale={target_size}:{target_size}",
        "-pix_fmt", "rgb24",
        "-f", "rawvideo",
        "-",
    ]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        check=True,
    )
    
    # Parse raw video frames
    frame_size = target_size * target_size * 3
    raw_data = result.stdout
    actual_frames = len(raw_data) // frame_size
    
    if actual_frames == 0:
        raise ValueError(f"No frames extracted from {video_path} at {start_time}s")
    
    frames = np.frombuffer(raw_data[:actual_frames * frame_size], dtype=np.uint8)
    frames = frames.reshape(actual_frames, target_size, target_size, 3)
    
    # Pad or truncate to exact num_frames
    if len(frames) < num_frames:
        # Pad by repeating last frame
        padding = np.tile(frames[-1:], (num_frames - len(frames), 1, 1, 1))
        frames = np.concatenate([frames, padding], axis=0)
    elif len(frames) > num_frames:
        frames = frames[:num_frames]
    
    return frames


@dataclass
class VideoChunk:
    """Represents a chunk of video to process."""
    index: int
    start_time: float
    duration: float
    frames: Optional[np.ndarray] = None


async def extract_frames_async(
    video_path: str,
    start_time: float,
    duration: float,
    num_frames: int,
    target_size: int = 384,
    ffmpeg_threads: int = 0,
) -> np.ndarray:
    """Async wrapper for extract_frames_ffmpeg using thread pool."""
    return await asyncio.to_thread(
        extract_frames_ffmpeg,
        video_path,
        start_time,
        duration,
        num_frames,
        target_size,
        ffmpeg_threads,
    )


def _extract_all_chunks_single_ffmpeg(
    video_path: str,
    chunk_defs: list[tuple[int, float, float]],
    num_frames_per_chunk: int,
    target_size: int,
    ffmpeg_threads: int = 0,
) -> list[np.ndarray]:
    """
    Extract frames for ALL chunks in a single FFmpeg call.
    
    Uses the select filter to pick specific frame timestamps, avoiding
    multiple process spawns and file seeks.
    
    Args:
        video_path: Path to video file or URL
        chunk_defs: List of (index, start_time, duration) tuples
        num_frames_per_chunk: Frames to extract per chunk
        target_size: Output frame size (square)
        ffmpeg_threads: Number of threads for FFmpeg (0 = auto)
    
    Returns:
        List of numpy arrays, one per chunk
    """
    # Build list of all timestamps to extract
    all_timestamps = []
    for idx, start, duration in chunk_defs:
        # Uniformly sample timestamps within each chunk
        for i in range(num_frames_per_chunk):
            t = start + (i * duration / num_frames_per_chunk)
            all_timestamps.append(t)
    
    if not all_timestamps:
        return []
    
    # Build select filter expression: select frames nearest to our timestamps
    # Using eq(n,frame_num) would require knowing frame numbers, so instead
    # we use pts-based selection with a small tolerance
    # The 'select' filter with 'lt(prev_pts,T)*gte(pts,T)' picks first frame >= T
    
    # For efficiency, we'll extract at a high fps and pick specific frames,
    # or use the thumbnail filter. But simplest: extract all frames near our
    # timestamps using the 'select' filter.
    
    # Build the select expression for all timestamps
    # select='eq(n,0)+eq(n,10)+eq(n,20)...' but we need PTS-based selection
    # Better approach: use fps filter to get enough frames, then select in numpy
    
    # Calculate total time span and required fps
    min_t = min(all_timestamps)
    max_t = max(all_timestamps)
    total_duration = max_t - min_t + 0.1  # small buffer
    
    # We need at least len(all_timestamps) frames over total_duration
    # But we want to be precise, so let's use select filter with expressions
    
    # Build select expression: for each timestamp T, select frame where pts >= T and prev_pts < T
    # This is complex. Simpler approach: output frames at specific PTS values.
    
    # Most efficient single-pass approach: use the 'select' filter with timestamp checks
    # select='between(t,T1-eps,T1+eps)+between(t,T2-eps,T2+eps)+...'
    eps = 0.02  # 20ms tolerance for frame selection
    
    select_parts = [f"between(t,{t-eps},{t+eps})" for t in all_timestamps]
    select_expr = "+".join(select_parts)
    
    cmd = [
        "ffmpeg",
        "-threads", str(ffmpeg_threads),
        "-i", video_path,
        "-vf", f"select='{select_expr}',scale={target_size}:{target_size}",
        "-vsync", "vfr",  # Variable frame rate to preserve selected frames
        "-pix_fmt", "rgb24",
        "-f", "rawvideo",
        "-",
    ]
    
    result = subprocess.run(cmd, capture_output=True, check=True)
    
    # Parse raw video frames
    frame_size = target_size * target_size * 3
    raw_data = result.stdout
    total_frames = len(raw_data) // frame_size
    
    if total_frames == 0:
        raise ValueError(f"No frames extracted from {video_path}")
    
    all_frames = np.frombuffer(raw_data[:total_frames * frame_size], dtype=np.uint8)
    all_frames = all_frames.reshape(total_frames, target_size, target_size, 3)
    
    # Split into chunks
    chunk_frames = []
    frame_idx = 0
    
    for idx, start, duration in chunk_defs:
        # Take num_frames_per_chunk frames for this chunk
        end_idx = min(frame_idx + num_frames_per_chunk, total_frames)
        chunk_data = all_frames[frame_idx:end_idx]
        
        # Pad if needed
        if len(chunk_data) < num_frames_per_chunk:
            if len(chunk_data) == 0:
                # No frames for this chunk, create black frames
                chunk_data = np.zeros((num_frames_per_chunk, target_size, target_size, 3), dtype=np.uint8)
            else:
                padding = np.tile(chunk_data[-1:], (num_frames_per_chunk - len(chunk_data), 1, 1, 1))
                chunk_data = np.concatenate([chunk_data, padding], axis=0)
        
        chunk_frames.append(chunk_data)
        frame_idx = end_idx
    
    return chunk_frames


async def chunk_video_async(
    video_path: str,
    chunk_duration: float = 10.0,
    num_frames_per_chunk: int = 16,
    target_size: int = 384,
    use_single_ffmpeg: bool = False,
    ffmpeg_threads: int = 0,
) -> list[VideoChunk]:
    """
    Split video into fixed-duration chunks with frame extraction.
    
    Works with local files and URLs (including presigned S3 URLs).
    
    Args:
        video_path: Path to video file or URL
        chunk_duration: Duration of each chunk in seconds
        num_frames_per_chunk: Frames to extract per chunk
        target_size: Frame size
        use_single_ffmpeg: If True, extract all chunks in one FFmpeg call (faster).
                          If False, use parallel FFmpeg calls per chunk.
        ffmpeg_threads: Number of threads for FFmpeg decoding (0 = auto)
    
    Returns:
        List of VideoChunk with frames loaded
    """
    # Get metadata (sync call, fast)
    metadata = await asyncio.to_thread(get_video_metadata, video_path)
    
    # Build chunk definitions
    chunk_defs = []
    start = 0.0
    index = 0
    
    while start < metadata.duration:
        duration = min(chunk_duration, metadata.duration - start)
        
        # Skip very short final chunks
        if duration < 0.5:
            break
        
        chunk_defs.append((index, start, duration))
        start += chunk_duration
        index += 1
    
    if not chunk_defs:
        return []
    
    if use_single_ffmpeg:
        # Single FFmpeg call - more efficient, especially for URLs
        frame_results = await asyncio.to_thread(
            _extract_all_chunks_single_ffmpeg,
            video_path,
            chunk_defs,
            num_frames_per_chunk,
            target_size,
            ffmpeg_threads,
        )
    else:
        # Multiple parallel FFmpeg calls, limited to NUM_WORKERS concurrency
        semaphore = asyncio.Semaphore(NUM_WORKERS)

        async def extract_with_limit(idx, start, duration):
            async with semaphore:
                return await extract_frames_async(
                    video_path,
                    start_time=start,
                    duration=duration,
                    num_frames=num_frames_per_chunk,
                    target_size=target_size,
                    ffmpeg_threads=ffmpeg_threads,
                )

        extraction_tasks = [
            extract_with_limit(idx, start, duration)
            for idx, start, duration in chunk_defs
        ]
        frame_results = await asyncio.gather(*extraction_tasks)

    # Build chunk objects
    chunks = [
        VideoChunk(
            index=idx,
            start_time=start,
            duration=duration,
            frames=frames,
        )
        for (idx, start, duration), frames in zip(chunk_defs, frame_results)
    ]
    
    return chunks


def chunk_video(
    video_path: str,
    chunk_duration: float = 10.0,
    num_frames_per_chunk: int = 16,
    target_size: int = 384,
    use_single_ffmpeg: bool = True,
    ffmpeg_threads: int = 0,
) -> list[VideoChunk]:
    """
    Split video into fixed-duration chunks.
    
    Args:
        video_path: Path to video file or URL
        chunk_duration: Duration of each chunk in seconds
        num_frames_per_chunk: Frames to extract per chunk
        target_size: Frame size
        use_single_ffmpeg: If True, extract all chunks in one FFmpeg call (faster).
                          If False, use sequential FFmpeg calls per chunk.
        ffmpeg_threads: Number of threads for FFmpeg decoding (0 = auto)
    
    Returns:
        List of VideoChunk with frames loaded
    """
    metadata = get_video_metadata(video_path)
    
    # Build chunk definitions
    chunk_defs = []
    start = 0.0
    index = 0
    
    while start < metadata.duration:
        duration = min(chunk_duration, metadata.duration - start)
        
        # Skip very short final chunks
        if duration < 0.5:
            break
        
        chunk_defs.append((index, start, duration))
        start += chunk_duration
        index += 1
    
    if not chunk_defs:
        return []
    
    if use_single_ffmpeg:
        # Single FFmpeg call - more efficient, especially for URLs
        frame_results = _extract_all_chunks_single_ffmpeg(
            video_path,
            chunk_defs,
            num_frames_per_chunk,
            target_size,
            ffmpeg_threads,
        )
    else:
        # Sequential FFmpeg calls (original approach)
        frame_results = []
        for idx, start, duration in chunk_defs:
            frames = extract_frames_ffmpeg(
                video_path,
                start_time=start,
                duration=duration,
                num_frames=num_frames_per_chunk,
                target_size=target_size,
                ffmpeg_threads=ffmpeg_threads,
            )
            frame_results.append(frames)
    
    # Build chunk objects
    chunks = [
        VideoChunk(
            index=idx,
            start_time=start,
            duration=duration,
            frames=frames,
        )
        for (idx, start, duration), frames in zip(chunk_defs, frame_results)
    ]
    
    return chunks


def frames_to_pil_list(frames: np.ndarray) -> list[Image.Image]:
    """Convert numpy frames array to list of PIL Images."""
    return [Image.fromarray(frame) for frame in frames]
