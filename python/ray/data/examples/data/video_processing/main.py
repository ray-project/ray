"""CLI example for `video_processor.py`.

Run this script to download (or read) a video source, sample a handful of frames,
and print summary metadata. It accepts both local files and HTTP(S) URLs.

Example:

        python main.py --source https://storage.googleapis.com/ray-demo-assets/video/ray-demo-video.mp4

Install optional dependencies first:

        pip install av pillow numpy
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Dict, Optional

from video_processor import VideoProcessor

DEFAULT_SOURCE = (
    "https://storage.googleapis.com/ray-demo-assets/video/ray-demo-video.mp4"
)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample a few frames from a video with VideoProcessor"
    )
    parser.add_argument(
        "--source",
        "-s",
        default=DEFAULT_SOURCE,
        help=(
            "Video source to process (HTTP(S) URL, data URI, or local path). "
            f"Defaults to {DEFAULT_SOURCE}."
        ),
    )
    sampling = parser.add_mutually_exclusive_group()
    sampling.add_argument(
        "--fps",
        type=float,
        default=None,
        help="Timeline sampling rate in frames-per-second.",
    )
    sampling.add_argument(
        "--num-frames",
        type=int,
        default=None,
        help="Take the first N decoded frames deterministically.",
    )
    return parser.parse_args(argv)


def build_sampling(args: argparse.Namespace) -> Optional[Dict[str, float]]:
    if args.num_frames is not None:
        return {"num_frames": args.num_frames}
    if args.fps is not None:
        return {"fps": args.fps}
    return None


async def run(args: argparse.Namespace) -> None:
    processor = VideoProcessor(
        sampling=build_sampling(args),
    )

    print(f"Processing {args.source} ...", flush=True)
    results = await processor.process([args.source])

    if not results:
        print("No sources returned results.")
        return

    video = results[0]
    frames = video.get("frames", [])
    meta = video.get("meta", {})

    if meta.get("failed"):
        print("Video processing failed:")
        for key, value in meta.items():
            print(f"  {key}: {value}")
        return

    rounded_ts = [round(ts, 2) for ts in meta.get("frame_timestamps", [])]

    print("Success! Summary:")
    print(f"  Frames sampled: {len(frames)}")
    print(f"  Frame timestamps: {rounded_ts}")
    print(f"  Video size (W, H): {meta.get('video_size')}")
    print(f"  Source repr: {meta.get('source')}")

    if frames:
        first = frames[0]
        print(
            f"  First frame type: {type(first).__name__}, mode={getattr(first, 'mode', None)}"
        )


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)


if __name__ == "__main__":
    main()
