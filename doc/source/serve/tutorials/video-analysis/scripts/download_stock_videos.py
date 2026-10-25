#!/usr/bin/env python3
"""
Download stock videos from Pexels and upload to S3 (async version).

Videos are normalized to consistent specs before upload for predictable performance.

Usage:
    python scripts/download_stock_videos.py --api-key YOUR_KEY --bucket YOUR_BUCKET
"""

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional

import aioboto3
import httpx
from botocore.exceptions import ClientError

from constants import (
    SEARCH_QUERIES,
    PEXELS_API_BASE,
    MAX_CONCURRENT_DOWNLOADS,
    MAX_CONCURRENT_UPLOADS,
    S3_VIDEOS_PREFIX,
    NORMALIZE_WIDTH,
    NORMALIZE_HEIGHT,
    NORMALIZE_FPS,
)

from utils.s3 import get_s3_region


def normalize_video(
    input_path: Path,
    output_path: Path,
    width: int = NORMALIZE_WIDTH,
    height: int = NORMALIZE_HEIGHT,
    fps: int = NORMALIZE_FPS,
    preset: str = "fast",
) -> bool:
    """
    Normalize video to consistent specs using ffmpeg.
    
    Applies:
    - Resolution scaling with letterboxing to preserve aspect ratio
    - Consistent FPS
    - H.264 codec with main profile
    - 1-second GOP for fast seeking
    - Removes audio
    
    Args:
        input_path: Path to input video
        output_path: Path for normalized output
        width: Target width (default 1280)
        height: Target height (default 720)
        fps: Target FPS (default 30)
        preset: x264 encoding preset (default "fast")
    
    Returns:
        True if successful, False otherwise
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-vf", f"scale={width}:{height}:force_original_aspect_ratio=decrease,pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,fps={fps}",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-profile:v", "main",
        "-preset", preset,
        "-g", str(fps),  # GOP size = 1 second
        "-keyint_min", str(fps),
        "-sc_threshold", "0",
        "-movflags", "+faststart",
        "-an",  # Remove audio
        str(output_path),
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"      ⚠️  ffmpeg normalization failed: {result.stderr[:200]}")
        return False
    return True


async def search_videos(
    client: httpx.AsyncClient,
    api_key: str,
    query: str,
    per_page: int = 5
) -> list[dict]:
    """Search for videos on Pexels."""
    headers = {"Authorization": api_key}
    params = {
        "query": query,
        "per_page": per_page,
        "orientation": "landscape",
        "size": "medium",
    }
    
    try:
        response = await client.get(
            f"{PEXELS_API_BASE}/search",
            headers=headers,
            params=params,
            timeout=30.0
        )
        response.raise_for_status()
        data = response.json()
        return data.get("videos", [])
    except httpx.HTTPError as e:
        print(f"  ⚠️  Error searching for '{query}': {e}")
        return []


def get_best_video_file(video: dict, max_width: int = 1280) -> Optional[dict]:
    """Get the best quality video file under max_width."""
    video_files = video.get("video_files", [])
    
    # Filter to reasonable sizes and sort by quality
    suitable = [
        vf for vf in video_files
        if vf.get("width", 0) <= max_width and vf.get("quality") in ("hd", "sd")
    ]
    
    if not suitable:
        suitable = video_files
    
    if not suitable:
        return None
    
    # Sort by width descending (prefer higher quality)
    suitable.sort(key=lambda x: x.get("width", 0), reverse=True)
    return suitable[0]


async def download_video(
    client: httpx.AsyncClient,
    url: str,
    dest_path: Path
) -> bool:
    """Download a video file."""
    try:
        async with client.stream("GET", url, timeout=120.0) as response:
            response.raise_for_status()
            with open(dest_path, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"  ⚠️  Download error: {e}")
        return False


def sanitize_metadata(metadata: dict) -> dict:
    """Sanitize metadata values to ASCII-only for S3."""
    result = {}
    for k, v in metadata.items():
        # Convert to string and encode as ASCII, replacing non-ASCII chars
        val = str(v).encode("ascii", errors="replace").decode("ascii")
        result[k] = val
    return result


async def upload_to_s3(
    s3_client,
    local_path: Path,
    bucket: str,
    s3_key: str,
    metadata: dict
) -> bool:
    """Upload a file to S3 with metadata."""
    try:
        extra_args = {
            "ContentType": "video/mp4",
            "Metadata": sanitize_metadata(metadata)
        }
        await s3_client.upload_file(str(local_path), bucket, s3_key, ExtraArgs=extra_args)
        return True
    except ClientError as e:
        print(f"  ⚠️  S3 upload error: {e}")
        return False


def generate_filename(video: dict, query: str, index: int) -> str:
    """Generate a descriptive filename for the video."""
    clean_query = query.replace(" ", "_").replace("/", "-")[:30]
    video_id = video.get("id", "unknown")
    return f"{clean_query}_{video_id}_{index:02d}.mp4"


async def process_video(
    http_client: httpx.AsyncClient,
    s3_client,
    video: dict,
    index: int,
    temp_dir: Path,
    local_dir: Optional[Path],
    bucket: Optional[str],
    s3_prefix: str,
    download_sem: asyncio.Semaphore,
    upload_sem: asyncio.Semaphore,
    normalize: bool = True,
    normalize_width: int = NORMALIZE_WIDTH,
    normalize_height: int = NORMALIZE_HEIGHT,
    normalize_fps: int = NORMALIZE_FPS,
) -> Optional[dict]:
    """Download, normalize, and upload a single video."""
    video_file = get_best_video_file(video)
    if not video_file:
        print(f"  {index+1:2d}. ⚠️  No suitable video file found, skipping")
        return None

    filename = generate_filename(video, video["_query"], index)
    raw_path = temp_dir / f"raw_{filename}"
    normalized_path = temp_dir / filename
    
    print(f"  {index+1:2d}. Downloading: {filename}")
    
    download_url = video_file.get("link")
    if not download_url:
        print(f"      ⚠️  No download URL, skipping")
        return None

    # Download with semaphore
    async with download_sem:
        if not await download_video(http_client, download_url, raw_path):
            return None
    
    raw_size_mb = raw_path.stat().st_size / (1024 * 1024)
    print(f"      ✅ Downloaded ({raw_size_mb:.1f} MB)")

    # Normalize video
    if normalize:
        print(f"      🔄 Normalizing to {normalize_width}x{normalize_height}@{normalize_fps}fps...")
        # Run normalization in thread pool to not block event loop
        success = await asyncio.to_thread(
            normalize_video,
            raw_path,
            normalized_path,
            normalize_width,
            normalize_height,
            normalize_fps,
        )
        if not success:
            print(f"      ⚠️  Normalization failed, using original")
            shutil.move(raw_path, normalized_path)
        else:
            normalized_size_mb = normalized_path.stat().st_size / (1024 * 1024)
            print(f"      ✅ Normalized ({raw_size_mb:.1f} MB → {normalized_size_mb:.1f} MB)")
            raw_path.unlink(missing_ok=True)
        
        # Update dimensions to normalized values
        final_width = normalize_width
        final_height = normalize_height
    else:
        # No normalization, just rename
        shutil.move(raw_path, normalized_path)
        final_width = video_file.get("width")
        final_height = video_file.get("height")

    # Copy to local dir if specified
    if local_dir:
        local_path = local_dir / filename
        shutil.copy2(normalized_path, local_path)
        print(f"      📁 Saved locally: {local_path}")

    result = None
    
    # Upload to S3
    if s3_client and bucket:
        s3_key = f"{s3_prefix}{filename}"
        metadata = {
            "pexels_id": str(video.get("id", "")),
            "query": video["_query"],
            "width": str(final_width),
            "height": str(final_height),
            "duration": str(video.get("duration", "")),
            "photographer": video.get("user", {}).get("name", ""),
            "normalized": str(normalize),
        }
        
        async with upload_sem:
            if await upload_to_s3(s3_client, normalized_path, bucket, s3_key, metadata):
                print(f"      ☁️  Uploaded to: s3://{bucket}/{s3_key}")
                result = {
                    "filename": filename,
                    "s3_uri": f"s3://{bucket}/{s3_key}",
                    "s3_key": s3_key,
                    "pexels_id": video.get("id"),
                    "query": video["_query"],
                    "duration": video.get("duration"),
                    "width": final_width,
                    "height": final_height,
                }
    elif local_dir:
        # Local only mode
        result = {
            "filename": filename,
            "local_path": str(local_dir / filename),
            "pexels_id": video.get("id"),
            "query": video["_query"],
            "duration": video.get("duration"),
            "width": final_width,
            "height": final_height,
        }

    # Clean up temp file
    normalized_path.unlink(missing_ok=True)
    
    return result


async def download_sample_videos(
    api_key: str | None = None,
    bucket: str | None = None,
    total: int = 20,
    per_query: int = 1,
    local_dir: str | None = None,
    dry_run: bool = False,
    skip_s3: bool = False,
    normalize: bool = True,
    width: int = NORMALIZE_WIDTH,
    height: int = NORMALIZE_HEIGHT,
    fps: int = NORMALIZE_FPS,
    s3_prefix: str = S3_VIDEOS_PREFIX,
    overwrite: bool = True,
) -> list[str]:
    """Download videos from Pexels, normalize them, and upload to S3.
    
    If a manifest already exists in S3, returns the existing video paths
    without downloading new videos.
    
    Args:
        api_key: Pexels API key. Falls back to PEXELS_API_KEY env var.
        bucket: S3 bucket name. Falls back to S3_BUCKET env var.
        total: Total number of videos to download.
        per_query: Number of videos per search query.
        local_dir: Optional local directory to save videos.
        dry_run: If True, only show what would be downloaded.
        skip_s3: If True, skip S3 upload (local download only).
        normalize: If True, normalize videos to consistent specs.
        width: Normalized video width.
        height: Normalized video height.
        fps: Normalized video FPS.
        
    Returns:
        List of video paths (S3 URIs or local paths).
    """
    # Get configuration from args or environment
    api_key = api_key or os.environ.get("PEXELS_API_KEY")
    bucket = bucket or os.environ.get("S3_BUCKET")
    
    if not bucket and not skip_s3:
        print("❌ Error: S3 bucket required (--bucket or S3_BUCKET)")
        print("   Set it or use --skip-s3 to only download locally")
        sys.exit(1)

    # Setup S3 session
    session = aioboto3.Session(region_name=get_s3_region(bucket))
    s3_client = None
    
    if not skip_s3:
        async with session.client("s3") as s3:
            try:
                await s3.head_bucket(Bucket=bucket)
                print(f"✅ S3 bucket '{bucket}' accessible")
            except ClientError as e:
                print(f"❌ Error: Cannot access S3 bucket '{bucket}': {e}")
                sys.exit(1)
            
            if overwrite:
                print("🔄 Overwriting existing manifest")
                await s3.delete_object(Bucket=bucket, Key=f"{s3_prefix}manifest.json")

            # Check if manifest already exists - return early if so
            manifest_key = f"{s3_prefix}manifest.json"
            try:
                response = await s3.get_object(Bucket=bucket, Key=manifest_key)
                manifest_data = await response["Body"].read()
                manifest = json.loads(manifest_data.decode("utf-8"))
                
                # Extract paths from existing manifest
                video_paths = []
                for v in manifest.get("videos", []):
                    if "s3_uri" in v:
                        video_paths.append(v["s3_uri"])
                    elif "local_path" in v:
                        video_paths.append(v["local_path"])
                
                print(f"✅ Found existing manifest with {len(video_paths)} videos in S3")
                print(f"   Skipping Pexels API download")
                return video_paths
            except ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchKey":
                    raise
                # Manifest doesn't exist, continue with download
                print("📥 No existing manifest found, will download from Pexels")
    
    # Need API key for downloading
    if not api_key:
        print("❌ Error: Pexels API key required (--api-key or PEXELS_API_KEY)")
        print("   Get your free API key at: https://www.pexels.com/api/")
        sys.exit(1)

    # Create local directory if specified
    local_dir_path = None
    if local_dir:
        local_dir_path = Path(local_dir)
        local_dir_path.mkdir(parents=True, exist_ok=True)
        print(f"📁 Local directory: {local_dir_path}")

    # Create temp directory for downloads
    temp_dir = Path(tempfile.mkdtemp(prefix="pexels_videos_"))
    print(f"📁 Temp directory: {temp_dir}")

    # Track downloaded videos
    video_ids_seen = set()

    print(f"\n🔍 Searching for {total} videos across {len(SEARCH_QUERIES)} queries...\n")

    # Search and collect videos concurrently
    all_videos = []
    
    async with httpx.AsyncClient() as http_client:
        # Search all queries concurrently
        search_tasks = [
            search_videos(http_client, api_key, query, per_page=per_query + 2)
            for query in SEARCH_QUERIES
        ]
        
        results = await asyncio.gather(*search_tasks)
        
        for query, videos in zip(SEARCH_QUERIES, results):
            print(f"  Found {len(videos)} for '{query}'")
            for video in videos:
                if len(all_videos) >= total:
                    break
                video_id = video.get("id")
                if video_id and video_id not in video_ids_seen:
                    video_ids_seen.add(video_id)
                    video["_query"] = query
                    all_videos.append(video)

    all_videos = all_videos[:total]
    print(f"\n📹 Selected {len(all_videos)} unique videos\n")

    if dry_run:
        print("🔍 DRY RUN - Would download these videos:\n")
        for i, video in enumerate(all_videos):
            video_file = get_best_video_file(video)
            if video_file:
                filename = generate_filename(video, video["_query"], i)
                print(f"  {i+1:2d}. {filename}")
                print(f"      URL: {video_file.get('link', 'N/A')[:80]}...")
                print(f"      Size: {video_file.get('width')}x{video_file.get('height')}")
                print()
        return []

    # Download and upload videos concurrently
    if normalize:
        print(f"⬇️  Downloading, normalizing ({width}x{height}@{fps}fps), and uploading videos...\n")
    else:
        print("⬇️  Downloading and uploading videos (no normalization)...\n")
    
    download_sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    upload_sem = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    
    downloaded_videos = []
    
    async with httpx.AsyncClient() as http_client:
        if skip_s3:
            # No S3, just download
            tasks = [
                process_video(
                    http_client, None, video, i, temp_dir, local_dir_path,
                    None, s3_prefix, download_sem, upload_sem,
                    normalize=normalize,
                    normalize_width=width,
                    normalize_height=height,
                    normalize_fps=fps,
                )
                for i, video in enumerate(all_videos)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            downloaded_videos = [r for r in results if r is not None and not isinstance(r, Exception)]
            for r in results:
                if isinstance(r, Exception):
                    print(f"  ⚠️  Task failed: {r}")
        else:
            # With S3
            async with session.client("s3") as s3_client:
                tasks = [
                    process_video(
                        http_client, s3_client, video, i, temp_dir, local_dir_path,
                        bucket, s3_prefix, download_sem, upload_sem,
                        normalize=normalize,
                        normalize_width=width,
                        normalize_height=height,
                        normalize_fps=fps,
                    )
                    for i, video in enumerate(all_videos)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                downloaded_videos = [r for r in results if r is not None and not isinstance(r, Exception)]
                # Log any exceptions
                for r in results:
                    if isinstance(r, Exception):
                        print(f"  ⚠️  Task failed: {r}")

    # Save manifest
    manifest = {
        "total_videos": len(downloaded_videos),
        "s3_bucket": bucket if not skip_s3 else None,
        "s3_prefix": s3_prefix if not skip_s3 else None,
        "local_dir": str(local_dir_path) if local_dir_path else None,
        "normalized": normalize,
        "normalize_settings": {
            "width": width,
            "height": height,
            "fps": fps,
        } if normalize else None,
        "videos": downloaded_videos,
    }

    manifest_path = Path("video_manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\n📋 Manifest saved to: {manifest_path}")

    # Also upload manifest to S3
    if not skip_s3 and bucket:
        async with session.client("s3") as s3_client:
            manifest_s3_key = f"{s3_prefix}manifest.json"
            try:
                await s3_client.put_object(
                    Bucket=bucket,
                    Key=manifest_s3_key,
                    Body=json.dumps(manifest, indent=2),
                    ContentType="application/json"
                )
                print(f"☁️  Manifest uploaded to: s3://{bucket}/{manifest_s3_key}")
            except ClientError as e:
                print(f"⚠️  Failed to upload manifest: {e}")

    # Cleanup temp dir
    try:
        temp_dir.rmdir()
    except OSError:
        pass  # May not be empty if some downloads failed

    print(f"\n✅ Done! Processed {len(downloaded_videos)} videos.")
    
    # Extract paths from downloaded videos
    video_paths = []
    for v in downloaded_videos:
        if "s3_uri" in v:
            video_paths.append(v["s3_uri"])
        elif "local_path" in v:
            video_paths.append(v["local_path"])
    
    if video_paths:
        print("\n📝 Sample paths for testing:")
        for path in video_paths[:5]:
            print(f"   {path}")
    
    return video_paths


def main():
    parser = argparse.ArgumentParser(
        description="Download Pexels videos, normalize them, and upload to S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Normalization applies:
  - Resolution scaling with letterboxing (preserves aspect ratio)
  - Consistent FPS
  - H.264 codec (libx264, main profile)
  - 1-second GOP for fast seeking
  - Removes audio

Examples:
  # Download and normalize to default 1280x720@30fps
  python scripts/download_stock_videos.py --api-key KEY --bucket BUCKET

  # Custom resolution
  python scripts/download_stock_videos.py --api-key KEY --bucket BUCKET --width 1920 --height 1080 --fps 24

  # Skip normalization (upload original files)
  python scripts/download_stock_videos.py --api-key KEY --bucket BUCKET --no-normalize
        """
    )
    parser.add_argument("--api-key", type=str, help="Pexels API key (or set PEXELS_API_KEY)")
    parser.add_argument("--bucket", type=str, help="S3 bucket name (or set S3_BUCKET)")
    parser.add_argument("--total", type=int, default=20, help="Total videos to download")
    parser.add_argument("--per-query", type=int, default=1, help="Videos per search query")
    parser.add_argument("--local-dir", type=str, help="Also save videos locally to this directory")
    parser.add_argument("--dry-run", action="store_true", help="Just show what would be downloaded")
    parser.add_argument("--skip-s3", action="store_true", help="Skip S3 upload, only download locally")
    
    # Normalization options
    parser.add_argument("--no-normalize", action="store_true", 
                        help="Skip video normalization (upload original files)")
    parser.add_argument("--width", type=int, default=NORMALIZE_WIDTH,
                        help=f"Normalized video width (default: {NORMALIZE_WIDTH})")
    parser.add_argument("--height", type=int, default=NORMALIZE_HEIGHT,
                        help=f"Normalized video height (default: {NORMALIZE_HEIGHT})")
    parser.add_argument("--fps", type=int, default=NORMALIZE_FPS,
                        help=f"Normalized video FPS (default: {NORMALIZE_FPS})")
    
    args = parser.parse_args()

    asyncio.run(download_sample_videos(
        api_key=args.api_key,
        bucket=args.bucket,
        total=args.total,
        per_query=args.per_query,
        local_dir=args.local_dir,
        dry_run=args.dry_run,
        skip_s3=args.skip_s3,
        normalize=not args.no_normalize,
        width=args.width,
        height=args.height,
        fps=args.fps,
    ))


if __name__ == "__main__":
    main()
