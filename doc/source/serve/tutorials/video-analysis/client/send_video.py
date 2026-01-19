#!/usr/bin/env python3
"""
Client script to send video to the Ray Serve API.

Usage:
    python -m client.send_video --video s3://bucket/path/to/video.mp4
    python -m client.send_video --video s3://bucket/video.mp4 --chunk-duration 5.0
    python -m client.send_video --video s3://bucket/video.mp4 --token YOUR_TOKEN
"""

import argparse
import time
import uuid

import httpx


def main():
    parser = argparse.ArgumentParser(description="Send video to Ray Serve API")
    parser.add_argument("--video", type=str, required=True, help="S3 URI: s3://bucket/key")
    parser.add_argument("--stream-id", type=str, default=None, help="Stream ID (random if not provided)")
    parser.add_argument("--num-frames", type=int, default=16, help="Frames per chunk")
    parser.add_argument("--chunk-duration", type=float, default=10.0, help="Chunk duration in seconds")
    parser.add_argument("--url", type=str, default="http://127.0.0.1:8000", help="Server URL")
    parser.add_argument("--token", type=str, default=None, help="Bearer token for Authorization header")
    args = parser.parse_args()
    
    # Generate random stream ID if not provided
    stream_id = args.stream_id or uuid.uuid4().hex[:8]
    
    payload = {
        "stream_id": stream_id,
        "video_path": args.video,
        "num_frames": args.num_frames,
        "chunk_duration": args.chunk_duration,
        "use_batching": True
    }
    
    print(f"📹 Processing video: {args.video}")
    print(f"   Stream ID: {stream_id}")
    print(f"   Chunk duration: {args.chunk_duration}s, Frames/chunk: {args.num_frames}")
    print()
    
    start = time.perf_counter()
    
    headers = {}
    if args.token:
        headers["Authorization"] = f"Bearer {args.token}"
    
    with httpx.Client(timeout=300.0) as client:
        response = client.post(f"{args.url}/analyze", json=payload, headers=headers)
    
    latency_ms = (time.perf_counter() - start) * 1000
    
    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: {response.text}")
        return
    
    result = response.json()
    
    print("=" * 60)
    print("✅ Response")
    print("=" * 60)
    print(f"Stream ID: {result['stream_id']}")
    print(f"Video duration: {result['video_duration']:.1f}s")
    print(f"Chunks processed: {result['num_chunks']}")
    print()
    
    print("🏷️  Top Tags (aggregated):")
    for tag in result["tags"]:
        print(f"   {tag['score']:.3f}  {tag['text']}")
    print()
    
    print("📝 Best Caption:")
    caption = result["retrieval_caption"]
    print(f"   {caption['score']:.3f}  {caption['text']}")
    print()
    
    # Scene changes
    scene_changes = result["scene_changes"]
    print(f"🎬 Scene Changes Detected: {result['num_scene_changes']}")
    if scene_changes:
        for sc in scene_changes:
            print(f"   {sc['timestamp']:6.2f}s  score={sc['score']:.3f}  (chunk {sc['chunk_index']}, frame {sc['frame_index']})")
    else:
        print("   (none detected)")
    print()
    
    # Show per-chunk results
    print("📊 Per-Chunk Results:")
    print("-" * 60)
    for chunk in result["chunks"]:
        print(f"  Chunk {chunk['chunk_index']}: {chunk['start_time']:.1f}s - {chunk['start_time'] + chunk['duration']:.1f}s")
        print(f"    Top tag: {chunk['tags'][0]['text']} ({chunk['tags'][0]['score']:.3f})")
        print(f"    Caption: {chunk['retrieval_caption']['text'][:50]}...")
        num_changes = len(chunk["scene_changes"])
        print(f"    Scene changes: {num_changes}")
    print()
    
    timing = result["timing_ms"]
    print("⏱️  Timing:")
    print(f"   S3 download:  {timing['s3_download_ms']:.1f} ms")
    print(f"   Video decode: {timing['decode_video_ms']:.1f} ms")
    print(f"   Encode (GPU): {timing['encode_ms']:.1f} ms")
    print(f"   Decode (CPU): {timing['decode_ms']:.1f} ms")
    print(f"   Total server: {timing['total_ms']:.1f} ms")
    print(f"   Round-trip:   {latency_ms:.1f} ms")
    
    if result['num_chunks'] > 1:
        avg_per_chunk = timing['total_ms'] / result['num_chunks']
        print(f"   Avg/chunk:    {avg_per_chunk:.1f} ms")


if __name__ == "__main__":
    main()
