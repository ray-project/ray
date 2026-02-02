#!/usr/bin/env python3
"""
Load testing script for the Ray Serve video API.

Usage:
    python -m client.load_test --video s3://bucket/path/to/video.mp4 --concurrency 4
    python -m client.load_test --video s3://bucket/video.mp4 --concurrency 8 --url http://localhost:8000
    python -m client.load_test --video s3://bucket/video.mp4 --concurrency 4 --token YOUR_TOKEN

Press Ctrl+C to stop and save results to CSV.
"""

import argparse
import asyncio
import csv
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx


@dataclass
class RequestResult:
    """Result of a single request."""
    request_id: str
    stream_id: str
    start_time: float
    end_time: float
    latency_ms: float
    success: bool
    status_code: int
    error: Optional[str] = None
    # Server-side timing (if successful)
    s3_download_ms: Optional[float] = None
    decode_video_ms: Optional[float] = None
    encode_ms: Optional[float] = None
    decode_ms: Optional[float] = None
    server_total_ms: Optional[float] = None
    # Response data
    num_chunks: Optional[int] = None
    video_duration: Optional[float] = None
    num_scene_changes: Optional[int] = None


@dataclass
class LoadTestStats:
    """Aggregated statistics for the load test."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float('inf')
    max_latency_ms: float = 0.0
    latencies: list = field(default_factory=list)
    
    def add_result(self, result: RequestResult):
        self.total_requests += 1
        if result.success:
            self.successful_requests += 1
            self.total_latency_ms += result.latency_ms
            self.min_latency_ms = min(self.min_latency_ms, result.latency_ms)
            self.max_latency_ms = max(self.max_latency_ms, result.latency_ms)
            self.latencies.append(result.latency_ms)
        else:
            self.failed_requests += 1
    
    @property
    def avg_latency_ms(self) -> float:
        if not self.latencies:
            return 0.0
        return self.total_latency_ms / len(self.latencies)
    
    @property
    def p50_latency_ms(self) -> float:
        return self._percentile(50)
    
    @property
    def p95_latency_ms(self) -> float:
        return self._percentile(95)
    
    @property
    def p99_latency_ms(self) -> float:
        return self._percentile(99)
    
    def _percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * p / 100)
        idx = min(idx, len(sorted_latencies) - 1)
        return sorted_latencies[idx]


class LoadTester:
    def __init__(
        self,
        video_path: str,
        url: str,
        concurrency: int,
        num_frames: int = 16,
        chunk_duration: float = 10.0,
        timeout: float = 300.0,
        token: Optional[str] = None,
    ):
        self.video_path = video_path
        self.url = url
        self.concurrency = concurrency
        self.num_frames = num_frames
        self.chunk_duration = chunk_duration
        self.timeout = timeout
        self.token = token
        
        self.results: list[RequestResult] = []
        self.stats = LoadTestStats()
        self.running = True
        self.start_time: Optional[float] = None
        self.request_counter = 0
        self._lock = asyncio.Lock()
        self._semaphore: asyncio.Semaphore = None
    
    def _build_payload(self) -> dict:
        """Build the request payload."""
        stream_id = uuid.uuid4().hex[:8]
        return {
            "stream_id": stream_id,
            "video_path": self.video_path,
            "num_frames": self.num_frames,
            "chunk_duration": self.chunk_duration,
            "use_batching": False,
        }
    
    async def _make_request(self, client: httpx.AsyncClient, request_id: str) -> RequestResult:
        """Make a single request and return the result."""
        payload = self._build_payload()
        stream_id = payload["stream_id"]
        
        start = time.perf_counter()
        start_timestamp = time.time()
        
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        
        try:
            response = await client.post(f"{self.url}/analyze", json=payload, headers=headers)
            end = time.perf_counter()
            latency_ms = (end - start) * 1000
            
            if response.status_code == 200:
                data = response.json()
                timing = data.get("timing_ms", {})
                return RequestResult(
                    request_id=request_id,
                    stream_id=stream_id,
                    start_time=start_timestamp,
                    end_time=time.time(),
                    latency_ms=latency_ms,
                    success=True,
                    status_code=response.status_code,
                    s3_download_ms=timing.get("s3_download_ms"),
                    decode_video_ms=timing.get("decode_video_ms"),
                    encode_ms=timing.get("encode_ms"),
                    decode_ms=timing.get("decode_ms"),
                    server_total_ms=timing.get("total_ms"),
                    num_chunks=data.get("num_chunks"),
                    video_duration=data.get("video_duration"),
                    num_scene_changes=data.get("num_scene_changes"),
                )
            else:
                return RequestResult(
                    request_id=request_id,
                    stream_id=stream_id,
                    start_time=start_timestamp,
                    end_time=time.time(),
                    latency_ms=latency_ms,
                    success=False,
                    status_code=response.status_code,
                    error=response.text[:200],
                )
        except Exception as e:
            end = time.perf_counter()
            latency_ms = (end - start) * 1000
            return RequestResult(
                request_id=request_id,
                stream_id=stream_id,
                start_time=start_timestamp,
                end_time=time.time(),
                latency_ms=latency_ms,
                success=False,
                status_code=0,
                error=str(e)[:200],
            )
    
    async def _request_task(self, client: httpx.AsyncClient, request_id: str):
        """Execute a single request and record the result. Releases semaphore when done."""
        try:
            result = await self._make_request(client, request_id)
            
            async with self._lock:
                self.results.append(result)
                self.stats.add_result(result)
            
            # Print progress
            status = "✓" if result.success else "✗"
            print(
                f"{status} {result.request_id} | "
                f"{result.latency_ms:7.1f}ms | "
                f"Total: {self.stats.total_requests} "
                f"(OK: {self.stats.successful_requests}, Fail: {self.stats.failed_requests})"
            )
        finally:
            self._semaphore.release()
    
    async def run(self):
        """Run the load test until interrupted."""
        self.start_time = time.time()
        self._semaphore = asyncio.Semaphore(self.concurrency)
        pending_tasks: set[asyncio.Task] = set()
        
        print("=" * 70)
        print("🚀 Starting Load Test")
        print("=" * 70)
        print(f"   Video:       {self.video_path}")
        print(f"   URL:         {self.url}")
        print(f"   Concurrency: {self.concurrency}")
        print(f"   Chunk dur:   {self.chunk_duration}s")
        print(f"   Frames/chunk:{self.num_frames}")
        print()
        print("Press Ctrl+C to stop and save results...")
        print("-" * 70)
        
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while self.running:
                # Block until a slot is available
                await self._semaphore.acquire()
                
                if not self.running:
                    self._semaphore.release()
                    break
                # Spawn task (it will release semaphore when done)
                self.request_counter += 1
                request_id = f"req_{self.request_counter:06d}"
                task = asyncio.create_task(self._request_task(client, request_id))
                pending_tasks.add(task)
                task.add_done_callback(pending_tasks.discard)
            
            # Wait for in-flight requests
            if pending_tasks:
                print(f"\n⏳ Waiting for {len(pending_tasks)} in-flight requests...")
                await asyncio.gather(*pending_tasks, return_exceptions=True)
    
    def stop(self):
        """Stop the load test."""
        self.running = False
    
    def save_results(self, output_path: str):
        """Save results to CSV."""
        if not self.results:
            print("No results to save.")
            return
        
        fieldnames = [
            "request_id", "stream_id", "start_time", "end_time", "latency_ms",
            "success", "status_code", "error",
            "s3_download_ms", "decode_video_ms", "encode_ms", "decode_ms", "server_total_ms",
            "num_chunks", "video_duration", "num_scene_changes"
        ]
        
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for result in self.results:
                writer.writerow(asdict(result))
        
        print(f"📁 Results saved to: {output_path}")
    
    def print_summary(self):
        """Print summary statistics."""
        if not self.results:
            print("No results to summarize.")
            return
        
        duration = time.time() - self.start_time if self.start_time else 0
        throughput = self.stats.total_requests / duration if duration > 0 else 0
        
        print()
        print("=" * 70)
        print("📊 Load Test Summary")
        print("=" * 70)
        print(f"   Duration:         {duration:.1f}s")
        print(f"   Total requests:   {self.stats.total_requests}")
        print(f"   Successful:       {self.stats.successful_requests}")
        print(f"   Failed:           {self.stats.failed_requests}")
        print(f"   Success rate:     {self.stats.successful_requests / self.stats.total_requests * 100:.1f}%")
        print(f"   Throughput:       {throughput:.2f} req/s")
        print()
        print("⏱️  Latency (successful requests):")
        if self.stats.latencies:
            print(f"   Min:    {self.stats.min_latency_ms:8.1f} ms")
            print(f"   Max:    {self.stats.max_latency_ms:8.1f} ms")
            print(f"   Avg:    {self.stats.avg_latency_ms:8.1f} ms")
            print(f"   P50:    {self.stats.p50_latency_ms:8.1f} ms")
            print(f"   P95:    {self.stats.p95_latency_ms:8.1f} ms")
            print(f"   P99:    {self.stats.p99_latency_ms:8.1f} ms")
        else:
            print("   (no successful requests)")
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Load test the Ray Serve video API")
    parser.add_argument("--video", type=str, required=True, help="S3 URI: s3://bucket/key")
    parser.add_argument("--url", type=str, default="http://127.0.0.1:8000", help="Server URL")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of concurrent workers")
    parser.add_argument("--num-frames", type=int, default=16, help="Frames per chunk")
    parser.add_argument("--chunk-duration", type=float, default=10.0, help="Chunk duration in seconds")
    parser.add_argument("--timeout", type=float, default=300.0, help="Request timeout in seconds")
    parser.add_argument("--output", type=str, default=None, help="Output CSV path (default: load_test_<timestamp>.csv)")
    parser.add_argument("--token", type=str, default=None, help="Bearer token for Authorization header")
    args = parser.parse_args()
    
    # Generate output path if not provided
    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"load_test_{timestamp}_{args.concurrency}.csv"
    
    tester = LoadTester(
        video_path=args.video,
        url=args.url,
        concurrency=args.concurrency,
        num_frames=args.num_frames,
        chunk_duration=args.chunk_duration,
        timeout=args.timeout,
        token=args.token,
    )
    
    # Track interrupt count for force exit
    interrupt_count = 0
    
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        nonlocal interrupt_count
        interrupt_count += 1
        
        if interrupt_count == 1:
            print("\n\n🛑 Stopping load test (press Ctrl+C again to force exit)...")
            tester.stop()
        else:
            print("\n\n⚠️  Force exiting...")
            tester.print_summary()
            tester.save_results(args.output)
            sys.exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        asyncio.run(tester.run())
    except KeyboardInterrupt:
        pass
    finally:
        tester.print_summary()
        tester.save_results(args.output)


if __name__ == "__main__":
    main()

