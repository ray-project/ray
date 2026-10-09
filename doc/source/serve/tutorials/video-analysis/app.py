"""
Ray Serve application: Video Embedding → Multi-Decoder.

Processes entire videos by chunking into segments.
Videos are downloaded from S3 to temp file, then processed locally (faster than streaming).

Encoder refs are passed directly to decoder; Ray Serve resolves dependencies automatically.

Usage:
    serve run app:app
    
    # With custom bucket:
    S3_BUCKET=my-bucket serve run app:app
"""

import logging
import os
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import aioboto3
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from ray import serve
from ray.serve.handle import DeploymentResponse

from deployments.encoder import VideoEncoder
from deployments.decoder import MultiDecoder
from utils.video import chunk_video_async
from constants import DEFAULT_NUM_FRAMES, DEFAULT_CHUNK_DURATION, FFMPEG_THREADS, NUM_WORKERS

logger = logging.getLogger(__name__)


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse s3://bucket/key into (bucket, key)."""
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key


class AnalyzeRequest(BaseModel):
    """Request schema for /analyze endpoint."""
    stream_id: str
    video_path: str  # S3 URI: s3://bucket/key
    num_frames: int = DEFAULT_NUM_FRAMES
    chunk_duration: float = DEFAULT_CHUNK_DURATION
    use_batching: bool = False  # Set False to compare unbatched performance


class TagResult(BaseModel):
    text: str
    score: float


class CaptionResult(BaseModel):
    text: str
    score: float


class TimingResult(BaseModel):
    s3_download_ms: float
    decode_video_ms: float
    encode_ms: float
    decode_ms: float
    total_ms: float


class SceneChange(BaseModel):
    """Detected scene change event."""
    timestamp: float  # Seconds from video start
    score: float  # Scene change score (higher = bigger change)
    chunk_index: int
    frame_index: int  # Frame index within chunk


class ChunkResult(BaseModel):
    """Result for a single chunk."""
    chunk_index: int
    start_time: float
    duration: float
    tags: list[TagResult]
    retrieval_caption: CaptionResult
    # Detected scene changes in this chunk
    scene_changes: list[SceneChange]


class AnalyzeResponse(BaseModel):
    """Response schema for /analyze endpoint."""
    stream_id: str
    # Aggregated results (across all chunks)
    tags: list[TagResult]
    retrieval_caption: CaptionResult
    # Scene change detection
    scene_changes: list[SceneChange]  # All detected scene changes
    num_scene_changes: int
    # Per-chunk results
    chunks: list[ChunkResult]
    num_chunks: int
    video_duration: float
    timing_ms: TimingResult


# FastAPI app
fastapi_app = FastAPI(
    title="Video Embedding API",
    description="GPU encoder → CPU multi-decoder using SigLIP embeddings",
)


@serve.deployment(
    # setting this to twice that of the encoder. So that requests can complete the
    # upfront CPU work and be queued for GPU processing.
    num_replicas="auto",
    ray_actor_options={"num_cpus": FFMPEG_THREADS},
    max_ongoing_requests=4,
    autoscaling_config={
        "min_replicas": 2,
        "max_replicas": 20,
        "target_num_ongoing_requests": 2,
    },
)
@serve.ingress(fastapi_app)
class VideoAnalyzer:
    """
    Main ingress deployment that orchestrates VideoEncoder and MultiDecoder.
    
    Encoder refs are passed directly to decoder; Ray Serve resolves dependencies.
    Downloads video from S3 to temp file for fast local processing.
    """
    
    def __init__(self, encoder: VideoEncoder, decoder: MultiDecoder):
        self.encoder = encoder
        self.decoder = decoder
        self._s3_session = aioboto3.Session()
        self._s3_client = None  # Cached client for reuse across requests
        logger.info("VideoAnalyzer ready")
    
    async def _get_s3_client(self):
        """Get or create a reusable S3 client."""
        if self._s3_client is None:
            self._s3_client = await self._s3_session.client("s3").__aenter__()
        return self._s3_client
    
    async def _download_video(self, s3_uri: str) -> Path:
        """Download video from S3 to temp file. Returns local path."""
        bucket, key = parse_s3_uri(s3_uri)
        
        # Create temp file with video extension
        suffix = Path(key).suffix or ".mp4"
        temp_file = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        temp_path = Path(temp_file.name)
        temp_file.close()
        
        try:
            s3 = await self._get_s3_client()
            await s3.download_file(bucket, key, str(temp_path))
        except Exception:
            # Clean up temp file if download fails
            temp_path.unlink(missing_ok=True)
            raise
        
        return temp_path

    def _aggregate_results(
        self,
        chunk_results: list[dict],
        top_k_tags: int = 5,
    ) -> dict:
        """
        Aggregate results from multiple chunks.
        
        Strategy:
        - Tags: Average scores across chunks, return top-k
        - Caption: Return the one with highest score across all chunks
        """
        # Aggregate tag scores
        tag_scores = defaultdict(list)
        for result in chunk_results:
            for tag in result["tags"]:
                tag_scores[tag["text"]].append(tag["score"])
        
        # Average tag scores and sort
        aggregated_tags = [
            {"text": text, "score": np.mean(scores)}
            for text, scores in tag_scores.items()
        ]
        aggregated_tags.sort(key=lambda x: x["score"], reverse=True)
        top_tags = aggregated_tags[:top_k_tags]
        
        # Best caption across all chunks
        best_caption = max(
            (r["retrieval_caption"] for r in chunk_results),
            key=lambda x: x["score"],
        )
        
        return {
            "tags": top_tags,
            "retrieval_caption": best_caption,
        }
    
    def _encode_chunk(self, frames: np.ndarray, use_batching: bool = False) -> DeploymentResponse:
        """Encode a single chunk's frames to embeddings. Returns DeploymentResponse ref."""
        return self.encoder.remote(frames, use_batching=use_batching)

    async def _decode_chunk(
        self,
        encoder_output: dict,
        chunk_index: int,
        chunk_start_time: float,
        chunk_duration: float,
        ema_state=None,
    ) -> dict:
        """Decode embeddings to tags, caption, scene changes."""
        return await self.decoder.remote(
            encoder_output=encoder_output,
            chunk_index=chunk_index,
            chunk_start_time=chunk_start_time,
            chunk_duration=chunk_duration,
            ema_state=ema_state,
        )
    
    @fastapi_app.post("/analyze", response_model=AnalyzeResponse)
    async def analyze(self, request: AnalyzeRequest) -> AnalyzeResponse:
        """
        Analyze a video from S3 and return tags, caption, and scene changes.
        
        Downloads video to temp file for fast local processing.
        Chunks the entire video and aggregates results.
        Encoder refs are passed directly to decoder for dependency resolution.
        """
        total_start = time.perf_counter()
        temp_path = None

        try:
            # Download video from S3 to temp file
            download_start = time.perf_counter()
            try:
                temp_path = await self._download_video(request.video_path)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Cannot download S3 video: {e}")
            s3_download_ms = (time.perf_counter() - download_start) * 1000

            # Chunk video with PARALLEL frame extraction from local file
            decode_start = time.perf_counter()
            try:
                chunks = await chunk_video_async(
                    str(temp_path),
                    chunk_duration=request.chunk_duration,
                    num_frames_per_chunk=request.num_frames,
                    ffmpeg_threads=FFMPEG_THREADS,
                    use_single_ffmpeg=True,
                )
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Cannot process video: {e}")

            decode_video_ms = (time.perf_counter() - decode_start) * 1000
            
            if not chunks:
                raise HTTPException(status_code=400, detail="No chunks extracted from video")
            
            # Calculate video duration from chunks
            video_duration = chunks[-1].start_time + chunks[-1].duration
            
            # Fire off all encoder calls (returns refs, not awaited)
            encode_start = time.perf_counter()
            encode_refs = [
                self._encode_chunk(chunk.frames, use_batching=request.use_batching) 
                for chunk in chunks
            ]
            encode_ms = (time.perf_counter() - encode_start) * 1000
            
            # Decode chunks SERIALLY, passing encoder refs directly.
            # Ray Serve resolves the encoder result when decoder needs it.
            # EMA state is tracked here (not in decoder) to ensure continuity
            # even when autoscaling routes requests to different replicas.
            decode_start = time.perf_counter()
            decode_results = []
            ema_state = None  # Will be initialized from first chunk's first frame
            for chunk, enc_ref in zip(chunks, encode_refs):
                dec_result = await self._decode_chunk(
                    encoder_output=enc_ref,
                    chunk_index=chunk.index,
                    chunk_start_time=chunk.start_time,
                    chunk_duration=chunk.duration,
                    ema_state=ema_state,
                )
                decode_results.append(dec_result)
                ema_state = dec_result["ema_state"]  # Carry forward for next chunk
            decode_ms = (time.perf_counter() - decode_start) * 1000
            
            # Collect results
            chunk_results = []
            per_chunk_results = []
            all_scene_changes = []
            
            for chunk, decoder_result in zip(chunks, decode_results):
                chunk_results.append(decoder_result)
                
                # Scene changes come directly from decoder
                chunk_scene_changes = [
                    SceneChange(**sc) for sc in decoder_result["scene_changes"]
                ]
                all_scene_changes.extend(chunk_scene_changes)
                
                per_chunk_results.append(ChunkResult(
                    chunk_index=chunk.index,
                    start_time=chunk.start_time,
                    duration=chunk.duration,
                    tags=[TagResult(**t) for t in decoder_result["tags"]],
                    retrieval_caption=CaptionResult(**decoder_result["retrieval_caption"]),
                    scene_changes=chunk_scene_changes,
                ))
            
            # Aggregate results
            aggregated = self._aggregate_results(chunk_results)
            
            total_ms = (time.perf_counter() - total_start) * 1000
            
            return AnalyzeResponse(
                stream_id=request.stream_id,
                tags=[TagResult(**t) for t in aggregated["tags"]],
                retrieval_caption=CaptionResult(**aggregated["retrieval_caption"]),
                scene_changes=all_scene_changes,
                num_scene_changes=len(all_scene_changes),
                chunks=per_chunk_results,
                num_chunks=len(chunks),
                video_duration=video_duration,
                timing_ms=TimingResult(
                    s3_download_ms=round(s3_download_ms, 2),
                    decode_video_ms=round(decode_video_ms, 2),
                    encode_ms=round(encode_ms, 2),
                    decode_ms=round(decode_ms, 2),
                    total_ms=round(total_ms, 2),
                ),
            )
        finally:
            # Clean up temp file
            if temp_path and temp_path.exists():
                temp_path.unlink(missing_ok=True)
    
    @fastapi_app.get("/health")
    async def health(self):
        """Health check endpoint."""
        return {"status": "healthy"}


encoder = VideoEncoder.bind()
decoder = MultiDecoder.bind(bucket=os.environ.get("S3_BUCKET"))
app = VideoAnalyzer.bind(encoder=encoder, decoder=decoder)
