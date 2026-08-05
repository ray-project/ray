"""SageMaker Async Inference handler for the video-indexing benchmark.

Runs the SAME workload as the Ray Serve consumer (download -> ffmpeg chunk ->
SigLIP embed -> write embeddings to S3), so the two engines are compared on
identical work with the same model. SageMaker's async endpoint queues each
invocation and autoscales instances on backlog; this handler is what a single
instance runs per request.

SageMaker inference-toolkit contract: model_fn / input_fn / predict_fn / output_fn.
"""

import json
from pathlib import Path
import tempfile

import boto3
import numpy as np
import torch
from transformers import AutoImageProcessor, AutoModel

from constants import (
    DEFAULT_CHUNK_DURATION,
    DEFAULT_NUM_FRAMES,
    FFMPEG_THREADS,
    MODEL_NAME,
)
from utils.embedding_store import write_video_embeddings
from utils.idempotency import is_done, mark_done, video_id_for
from utils.s3 import parse_s3_uri
from utils.video import chunk_video, frames_to_pil_list


class SiglipEncoder:
    """Plain (non-Ray) SigLIP frame encoder - identical model to VideoEncoder."""

    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.processor = AutoImageProcessor.from_pretrained(MODEL_NAME)
        self.model = AutoModel.from_pretrained(MODEL_NAME).to(self.device)
        self.model.eval()

    def encode_frames(self, frames: np.ndarray) -> np.ndarray:
        pil_images = frames_to_pil_list(frames)
        inputs = self.processor(images=pil_images, return_tensors="pt").to(self.device)
        with torch.no_grad():
            with torch.amp.autocast(device_type=self.device, enabled=self.device == "cuda"):
                outputs = self.model.get_image_features(**inputs)
                frame_embeddings = torch.nn.functional.normalize(outputs, p=2, dim=1)
        return frame_embeddings.cpu().numpy().astype(np.float32)


# --- SageMaker inference-toolkit handler ---

def model_fn(model_dir):
    return SiglipEncoder()


def input_fn(request_body, content_type="application/json"):
    if isinstance(request_body, (bytes, bytearray)):
        request_body = request_body.decode("utf-8")
    return json.loads(request_body)


def predict_fn(data, encoder):
    video_uri = data["video_uri"]
    vid = data.get("video_id") or video_id_for(video_uri)

    # Idempotency: same as the Ray Serve handler - redelivery / re-invoke skips.
    if is_done(vid):
        return {"video_id": vid, "status": "skipped_already_indexed"}

    bucket, key = parse_s3_uri(video_uri)
    s3 = boto3.client("s3")
    suffix = Path(key).suffix or ".mp4"
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    tmp.close()
    try:
        s3.download_file(bucket, key, tmp.name)
        chunks = chunk_video(
            tmp.name,
            chunk_duration=DEFAULT_CHUNK_DURATION,
            num_frames_per_chunk=DEFAULT_NUM_FRAMES,
            ffmpeg_threads=FFMPEG_THREADS,
            use_single_ffmpeg=True,
        )
        if not chunks:
            raise ValueError(f"No chunks extracted from {video_uri}")

        per_chunk = [encoder.encode_frames(c.frames) for c in chunks]
        frame_embeddings = np.concatenate(per_chunk, axis=0)
        pooled = frame_embeddings.mean(axis=0)
        pooled = pooled / (np.linalg.norm(pooled) + 1e-12)
        chunk_meta = [
            {"index": c.index, "start_time": c.start_time, "duration": c.duration}
            for c in chunks
        ]
        write_video_embeddings(vid, video_uri, pooled, frame_embeddings, chunk_meta)
        mark_done(vid)
        return {
            "video_id": vid,
            "status": "indexed",
            "num_chunks": len(chunks),
            "num_frames": int(frame_embeddings.shape[0]),
            "embedding_dim": int(pooled.shape[0]),
        }
    finally:
        Path(tmp.name).unlink(missing_ok=True)


def output_fn(prediction, accept="application/json"):
    return json.dumps(prediction), "application/json"
