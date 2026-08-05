"""
Async video indexing service.

Flow: client -> POST /index (immediate ack) -> Redis queue -> VideoIndexConsumer
(@task_consumer, CPU) -> VideoEncoder (GPU) -> embeddings stored to S3. The
consumer downloads the video from S3, chunks it with ffmpeg, encodes each chunk
on the GPU encoder, and writes per-video embeddings to S3.

The three deployments live in deployments/ (ingress, consumer, encoder); this
module just composes them into the app graph.

Usage (local, optional):
    serve run config.yaml
"""

from deployments.consumer import VideoIndexConsumer
from deployments.encoder import VideoEncoder
from deployments.ingress import IndexingIngress

encoder = VideoEncoder.bind()
consumer = VideoIndexConsumer.bind(encoder=encoder)
app = IndexingIngress.bind(consumer=consumer)
