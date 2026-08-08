# Project constants for the async video indexing service.

import os

# SigLIP model (shared with the video-analysis example).
MODEL_NAME = "google/siglip-so400m-patch14-384"

# Video chunking defaults.
DEFAULT_NUM_FRAMES = 1
DEFAULT_CHUNK_DURATION = 9999.0
# FFmpeg threads per chunking call. Also the consumer's num_cpus.
FFMPEG_THREADS = 6

# Redis: one instance is broker + result backend + autoscaling queue-depth
# source. Point REDIS_BROKER_URL at the ElastiCache primary endpoint when
# deployed; defaults target a local docker Redis for development.
REDIS_BROKER_URL = os.environ.get("REDIS_BROKER_URL", "redis://localhost:6379/0")
REDIS_BACKEND_URL = os.environ.get("REDIS_BACKEND_URL", "redis://localhost:6379/1")

# Task queue, dead-letter queues, and task name.
INDEX_QUEUE = "video_index_queue"
FAILED_QUEUE = "video_index_failed"  # application errors after retries
UNPROCESSABLE_QUEUE = "video_index_unprocessable"  # bad payload or no handler
TASK_INDEX_VIDEO = "index_video"

# S3 layout for stored embeddings (indexing output).
S3_BUCKET = os.environ.get("S3_BUCKET")
S3_EMBEDDINGS_PREFIX = "video-index/embeddings/"
S3_MANIFEST_PREFIX = "video-index/manifest/"
S3_DONE_PREFIX = "video-index/done/"

# Reliability.
MAX_RETRIES = 5
# visibility_timeout: how long a picked-up task stays invisible before Redis
# redelivers it (Celery's at-least-once mechanism on Redis). Set to a small
# multiple of p99 task time: high enough to avoid redelivering a still-running
# task (duplicate work), low enough that tasks orphaned by a replica scale-down
# or crash redeliver quickly. Our indexing tasks are ~1-2s, so 120s is ample.
VISIBILITY_TIMEOUT_S = 120
