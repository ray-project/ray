# Project constants

# SigLIP model
MODEL_NAME = "google/siglip-so400m-patch14-384"

# S3 paths
S3_VIDEOS_PREFIX = "stock-videos/"
S3_EMBEDDINGS_PREFIX = "embeddings/"

# Scene change detection
SCENE_CHANGE_THRESHOLD = 0.15  # EMA score threshold for detecting scene changes
EMA_ALPHA = 0.9  # EMA decay factor (higher = slower adaptation)

# Pexels API
PEXELS_API_BASE = "https://api.pexels.com/videos"

# Concurrency limits
MAX_CONCURRENT_DOWNLOADS = 5
MAX_CONCURRENT_UPLOADS = 5

# Video normalization defaults (384x384 matches model input size for fastest inference)
NORMALIZE_WIDTH = 384
NORMALIZE_HEIGHT = 384
NORMALIZE_FPS = 30

# Video search queries (for downloading stock videos)
SEARCH_QUERIES = [
    "kitchen cooking",
    "office meeting",
    "street city traffic",
    "living room home",
    "restaurant cafe",
    "parking lot cars",
    "classroom students",
    "warehouse industrial",
    "grocery store shopping",
    "gym exercise workout",
    "person speaking",
    "crowd people walking",
    "laptop computer work",
    "outdoor nature",
    "presentation business",
    "conversation talking",
    "running jogging",
    "dining food",
    "shopping mall",
    "park outdoor",
]

# Video chunking defaults
DEFAULT_NUM_FRAMES = 16
DEFAULT_CHUNK_DURATION = 10.0

# FFmpeg configuration, restricting to 2 threads to avoid over subscription.
FFMPEG_THREADS = 6
# Setting this to 2 because i am assuming average 
# video length in my corpus is 20 seconds.
# 20/10(default chunk duration) = 2
# this means we want to set num_cpus to 4 for deployment.
NUM_WORKERS = 3
