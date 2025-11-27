# -- Model --
MODEL_ID = "facebook/opt-1.3b"

# -- Dataset --
DATASET_SPLIT = "train"
DATASET_SAMPLES = 16  # Subset the dataset for the demo.

# -- Training --
STEPS = 2
BATCH_SIZE = 16
# Keep the learning rate low to stay within the trust region.
LEARNING_RATE = 1e-8
WEIGHT_DECAY = 1e-10
# Clip gradients to prevent large updates.
GRAD_CLIP_NORM = 1.0

# -- Generator --
# Maximum requests to process concurrently.
MAX_NUM_SEQS = 5
# Maximum generation length per sequence.
MAX_GENERATION_TOKENS = 128
# Maximum total tokens across a batch.
MAX_NUM_BATCHED_TOKENS = MAX_NUM_SEQS * MAX_GENERATION_TOKENS
# Maximum context length (prompt + generation).
MAX_CONTEXT_LENGTH = 512
TEMPERATURE = 0.7

# -- GRPO algorithm --
# Number of responses per prompt.
GROUP_SIZE = 4
# Trust region scale for the PPO ratio.
GRPO_CLIP_EPS = 0.1
# Discard old experiences to favor recent policies.
MAX_BUFFER_SIZE = BATCH_SIZE * GROUP_SIZE * 5


# -- Scaling --
NUM_NODES_PER_MODEL = 1
NUM_GPUS_PER_MODEL = 4
WORLD_SIZE_PER_MODEL = NUM_NODES_PER_MODEL * NUM_GPUS_PER_MODEL
REGISTRY_NAME = "ray-object-ref-registry-actor"
RAY_NAMESPACE = "foo-bar-baz"

# -- Logging --
LOG_EVERY = 1
