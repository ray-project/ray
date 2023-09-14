import os
from pathlib import Path

from ray.air.constants import (  # noqa: F401
    EVALUATION_DATASET_KEY,
    MODEL_KEY,
    PREPROCESSOR_KEY,
    TRAIN_DATASET_KEY,
    WILDCARD_KEY,
    COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV,
    DISABLE_LAZY_CHECKPOINTING_ENV,
    LAZY_CHECKPOINT_MARKER_FILE,
)


def _get_defaults_results_dir() -> str:
    return (
        # This can be overwritten by our libraries
        os.environ.get("RAY_AIR_LOCAL_CACHE_DIR")
        # This is a directory provided by Bazel automatically
        or os.environ.get("TEST_TMPDIR")
        # This is the old way to specify the results dir
        # Deprecate: Remove in 2.6
        or os.environ.get("TUNE_RESULT_DIR")
        # Default
        or Path("~/ray_results").expanduser().as_posix()
    )


# Autofilled ray.train.report() metrics. Keys should be consistent with Tune.
CHECKPOINT_DIR_NAME = "checkpoint_dir_name"
TIME_TOTAL_S = "_time_total_s"
WORKER_HOSTNAME = "_hostname"
WORKER_NODE_IP = "_node_ip"
WORKER_PID = "_pid"

# Will not be reported unless ENABLE_DETAILED_AUTOFILLED_METRICS_ENV
# env var is not 0
DETAILED_AUTOFILLED_KEYS = {WORKER_HOSTNAME, WORKER_NODE_IP, WORKER_PID, TIME_TOTAL_S}

# Default filename for JSON logger
RESULT_FILE_JSON = "results.json"

# Default directory where all Train logs, checkpoints, etc. will be stored.
DEFAULT_RESULTS_DIR = Path("~/ray_results").expanduser()

# The name of the subdirectory inside the trainer run_dir to store checkpoints.
TRAIN_CHECKPOINT_SUBDIR = "checkpoints"

# The key to use to specify the checkpoint id for Tune.
# This needs to be added to the checkpoint dictionary so if the Tune trial
# is restarted, the checkpoint_id can continue to increment.
TUNE_CHECKPOINT_ID = "_current_checkpoint_id"

# Deprecated configs can use this value to detect if the user has set it.
_DEPRECATED_VALUE = "DEPRECATED"

# ==================================================
#               Environment Variables
# ==================================================

ENABLE_DETAILED_AUTOFILLED_METRICS_ENV = (
    "TRAIN_RESULT_ENABLE_DETAILED_AUTOFILLED_METRICS"
)

# Integer value which if set will override the value of
# Backend.share_cuda_visible_devices. 1 for True, 0 for False.
ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV = "TRAIN_ENABLE_SHARE_CUDA_VISIBLE_DEVICES"

# Integer value which indicates the number of seconds to wait when creating
# the worker placement group before timing out.
TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV = "TRAIN_PLACEMENT_GROUP_TIMEOUT_S"

# Integer value which if set will change the placement group strategy from
# PACK to SPREAD. 1 for True, 0 for False.
TRAIN_ENABLE_WORKER_SPREAD_ENV = "TRAIN_ENABLE_WORKER_SPREAD"

RAY_AIR_NEW_PERSISTENCE_MODE = "RAY_AIR_NEW_PERSISTENCE_MODE"

# Set this to 0 to disable changing the working directory of each Tune Trainable
# or Train worker to the trial directory. Defaults to 1.
RAY_CHDIR_TO_TRIAL_DIR = "RAY_CHDIR_TO_TRIAL_DIR"

# NOTE: When adding a new environment variable, please track it in this list.
TRAIN_ENV_VARS = {
    ENABLE_DETAILED_AUTOFILLED_METRICS_ENV,
    ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
    TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV,
    TRAIN_ENABLE_WORKER_SPREAD_ENV,
    RAY_AIR_NEW_PERSISTENCE_MODE,
    RAY_CHDIR_TO_TRIAL_DIR,
}

# Blacklist virtualized networking.
DEFAULT_NCCL_SOCKET_IFNAME = "^lo,docker,veth"

# Key for AIR Checkpoint metadata in TrainingResult metadata
CHECKPOINT_METADATA_KEY = "checkpoint_metadata"

# Key for AIR Checkpoint world rank in TrainingResult metadata
CHECKPOINT_RANK_KEY = "checkpoint_rank"
