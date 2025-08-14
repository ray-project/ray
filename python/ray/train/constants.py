from pathlib import Path
from typing import Any

import ray
from ray._private.ray_constants import env_bool
from ray.air.constants import (  # noqa: F401
    COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV,
    EVALUATION_DATASET_KEY,
    MODEL_KEY,
    PREPROCESSOR_KEY,
    TRAIN_DATASET_KEY,
)


def _get_ray_train_session_dir() -> str:
    assert ray.is_initialized(), "Ray must be initialized to get the session dir."
    return Path(
        ray._private.worker._global_node.get_session_dir_path(), "artifacts"
    ).as_posix()


DEFAULT_STORAGE_PATH = Path("~/ray_results").expanduser().as_posix()

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

# The name of the subdirectory inside the trainer run_dir to store checkpoints.
TRAIN_CHECKPOINT_SUBDIR = "checkpoints"

# The key to use to specify the checkpoint id for Tune.
# This needs to be added to the checkpoint dictionary so if the Tune trial
# is restarted, the checkpoint_id can continue to increment.
TUNE_CHECKPOINT_ID = "_current_checkpoint_id"

# Deprecated configs can use this value to detect if the user has set it.
# This has type Any to allow it to be assigned to any annotated parameter
# without causing type errors.
_DEPRECATED_VALUE: Any = "DEPRECATED"


# ==================================================
#               Train V2 constants
# ==================================================

# Set this to 1 to enable deprecation warnings for V2 migration.
ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR = "RAY_TRAIN_ENABLE_V2_MIGRATION_WARNINGS"


V2_MIGRATION_GUIDE_MESSAGE = (
    "See this issue for more context and migration options: "
    "https://github.com/ray-project/ray/issues/49454. "
    "Disable these warnings by setting the environment variable: "
    f"{ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR}=0"
)


def _v2_migration_warnings_enabled() -> bool:
    return env_bool(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, True)


# ==================================================
#               Environment Variables
# ==================================================

ENABLE_DETAILED_AUTOFILLED_METRICS_ENV = (
    "TRAIN_RESULT_ENABLE_DETAILED_AUTOFILLED_METRICS"
)

# Integer value which if set will override the value of
# Backend.share_cuda_visible_devices. 1 for True, 0 for False.
ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV = "TRAIN_ENABLE_SHARE_CUDA_VISIBLE_DEVICES"

# Integer value which if set will not share HIP accelerator visible devices
# across workers. 1 for True (default), 0 for False.
ENABLE_SHARE_HIP_VISIBLE_DEVICES_ENV = "TRAIN_ENABLE_SHARE_HIP_VISIBLE_DEVICES"

# Integer value which if set will not share neuron-core accelerator visible cores
# across workers. 1 for True (default), 0 for False.
ENABLE_SHARE_NEURON_CORES_ACCELERATOR_ENV = (
    "TRAIN_ENABLE_SHARE_NEURON_CORES_ACCELERATOR"
)

# Integer value which if set will not share npu visible devices
# across workers. 1 for True (default), 0 for False.
ENABLE_SHARE_NPU_RT_VISIBLE_DEVICES_ENV = "TRAIN_ENABLE_SHARE_ASCEND_RT_VISIBLE_DEVICES"

# Integer value which indicates the number of seconds to wait when creating
# the worker placement group before timing out.
TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV = "TRAIN_PLACEMENT_GROUP_TIMEOUT_S"

# Integer value which if set will change the placement group strategy from
# PACK to SPREAD. 1 for True, 0 for False.
TRAIN_ENABLE_WORKER_SPREAD_ENV = "TRAIN_ENABLE_WORKER_SPREAD"

# Set this to 0 to disable changing the working directory of each Tune Trainable
# or Train worker to the trial directory. Defaults to 1.
RAY_CHDIR_TO_TRIAL_DIR = "RAY_CHDIR_TO_TRIAL_DIR"

# Set this to 1 to count preemption errors toward `FailureConfig(max_failures)`.
# Defaults to 0, which always retries on node preemption failures.
RAY_TRAIN_COUNT_PREEMPTION_AS_FAILURE = "RAY_TRAIN_COUNT_PREEMPTION_AS_FAILURE"

# Set this to 1 to start a StateActor and collect information Train Runs
# Defaults to 0
RAY_TRAIN_ENABLE_STATE_TRACKING = "RAY_TRAIN_ENABLE_STATE_TRACKING"

# Set this to 1 to only store the checkpoint score attribute with the Checkpoint
# in the CheckpointManager. The Result will only have the checkpoint score attribute
# but files written to disk like result.json will still have all the metrics.
# Defaults to 0.
# TODO: this is a temporary solution to avoid CheckpointManager OOM.
# See https://github.com/ray-project/ray/pull/54642#issue-3234029360 for more details.
TUNE_ONLY_STORE_CHECKPOINT_SCORE_ATTRIBUTE = (
    "TUNE_ONLY_STORE_CHECKPOINT_SCORE_ATTRIBUTE"
)


# NOTE: When adding a new environment variable, please track it in this list.
TRAIN_ENV_VARS = {
    ENABLE_DETAILED_AUTOFILLED_METRICS_ENV,
    ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
    ENABLE_SHARE_NEURON_CORES_ACCELERATOR_ENV,
    TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV,
    TRAIN_ENABLE_WORKER_SPREAD_ENV,
    RAY_CHDIR_TO_TRIAL_DIR,
    RAY_TRAIN_COUNT_PREEMPTION_AS_FAILURE,
    RAY_TRAIN_ENABLE_STATE_TRACKING,
    TUNE_ONLY_STORE_CHECKPOINT_SCORE_ATTRIBUTE,
}

# Key for AIR Checkpoint metadata in TrainingResult metadata
CHECKPOINT_METADATA_KEY = "checkpoint_metadata"

# Key for AIR Checkpoint world rank in TrainingResult metadata
CHECKPOINT_RANK_KEY = "checkpoint_rank"
