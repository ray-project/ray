from pathlib import Path

try:
    TUNE_INSTALLED = True
    from ray import tune  # noqa: F401
except ImportError:
    TUNE_INSTALLED = False

from ray.air.constants import (  # noqa: F401
    EVALUATION_DATASET_KEY,
    MODEL_KEY,
    PREPROCESSOR_KEY,
    TRAIN_DATASET_KEY,
    WILDCARD_KEY,
)

# Autofilled session.report() metrics. Keys should be consistent with Tune.
TIMESTAMP = "_timestamp"
TIME_THIS_ITER_S = "_time_this_iter_s"
TRAINING_ITERATION = "_training_iteration"

BASIC_AUTOFILLED_KEYS = {TIMESTAMP, TIME_THIS_ITER_S, TRAINING_ITERATION}

DATE = "_date"
HOSTNAME = "_hostname"
NODE_IP = "_node_ip"
PID = "_pid"
TIME_TOTAL_S = "_time_total_s"

# Will not be reported unless ENABLE_DETAILED_AUTOFILLED_METRICS_ENV
# env var is not 0
DETAILED_AUTOFILLED_KEYS = {DATE, HOSTNAME, NODE_IP, PID, TIME_TOTAL_S}

# Time between Session.get_next checks when fetching
# new results after signaling the training function to continue.
RESULT_FETCH_TIMEOUT = 0.2

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

# Env var name
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

# The key used to identify whether we have already warned about ray.train
# functions being used outside of the session
SESSION_MISUSE_LOG_ONCE_KEY = "train_warn_session_misuse"

# Reserved keyword used by the ``TorchWorkerProfiler`` and
# ``TorchTensorboardProfilerCallback`` for passing PyTorch Profiler data
# through ``session.report()``
PYTORCH_PROFILER_KEY = "_train_torch_profiler"

# Reserved keys used across all Callbacks.
# By default these will be filtered out from ``session.report()``.
# See ``TrainingCallback._preprocess_results`` for more details.
ALL_RESERVED_KEYS = {PYTORCH_PROFILER_KEY}
