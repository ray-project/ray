# Key to denote the preprocessor in the checkpoint dict.
PREPROCESSOR_KEY = "_preprocessor"

# Key to denote the model in the checkpoint dict.
MODEL_KEY = "model"

# Key to denote which dataset is the evaluation dataset.
# Only used in trainers which do not support multiple
# evaluation datasets.
EVALUATION_DATASET_KEY = "evaluation"

# Key to denote which dataset is the training dataset.
# This is the dataset that the preprocessor is fit on.
TRAIN_DATASET_KEY = "train"

# Name to use for the column when representing tensors in table format.
TENSOR_COLUMN_NAME = "__value__"

# The maximum length of strings returned by `__repr__` for AIR objects constructed with
# default values.
MAX_REPR_LENGTH = int(80 * 1.5)

# Timeout used when putting exceptions raised by runner thread into the queue.
_ERROR_REPORT_TIMEOUT = 10

# Timeout when fetching new results after signaling the training function to continue.
_RESULT_FETCH_TIMEOUT = 0.2

# Timeout for fetching exceptions raised by the training function.
_ERROR_FETCH_TIMEOUT = 1

# The key used to identify whether we have already warned about ray.air.session
# functions being used outside of the session
SESSION_MISUSE_LOG_ONCE_KEY = "air_warn_session_misuse"

# Name of attribute in Checkpoint storing current Tune ID for restoring
# training with Ray Train
CHECKPOINT_ID_ATTR = "_current_checkpoint_id"

# Name of the marker dropped by the Trainable. If a worker detects
# the presence of the marker in the trial dir, it will use lazy
# checkpointing.
LAZY_CHECKPOINT_MARKER_FILE = ".lazy_checkpoint_marker"


# The timestamp of when the result is generated.
# Default to when the result is processed by tune.
TIMESTAMP = "timestamp"

# (Auto-filled) Time in seconds this iteration took to run.
# This may be overridden to override the system-computed time difference.
TIME_THIS_ITER_S = "time_this_iter_s"

# (Auto-filled) The index of this training iteration.
TRAINING_ITERATION = "training_iteration"

# File that stores parameters of the trial.
EXPR_PARAM_FILE = "params.json"

# Pickle File that stores parameters of the trial.
EXPR_PARAM_PICKLE_FILE = "params.pkl"

# File that stores the progress of the trial.
EXPR_PROGRESS_FILE = "progress.csv"

# File that stores results of the trial.
EXPR_RESULT_FILE = "result.json"

# File that stores the pickled error file
EXPR_ERROR_PICKLE_FILE = "error.pkl"

# File that stores the error file
EXPR_ERROR_FILE = "error.txt"

# File that stores the checkpoint metadata
CHECKPOINT_TUNE_METADATA_FILE = ".tune_metadata"

# ==================================================
#               Environment Variables
# ==================================================

# Integer value which if set will copy files in reported AIR directory
# checkpoints instead of moving them (if worker is on the same node as Trainable)
COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV = (
    "TRAIN_COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING"
)

# NOTE: When adding a new environment variable, please track it in this list.
# TODO(ml-team): Most env var constants should get moved here.
AIR_ENV_VARS = {
    COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV,
    "RAY_AIR_FULL_TRACEBACKS",
    "RAY_AIR_NEW_OUTPUT",
}
