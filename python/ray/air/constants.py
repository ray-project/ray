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

# Key to denote all user-specified auxiliary datasets in DatasetConfig.
WILDCARD_KEY = "*"

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

# ==================================================
#               Environment Variables
# ==================================================

# Integer value which if set will copy files in reported AIR directory
# checkpoints instead of moving them (if worker is on the same node as Trainable)
COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV = (
    "TRAIN_COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING"
)

# Integer value which if set will disable lazy checkpointing
# (avoiding unnecessary serialization if worker is on the same node
# as Trainable)
DISABLE_LAZY_CHECKPOINTING_ENV = "TRAIN_DISABLE_LAZY_CHECKPOINTING"


# NOTE: When adding a new environment variable, please track it in this list.
# TODO(ml-team): Most env var constants should get moved here.
AIR_ENV_VARS = {
    COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV,
    DISABLE_LAZY_CHECKPOINTING_ENV,
}

import os

# fmt: off
# __sphinx_doc_begin__
# (Optional/Auto-filled) training is terminated. Filled only if not provided.
DONE = "done"

# (Optional) Enum for user controlled checkpoint
SHOULD_CHECKPOINT = "should_checkpoint"

# (Auto-filled) The hostname of the machine hosting the training process.
HOSTNAME = "hostname"

# (Auto-filled) The auto-assigned id of the trial.
TRIAL_ID = "trial_id"

# (Auto-filled) The auto-assigned id of the trial.
EXPERIMENT_TAG = "experiment_tag"

# (Auto-filled) The node ip of the machine hosting the training process.
NODE_IP = "node_ip"

# (Auto-filled) The pid of the training process.
PID = "pid"

# (Optional) Default (anonymous) metric when using tune.report(x)
DEFAULT_METRIC = "_metric"

# (Optional) Mean reward for current training iteration
EPISODE_REWARD_MEAN = "episode_reward_mean"

# (Optional) Mean loss for training iteration
MEAN_LOSS = "mean_loss"

# (Optional) Mean accuracy for training iteration
MEAN_ACCURACY = "mean_accuracy"

# Number of episodes in this iteration.
EPISODES_THIS_ITER = "episodes_this_iter"

# (Optional/Auto-filled) Accumulated number of episodes for this trial.
EPISODES_TOTAL = "episodes_total"

# The timestamp of when the result is generated.
# Default to when the result is processed by tune.
TIMESTAMP = "timestamp"

# Number of timesteps in this iteration.
TIMESTEPS_THIS_ITER = "timesteps_this_iter"

# (Auto-filled) Accumulated number of timesteps for this entire trial.
TIMESTEPS_TOTAL = "timesteps_total"

# (Auto-filled) Time in seconds this iteration took to run.
# This may be overridden to override the system-computed time difference.
TIME_THIS_ITER_S = "time_this_iter_s"

# (Auto-filled) Accumulated time in seconds for this entire trial.
TIME_TOTAL_S = "time_total_s"

# (Auto-filled) The index of this training iteration.
TRAINING_ITERATION = "training_iteration"
# __sphinx_doc_end__
# fmt: on

DEFAULT_EXPERIMENT_INFO_KEYS = ("trainable_name", EXPERIMENT_TAG, TRIAL_ID)

DEFAULT_RESULT_KEYS = (
    TRAINING_ITERATION,
    TIME_TOTAL_S,
    MEAN_ACCURACY,
    MEAN_LOSS,
)

# Metrics that don't require at least one iteration to complete
DEBUG_METRICS = (
    TRIAL_ID,
    "experiment_id",
    "date",
    TIMESTAMP,
    PID,
    HOSTNAME,
    NODE_IP,
    "config",
)

# Make sure this doesn't regress
AUTO_RESULT_KEYS = (
    TRAINING_ITERATION,
    TIME_TOTAL_S,
    EPISODES_TOTAL,
    TIMESTEPS_TOTAL,
    NODE_IP,
    HOSTNAME,
    PID,
    TIME_TOTAL_S,
    TIME_THIS_ITER_S,
    TIMESTAMP,
    "date",
    "time_since_restore",
    "timesteps_since_restore",
    "iterations_since_restore",
    "config",
)

# __duplicate__ is a magic keyword used internally to
# avoid double-logging results when using the Function API.
RESULT_DUPLICATE = "__duplicate__"

# __trial_info__ is a magic keyword used internally to pass trial_info
# to the Trainable via the constructor.
TRIAL_INFO = "__trial_info__"

# __stdout_file__/__stderr_file__ are magic keywords used internally
# to pass log file locations to the Trainable via the constructor.
STDOUT_FILE = "__stdout_file__"
STDERR_FILE = "__stderr_file__"


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
        or os.path.expanduser("~/ray_results")
    )


# Where Tune writes result files by default
DEFAULT_RESULTS_DIR = _get_defaults_results_dir()

DEFAULT_EXPERIMENT_NAME = "default"

# Meta file about status under each experiment directory, can be
# parsed by automlboard if exists.
JOB_META_FILE = "job_status.json"

# Meta file about status under each trial directory, can be parsed
# by automlboard if exists.
EXPR_META_FILE = "trial_status.json"

# File that stores parameters of the trial.
EXPR_PARAM_FILE = "params.json"

# Pickle File that stores parameters of the trial.
EXPR_PARAM_PICKLE_FILE = "params.pkl"

# File that stores the progress of the trial.
EXPR_PROGRESS_FILE = "progress.csv"

# File that stores results of the trial.
EXPR_RESULT_FILE = "result.json"

# File that stores the pickled error of the trial
EXPR_ERROR_PICKLE_FILE = "error.pkl"

# File that stores the error message of the trial
EXPR_ERROR_FILE = "error.txt"

# Config prefix when using ExperimentAnalysis.
CONFIG_PREFIX = "config"
