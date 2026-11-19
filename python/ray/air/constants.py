from ray._common.deprecation import (
    DeprecatedAlias as _DeprecatedAlias,
    make_deprecated_getattr as _make_deprecated_getattr,
)

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


# Private copies of `ray.tune.result.EXPR_*` for `ray.air.result`,
# which Train v2 also uses and so can't import `ray.tune` (`ray.tune` also
# imports `ray.air.result`, which would be a cycle). Keep the values in sync.
_EXPR_PROGRESS_FILE = "progress.csv"
_EXPR_RESULT_FILE = "result.json"
_EXPR_ERROR_PICKLE_FILE = "error.pkl"

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


# ==================================================
#        Deprecated aliases (removed in Ray 3.0)
# ==================================================

# These are Ray Tune result keys and file names, so they now live in
# `ray.tune.result`. They are kept out of the module globals so that `__getattr__`
# can warn on access.
_MOVED_TO_TUNE_RESULT = (
    "EXPR_ERROR_FILE",
    "EXPR_ERROR_PICKLE_FILE",
    "EXPR_PARAM_FILE",
    "EXPR_PARAM_PICKLE_FILE",
    "EXPR_PROGRESS_FILE",
    "EXPR_RESULT_FILE",
    "TIME_THIS_ITER_S",
    "TIMESTAMP",
    "TRAINING_ITERATION",
)

_DEPRECATED_ALIASES = {
    _name: _DeprecatedAlias(
        f"ray.tune.result:{_name}",
        ((f"ray.tune.result.{_name}", ""),),
        note=(
            "See this issue for more context and migration options: "
            "https://github.com/ray-project/ray/issues/49454."
        ),
    )
    for _name in _MOVED_TO_TUNE_RESULT
}

__getattr__ = _make_deprecated_getattr(__name__, _DEPRECATED_ALIASES, "3.0")


def __dir__():
    return sorted(set(globals()) | set(_DEPRECATED_ALIASES))
