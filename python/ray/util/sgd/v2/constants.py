from pathlib import Path

try:
    TUNE_INSTALLED = True
    from ray import tune  # noqa: F401
except ImportError:
    TUNE_INSTALLED = False

# Autofilled sgd.report() metrics. Keys should be consistent with Tune.
TIMESTAMP = "_timestamp"
TIME_THIS_ITER_S = "_time_this_iter_s"
TRAINING_ITERATION = "_training_iteration"

BASIC_AUTOFILLED_KEYS = {TIMESTAMP, TIME_THIS_ITER_S, TRAINING_ITERATION}

DATE = "_date"
HOSTNAME = "_hostname"
NODE_IP = "_node_ip"
PID = "_pid"
TIME_TOTAL_S = "_time_total_s"

# Env var name
ENABLE_DETAILED_AUTOFILLED_METRICS_ENV = (
    "SGD_RESULT_ENABLE_DETAILED_AUTOFILLED_METRICS")

# Will not be reported unless ENABLE_DETAILED_AUTOFILLED_METRICS_ENV
# env var is not 0
DETAILED_AUTOFILLED_KEYS = {DATE, HOSTNAME, NODE_IP, PID, TIME_TOTAL_S}

# Time between Session.get_next checks when fetching
# new results after signaling the training function to continue.
RESULT_FETCH_TIMEOUT = 0.2

# Default filename for JSON logger
RESULT_FILE_JSON = "results.json"

# Default directory where all SGD logs, checkpoints, etc. will be stored.
DEFAULT_RESULTS_DIR = Path("~/ray_results").expanduser()

# File name to use for checkpoints saved with Tune
TUNE_CHECKPOINT_FILE_NAME = "checkpoint"

# The key to use to specify the checkpoint id for Tune.
# This needs to be added to the checkpoint dictionary so if the Tune trial
# is restarted, the checkpoint_id can continue to increment.
TUNE_CHECKPOINT_ID = "_current_checkpoint_id"
