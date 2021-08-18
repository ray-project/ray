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
