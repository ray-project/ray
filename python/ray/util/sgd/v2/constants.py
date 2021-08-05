# Autofilled sgd.report() metrics. Keys should be consistent with Tune.
TIMESTAMP = "timestamp"
TIME_THIS_ITER_S = "time_this_iter_s"
TRAINING_ITERATION = "training_iteration"

DATE = "date"
HOSTNAME = "hostname"
NODE_IP = "node_ip"
PID = "pid"
TIME_TOTAL_S = "time_total_s"

# Will not be reported unless SGD_RESULT_ENABLE_DETAILED_AUTOFILLED_METRICS
# env var is not 0
DETAILED_AUTOFILLED_KEYS = {DATE, HOSTNAME, NODE_IP, PID, TIME_TOTAL_S}

# Time between BackendExecutor.fetch_next checks when fetching
# new results after signaling the training function to continue.
RESULT_FETCH_TIMEOUT = 0.2

# Default filename for JSON logger
RESULT_FILE_JSON = "results.json"
