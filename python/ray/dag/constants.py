import os

# Reserved keys used to handle ClassMethodNode in Ray DAG building.
PARENT_CLASS_NODE_KEY = "parent_class_node"
PREV_CLASS_METHOD_CALL_KEY = "prev_class_method_call"
BIND_INDEX_KEY = "bind_index"
IS_CLASS_METHOD_OUTPUT_KEY = "is_class_method_output"

# Reserved keys used to handle CollectiveOutputNode in Ray DAG building.
COLLECTIVE_OPERATION_KEY = "collective_operation"

# Reserved key to distinguish DAGNode type and avoid collision with user dict.
DAGNODE_TYPE_KEY = "__dag_node_type__"

# Feature flag to turn off the deadlock detection.
RAY_ADAG_ENABLE_DETECT_DEADLOCK = (
    os.environ.get("RAY_ADAG_ENABLE_DETECT_DEADLOCK", "1") == "1"
)

# Feature flag to turn on profiling.
RAY_ADAG_ENABLE_PROFILING = os.environ.get("RAY_ADAG_ENABLE_PROFILING", "0") == "1"

# Feature flag to turn on visualization of the execution schedule.
RAY_ADAG_VISUALIZE_SCHEDULE = os.environ.get("RAY_ADAG_VISUALIZE_SCHEDULE", "0") == "1"
