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
RAY_CGRAPH_ENABLE_DETECT_DEADLOCK = (
    os.environ.get("RAY_CGRAPH_ENABLE_DETECT_DEADLOCK", "1") == "1"
)

# Feature flag to turn on profiling.
RAY_CGRAPH_ENABLE_PROFILING = os.environ.get("RAY_CGRAPH_ENABLE_PROFILING", "0") == "1"

# Feature flag to turn on NVTX (NVIDIA Tools Extension Library) profiling.
# With this flag, Compiled Graph uses nvtx to automatically annotate and profile
# function calls during each actor's execution loop.
# This cannot be used together with RAY_CGRAPH_ENABLE_TORCH_PROFILING.
RAY_CGRAPH_ENABLE_NVTX_PROFILING = (
    os.environ.get("RAY_CGRAPH_ENABLE_NVTX_PROFILING", "0") == "1"
)

# Feature flag to turn on torch profiling.
# This cannot be used together with RAY_CGRAPH_ENABLE_NVTX_PROFILING.
RAY_CGRAPH_ENABLE_TORCH_PROFILING = (
    os.environ.get("RAY_CGRAPH_ENABLE_TORCH_PROFILING", "0") == "1"
)

# Feature flag to turn on visualization of the execution schedule.
RAY_CGRAPH_VISUALIZE_SCHEDULE = (
    os.environ.get("RAY_CGRAPH_VISUALIZE_SCHEDULE", "0") == "1"
)
