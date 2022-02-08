# Ray dag constant used in DAG building API to mark entrypoints of a DAG
# Should only be function or class method. A DAG can have multiple entrypoints.

# Ex:
#             A.forward
#          /            \
#     input               ensemble -> output
#          \            /
#             B.forward

# In this pipeline, each user input is broadcasted to both A.forward and
# B.forward as first stop of the DAG, and authored like

# a = A.forward.bind(ray.dag.INPUT)
# b = B.forward.bind(ray.dag.INPUT)
# dag = ensemble.bind(a, b)

# dag.execute(user_input) --> broadcast to a and b
INPUT = "__INPUT"
