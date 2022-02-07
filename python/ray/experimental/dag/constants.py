"""
Ray dag constant used in DAG building API to mark entrypoints of a DAG
Should only be function or class method. A DAG can have multiple entrypoints.

Ex:
            A.forward
         /            \
    input               ensemble -> output
         \            /
            B.forward

In this pipeline, each user input is broadcasted to both A.forward and
B.forward as first stop of the DAG, and authored like

a = A.forward.bind(ray.dag.DAG_ENTRY_POINT)
b = B.forward.bind(ray.dag.DAG_ENTRY_POINT)
dag = ensemble.bind(a, b)

dag.execute(user_input) --> broadcast to a and b
"""
DAG_ENTRY_POINT = "__dag_entry_point"
