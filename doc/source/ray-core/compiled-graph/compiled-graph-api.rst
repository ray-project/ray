Compiled Graph API
==================

Input and Output Nodes
----------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.dag.input_node.InputNode
    ray.dag.output_node.MultiOutputNode

DAG Construction
----------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.actor.ActorMethod.bind
    ray.dag.DAGNode.with_tensor_transport
    ray.experimental.compiled_dag_ref.CompiledDAGRef

Compiled Graph Operations
-------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.dag.DAGNode.experimental_compile
    ray.dag.compiled_dag_node.CompiledDAG.execute
    ray.dag.compiled_dag_node.CompiledDAG.visualize

Configurations
--------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ray.dag.context.DAGContext
