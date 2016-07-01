# Utilities to deal with computation graphs

import graphviz

def graph_to_graphviz(computation_graph):
  """
  Convert the computation graph to graphviz format.

  Args:
    computation_graph [graph_pb2.CompGraph]: protocol buffer description of
      the computation graph

  Returns:
    Graphviz description of the computation graph
  """
  dot = graphviz.Digraph(format="pdf")
  dot.node("op-root", shape="box")
  for (i, op) in enumerate(computation_graph.operation):
    if op.HasField("task"):
      dot.node("op" + str(i), shape="box", label=str(i) + "\n" + op.task.name.split(".")[-1])
      for res in op.task.result:
        dot.edge("op" + str(i), str(res))
    elif op.HasField("put"):
      dot.node("op" + str(i), shape="box", label=str(i) + "\n" + "put")
      dot.edge("op" + str(i), str(op.put.objref))
    elif op.HasField("get"):
      dot.node("op" + str(i), shape="box", label=str(i) + "\n" + "get")
    creator_operationid = op.creator_operationid if op.creator_operationid != 2 ** 64 - 1 else "-root"
    dot.edge("op" + str(creator_operationid), "op" + str(i), style="dotted", constraint="false")
    for arg in op.task.arg:
      if not arg.HasField("obj"):
        dot.node(str(arg.ref))
        dot.edge(str(arg.ref), "op" + str(i))
  return dot
