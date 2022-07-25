# Pattern: Combine two nodes with passing same input in parallel

The example shows how to pass same input to two nodes in parallel and combine the outputs

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/deployment_graph_combine_two_nodes_with_passing_same_input_parallel.svg)

## Code

+++

```{eval-rst}
.. literalinclude:: ../doc_code/deployment_graph_combine_two_nodes_with_passing_input_parallel.py
   :language: python
```

## Outputs

The graph will pass input into two nodes and sum the outputs of the two model.\
Model output1: 1(input) + 0(weight) = 1 \
Model output2: 1(input) + 1(weight) = 2 \
Combine sum: 1 (output1) + 2 (output2) = 3

```
3
```

+++