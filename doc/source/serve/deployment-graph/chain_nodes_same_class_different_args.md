# Pattern: Chain nodes with same class and different args

The example shows how to chain nodes using the same class and having different args passed in

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/chain_nodes_same_class_different_args.svg)

## Code

+++

```{eval-rst}
.. literalinclude:: ../doc_code/deployment_graph_same_class_different_args.py
   :language: python
```

## Outputs

The graph will add all nodes weights plus the input (which is 0 in this case).
0(input) + 0(weight) + 1(weight) + 2(weight) = 3

```
3
```

+++