# Pattern: Linear Pipeline

This [deployment graph pattern](serve-deployment-graph-patterns-overview) is a linear pipeline of deployments. The request flows from each deployment to the next, getting transformed each time.

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/chain_nodes_same_class_different_args.svg)

## Code

```{literalinclude} ../../doc_code/deployment_graph_patterns/linear_pipeline.py
:language: python
:start-after: __graph_start__
:end-before: __graph_end__
```

## Execution

This graph has three nodes, which are all instances of the `Model` deployment. Each `Model` is constructed with a different `weight`, and its `forward` method adds that `weight` to the input.

The call graph calls each deployment's `forward` method, one after another, which adds all the `Model`'s `weights` to the input. The code executes the graph on an input of 0 and after adding all the weights (0, 1, and 2), it gets a final `sum` of 3:

```console
$ python linear_pipeline.py

3
```
