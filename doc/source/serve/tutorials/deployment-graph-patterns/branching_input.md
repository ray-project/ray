(deployment-graph-pattern-branching-input)=

# Pattern: Branching Input

This [deployment graph pattern](serve-deployment-graph-patterns-overview) lets you pass the same input to multiple deployments in parallel. You can then aggregate these deployments' intermediate outputs in another deployment.

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/deployment_graph_combine_two_nodes_with_passing_same_input_parallel.svg)

## Code

```{literalinclude} ../../doc_code/deployment_graph_patterns/branching_input.py
:language: python
:start-after: __graph_start__
:end-before: __graph_end__
```

## Execution

This graph includes two `Model` nodes, with `weights` of 0 and 1. It passes the input into the two `Models`, and they add their own weights to it. Then, it uses the `combine` deployment to add the two `Model` deployments' outputs together.

The resulting calculation is:

```
input = 1
output1 = input + weight_1 = 0 + 1 = 1
output2 = input + weight_2 = 1 + 1 = 2
combine_output = output1 + output2 = 1 + 2 = 3
```

The final output is 3:

```
$ python branching_input.py

3
```
