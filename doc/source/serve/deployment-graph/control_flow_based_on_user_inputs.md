# Pattern: Control flow based on the user inputs

The example shows how to user inputs to control the graph flow

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/control_flow_based_on_user_inputs.svg)

## Code

+++

```{eval-rst}
.. literalinclude:: ../doc_code/deployment_graph_control_flow_based_on_user_inputs.py
   :language: python
```

## Outputs

The code uses 'max' to do combine from the output of the two models.

Model output1: 1(input) + 0(weight) = 1 \
Model output2: 1(input) + 1(weight) = 2 \
So the combine max is 2, the combine sum is 3

```
2
3
```

+++