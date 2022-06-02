# Pattern: Control flow based on the user inputs

The example shows how to user inputs to control the graph flow

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/control_flow_based_on_user_inputs.svg)

## Code

+++

```{eval-rst}
.. literalinclude:: ../doc_code/deployment_graph_control_flow_based_on_user_inputs.py
   :language: python
```

````{note}
1. The dag.execute() take arbitrary number of arguments, and internally we implemented data objects to facilitate accessing by index or key.


   code example:
   ```python
   dag = combine.bind(output1, output2, user_input[1])
   ```


2. value1 and value2 are ObjectRef passed into the combine, the value of ObjectRef will be resolved at the runtime.

3. we can pass value1 and value2 as a list. In this case, we are passing the ObjectRef as reference, the value of ObjectRef will not be addressed automatically. We need to explicitly use ray.get() to address value before we do sum() or max() function. ([passing objects by reference](https://docs.ray.io/en/latest/ray-core/objects.html?#passing-objects-by-reference))

   code example:
   ```python
   dag = combine.bind([output1, output2], user_input[1])
   ...
   @serve.deployment
   def combine(value_refs, combine_type):
       values = ray.get(value_refs)
   ...
   ```
````

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