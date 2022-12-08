# Pattern: Conditional

This [deployment graph pattern](serve-deployment-graph-patterns-overview) allows you to control your graph's flow using conditionals. You can use this pattern to introduce a dynamic path for your requests to flow through.

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/control_flow_based_on_user_inputs.svg)

## Code

```{literalinclude} ../../doc_code/deployment_graph_patterns/conditional.py
:language: python
:start-after: __graph_start__
:end-before: __graph_end__
```

:::{note}
`combine` takes in intermediate values from the call graph as the individual arguments, `value1` and `value2`. You can also aggregate and pass these intermediate values as a list argument. However, this list contains references to the values, rather than the values themselves. You must explicitly use `await` to get the actual values before using them. Use `await` instead of `ray.get` to avoid [blocking the deployment](serve-model-composition-await-warning).

For example:
```python
dag = combine.bind([output1, output2], user_input[1])
...
@serve.deployment
async def combine(value_refs, combine_type):
   values = await value_refs
   value1, value2 = values
...
```
:::

## Execution

The graph creates two `Model` nodes, with `weights` of 0 and 1. It then takes the `user_input` and unpacks it into two parts: a number and an operation.

:::{note}
`handle.predict.remote()` can take an arbitrary number of arguments. These arguments can be unpacked by indexing into the `InputNode`. For example,

```python
with InputNode() as user_input:
   input_number, input_operation = user_input[0], user_input[1]
```
:::

It passes the number into the two `Model` nodes, similar to the [branching input](deployment-graph-pattern-branching-input) pattern. Then it passes the requested operation, as well as the intermediate outputs, to the `combine` deployment to get a final result.

The example script makes two requests to the graph, both with a number input of 1. The resulting calculations are

`max`:

```
input = 1
output1 = input + weight_1 = 0 + 1 = 1
output2 = input + weight_2 = 1 + 1 = 2
combine_output = max(output1, output2) = max(1, 2) = 2
```

`sum`:

```
input = 1
output1 = input + weight_1 = 0 + 1 = 1
output2 = input + weight_2 = 1 + 1 = 2
combine_output = output1 + output2 = 1 + 2 = 3
```

The final outputs are 2 and 3:

```
$ python conditional.py

2
3
```
