---
description: "Learn about using labels to control how Ray schedules tasks, actors, and placement groups to nodes in your Kubernetes cluster."
---

(labels)=
# Use labels to control scheduling

In Ray version 2.49.0 and above, you can use labels to control scheduling for KubeRay. Labels are a beta feature.

This page provides a conceptual overview and usage instructions for labels. Labels are key-value pairs that provide a human-readable configuration for users to control how Ray schedules tasks, actors, and placement group bundles to specific nodes.


```{note} 
Ray labels share the same syntax and formatting restrictions as Kubernetes labels, but are conceptually distinct. See the [Kubernetes docs on labels and selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set).
```


## How do labels work?

The following is a high-level overview of how you use labels to control scheduling:

- Ray sets default labels that describe the underlying compute. See [](defaults).
- You define custom labels as key-value pairs. See [](custom).
- You specify *label selectors* in your Ray code to define label requirements. You can specify these requirements at the task, actor, or placement group bundle level. See [](label-selectors).
- Ray schedules tasks, actors, or placement group bundles based on the specified label selectors.
- In Ray 2.50.0 and above, if you're using a dynamic cluster with autoscaler V2 enabled, the cluster scales up to add new nodes from a designated worker group to fulfill label requirements.

(defaults)=
## Default node labels 
```{note}
Ray reserves all labels under ray.io namespace.
```
During cluster initialization or as autoscaling events add nodes to your cluster, Ray assigns the following default labels to each node:

| Label | Description |
| --- | --- |
| `ray.io/node-id` | A unique ID generated for the node. |
| `ray.io/accelerator-type` | The accelerator type of the node, for example `L4`. CPU-only machines don't have the label. See {ref}`accelerator types <accelerator-types>` for a mapping of values. |

```{note} 
You can override default values using `ray start` parameters.
```

The following are examples of default labels:

```python
"ray.io/accelerator-type": "L4" # Default label indicating the machine has Nvidia L4 GPU
```

(custom)=
## Define custom labels

You can add custom labels to your nodes using the `--labels` or `--labels-file` parameter when running `ray start`.

```bash
# Examples 1: Start a head node with cpu-family and test-label labels
ray start --head --labels="cpu-family=amd,test-label=test-value"

# Example 2: Start a head node with labels from a label file
ray start --head --labels-files='./test-labels-file'

# The file content can be the following (should be a valid YAML file):
# "test-label": "test-value"
# "test-label-2": "test-value-2"
```

```{note} 
You can't set labels using `ray.init()`. Local Ray clusters don't support labels.
```

(label-selectors)=
## Specify label selectors

You add label selector logic to your Ray code when defining Ray tasks, actors, or placement group bundles. Label selectors define the label requirements for matching your Ray code to a node in your Ray cluster.

Label selectors specify the following:

- The key of the label.
- Operator logic for matching.
- The value or values to match on.

The following table shows the basic syntax for label selector operator logic:

| Operator | Description | Example syntax |
| --- | --- | --- |
| Equals | Label matches exactly one value. | `{“key”: “value”}`
| Not equal | Label matches anything by one value. | `{“key”: “!value”}`
| In | Label matches one of the provided values. | `{“key”: “in(val1,val2)”}`
| Not in | Label matches none of the provided values. | `{“key”: “!in(val1,val2)”}`

You can specify one or more label selectors as a dict. When specifying multiple label selectors, the candidate node must meet all requirements. The following example configuration uses a custom label to require an `m5.16xlarge` EC2 instance and a default label to require node ID to be 123:

```python
label_selector={"instance_type": "m5.16xlarge", "ray.io/node-id": "123"}  
```

## Specify label requirements for tasks and actors

Use the following syntax to add label selectors to tasks and actors:

```python
# An example for specifing label_selector in task's @ray.remote annotation
@ray.remote(label_selector={"label_name":"label_value"})
def f():
    pass

# An example of specifying label_selector in actor's @ray.remote annotation
@ray.remote(label_selector={"ray.io/accelerator-type": "H100"})
class Actor:
    pass

# An example of specifying label_selector in task's options
@ray.remote
def test_task_label_in_options():
    pass

test_task_label_in_options.options(label_selector={"test-lable-key": "test-label-value"}).remote()

# An example of specifying label_selector in actor's options
@ray.remote
class Actor:
    pass

actor_1 = Actor.options(
    label_selector={"ray.io/accelerator-type": "H100"},
).remote()
```

## Specify label requirements for placement group bundles

Use the `bundle_label_selector` option to add label selector to placement group bundles. See the following examples:

```python
# All bundles require the same labels:
ray.util.placement_group(
    bundles=[{"GPU": 1}, {"GPU": 1}],
    bundle_label_selector=[{"ray.io/accelerator-type": "H100"}] * 2,
)

# Bundles require different labels:
ray.util.placement_group(
    bundles=[{"CPU": 1}] + [{"GPU": 1}] * 2,
    bundle_label_selector=[{"ray.io/market-type": "spot"}] + [{"ray.io/accelerator-type": "H100"}] * 2
)
```
## Using labels with autoscaler

Autoscaler V2 supports label-based scheduling. To enable autoscaler to scale up nodes to fulfill label requirements, you need to create multiple worker groups for different label requirement combinations and specify all the corresponding labels in the `rayStartParams` field in the Ray cluster configuration. For example:

```python
    rayStartParams: {
      labels: "region=me-central1,ray.io/accelerator-type=H100"
    }
```

## Monitor nodes using labels

The Ray dashboard automatically shows the following information:
- Labels for each node. See {py:attr}`ray.util.state.common.NodeState.labels`.
- Label selectors set for each task, actor, or placement group bundle. See {py:attr}`ray.util.state.common.TaskState.label_selector` and {py:attr}`ray.util.state.common.ActorState.label_selector`.

Within a task, you can programmatically obtain the node label from the RuntimeContextAPI using `ray.get_runtime_context().get_node_labels()`. This returns a Python dict. See the following example:

```python
@ray.remote
def test_task_label():
   node_labels = ray.get_runtime_context().get_node_labels()
   print(f"[test_task_label] node labels: {node_labels}")

"""
Example output:
(test_task_label pid=68487) [test_task_label] node labels: {'test-label-1': 'test-value-1', 'test-label-key': 'test-label-value', 'test-label-2': 'test-value-2'}
"""
```
You can also access information about node label and label selector information using the state API and state CLI.
