---
description: "Learn about using labels to control how Ray schedules tasks, actors, and placement groups to nodes in your Kubernetes cluster."
---

(labels)=
# Use labels to control scheduling

In Ray version 2.49.0 and above, you can use labels to control scheduling for KubeRay. Labels are supported as a beta feature.

This page provides a conceptual overview and usage instructions for labels. Labels are key-value pairs that provide a human-readable configuration for users to control how Ray schedules tasks, actors, and placement group bundles to specific nodes.

.. note:: Ray labels share the same syntax and formatting restrictions as Kubernetes labels, but are conceptually distinct. See the [Kubernetes docs on labels and selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set).

## How do labels work?

The following is a high-level overview of how you use labels to control scheduling:

- Ray sets default labels that describe the underlying compute. See [](defaults).
- You define custom labels as key-value pairs. See [](custom).
- You specify *label selectors* in your Ray code to define label requirements. You can specify these requirements at the task, actor, or placement group bundle level. See [](label-selectors).
- Ray schedules tasks, actors or placement group bundles based on the specified label selectors.
- If you're using a dynamic cluster with autoscaler V2 enabled, the cluster scales up to add new nodes from a designated worker group to fulfill label requirements.

(defaults)=
## Default node labels 

During cluster initilization or as autoscaling events add nodes to your cluster, Ray assigns the following default labels to each node:

| Label | Description |
| --- | --- |
| `ray.io/node-id` | A unique ID generated for the node. |
| `ray.io/accelerator-type` | The accelerator type of the node, for example `L4`. CPU-only machines have an empty string. See {ref}`accelerator types <accelerator-types>` for a mapping of values. |

.. note:: You can override default values using `ray start` parameters.

The following are examples of default labels:

```python
"ray.io/accelerator-type": "" # Default label indicating the machine is CPU-only.
```

(custom)=
## Define custom labels

You can add custom labels to your nodes using the `--labels` or `--labels-file` parameter when running `ray start`. See the following examples:

<!-- INSERT EXAMPLES -->

.. note:: You can't set labels using `ray.init()`. Local Ray clusters don't support labels.

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
| In | Label matches on of the provided values. | `{“key”: “in(val1,val2)”}`
| Not in | Label matches none of the provided values. | `{“key”: “!in(val1,val2)”}`

You can specify one or more label selectors as a dict. When specifying multiple label selectors, the candidate node must meet all requirements. The following example configuration uses a custom label to require an `m5.16xlarge` EC2 instance and a default label to require a spot instance:

```python
label_selector={"instance_type": "m5.16xlarge", “ray.io/market_type”: “spot”}  
```

## Specify label requirements for Tasks & Actors

Use the following syntax to add label selectors to tasks and actors:

```python
@ray.remote(label_selector={"label_name":"label_value"})
def f():
    pass
```

<!-- INSERT ADDITIONAL EXAMPLES AS DESIRED -->

## Specify label requirements for placement group bundles

Use the `bundle_label_selector` option to add label selector to placement group bundles. See the following examples:

```python
# All bundles require the same labels:
ray.util.placement_group(
    bundles=[{"GPU": 1}, {"GPU": 1}],
    bundle_label_selector=[{"ray.io/accelerator-type": "H100"} * 2],
)

# Bundles require different labels:
ray.util.placement_group(
    bundles=[{"CPU": 1}] + [{"GPU": 1} * 2],
    bundle_label_selector=[{"ray.io/market-type": "spot"}] + [{"ray.io/accelerator-type": "H100"} * 2]
)
```

<!-- Commenting out until code is provided

### An end-to-end example

TBD -->

## Monitor nodes using labels

The Ray dashboard automatically shows the following information:
- Labels for each node.
- Label selectors set for each task, actor, or placement group bundle.

<!-- ADD LINKS TO THE ABOVE WHEN AVAILABLE -->

Within a task, you can programmatically obtain the node label from the RuntimeContextAPI using `ray.get_runtime_context().get_node_labels()`. This returns a Python dict.

You can also access information about node label and label selector information using the state API.

<!-- DJS: cannot figure out how to document this. -->

