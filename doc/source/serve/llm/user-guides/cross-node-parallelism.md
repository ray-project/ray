(cross-node-parallelism)=
# Cross-node parallelism

Ray Serve LLM supports cross-node tensor parallelism (TP) and pipeline parallelism (PP), allowing you to distribute model inference across multiple GPUs and nodes. This capability enables you to:

- Deploy models that don't fit on a single GPU or node.
- Scale model serving across your cluster's available resources.
- Leverage Ray's placement group strategies to control worker placement for performance or fault tolerance.

::::{note}
By default, Ray Serve LLM uses the `PACK` placement strategy, which tries to place workers on as few nodes as possible. If workers can't fit on a single node, they automatically spill to other nodes. This enables cross-node deployments when single-node resources are insufficient.
::::

## Tensor parallelism

Tensor parallelism splits model weights across multiple GPUs, with each GPU processing a portion of the model's tensors for each forward pass. This approach is useful for models that don't fit on a single GPU.

The following example shows how to configure tensor parallelism across 2 GPUs:

::::{tab-set}

:::{tab-item} Python
:sync: python

```{literalinclude} ../../doc_code/cross_node_parallelism_example.py
:language: python
:start-after: __cross_node_tp_example_start__
:end-before: __cross_node_tp_example_end__
```
:::

::::

## Pipeline parallelism

Pipeline parallelism splits the model's layers across multiple GPUs, with each GPU processing a subset of the model's layers. This approach is useful for very large models where tensor parallelism alone isn't sufficient.

The following example shows how to configure pipeline parallelism across 2 GPUs:

::::{tab-set}

:::{tab-item} Python
:sync: python

```{literalinclude} ../../doc_code/cross_node_parallelism_example.py
:language: python
:start-after: __cross_node_pp_example_start__
:end-before: __cross_node_pp_example_end__
```
:::

::::

## Combined tensor and pipeline parallelism

For extremely large models, you can combine both tensor and pipeline parallelism. The total number of GPUs is the product of `tensor_parallel_size` and `pipeline_parallel_size`.

The following example shows how to configure a model with both TP and PP (4 GPUs total):

::::{tab-set}

:::{tab-item} Python
:sync: python

```{literalinclude} ../../doc_code/cross_node_parallelism_example.py
:language: python
:start-after: __cross_node_tp_pp_example_start__
:end-before: __cross_node_tp_pp_example_end__
```
:::

::::

## Custom placement groups

You can customize how Ray places vLLM engine workers across nodes through the `placement_group_config` parameter. This parameter accepts a dictionary with `bundles` (a list of resource dictionaries) and `strategy` (placement strategy).

Ray Serve LLM uses the `PACK` strategy by default, which tries to place workers on as few nodes as possible. If workers can't fit on a single node, they automatically spill to other nodes. For more details on all available placement strategies, see {ref}`Ray Core's placement strategies documentation <pgroup-strategy>`.

::::{note}
Data parallel deployments automatically override the placement strategy to `STRICT_PACK` because each replica must be co-located for correct data parallel behavior.
::::

While you can specify the degree of tensor and pipeline parallelism, the specific assignment of model ranks to GPUs is managed by the vLLM engine and can't be directly configured through the Ray Serve LLM API. Ray Serve automatically injects accelerator type labels into bundles and merges the first bundle with replica actor resources (CPU, GPU, memory).

The following example shows how to use the `SPREAD` strategy to distribute workers across multiple nodes for fault tolerance:

::::{tab-set}

:::{tab-item} Python
:sync: python

```{literalinclude} ../../doc_code/cross_node_parallelism_example.py
:language: python
:start-after: __custom_placement_group_spread_example_start__
:end-before: __custom_placement_group_spread_example_end__
```
:::

::::

