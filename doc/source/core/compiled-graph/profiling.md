---
myst:
  html_meta:
    description: "Profile Ray Compiled Graph execution with the PyTorch or Nsight profilers to find task-level and system overhead bottlenecks."
---

# Profiling

Ray Compiled Graph provides both PyTorch-based and Nsight-based profiling functionalities to better understand the performance of individual tasks, system overhead, and performance bottlenecks. You can pick your favorite profiler based on your preference.

## PyTorch profiler

To run PyTorch profiling on Compiled Graph, set the environment variable `RAY_CGRAPH_ENABLE_TORCH_PROFILING=1` when you run the script. For example, for a Compiled Graph script in `example.py`, run the following command:

```bash
RAY_CGRAPH_ENABLE_TORCH_PROFILING=1 python3 example.py
```

After execution, Compiled Graph generates the profiling results in the `compiled_graph_torch_profiles` directory under the current working directory. Compiled Graph generates one trace file per actor.

To visualize the traces, use the [Perfetto UI](https://ui.perfetto.dev/).

## Nsight system profiler

Compiled Graph builds on Ray's profiling capabilities and uses Nsight system profiling.

To run Nsight profiling on Compiled Graph, specify the runtime environment for the actors involved, as described in {ref}`Run Nsight on Ray <run-nsight-on-ray>`. The following example shows this setup:

```{literalinclude} ../doc_code/cgraph_profiling.py
:language: python
:start-after: __profiling_setup_start__
:end-before: __profiling_setup_end__
```

Then, create a Compiled Graph as usual.

```{literalinclude} ../doc_code/cgraph_profiling.py
:language: python
:start-after: __profiling_execution_start__
:end-before: __profiling_execution_end__
```

Finally, run the script as usual.

```bash
python3 example.py
```

After execution, Compiled Graph generates the profiling results under the `/tmp/ray/session_*/logs/{profiler_name}` directory.

For fine-grained performance analysis of method calls and system overhead, set the environment variable `RAY_CGRAPH_ENABLE_NVTX_PROFILING=1` when you run the script:

```bash
RAY_CGRAPH_ENABLE_NVTX_PROFILING=1 python3 example.py
```

This command uses the [NVIDIA Tools Extension (NVTX) library](https://nvtx.readthedocs.io/en/latest/index.html#) to automatically annotate all methods called in the execution loops of the compiled graph.

To visualize the profiling results, follow the instructions in {ref}`Nsight Profiling Result <profiling-result>`.

## Visualization

To visualize the graph structure, call the {func}`visualize <ray.dag.compiled_dag_node.CompiledDAG.visualize>` method after calling {func}`experimental_compile <ray.dag.DAGNode.experimental_compile>` on the graph.

```{literalinclude} ../doc_code/cgraph_visualize.py
:language: python
:start-after: __cgraph_visualize_start__
:end-before: __cgraph_visualize_end__
```

By default, Ray generates a PNG image named `compiled_graph.png` and saves it in the current working directory. This requires `graphviz`.

The following image shows the visualization for the preceding code. Tasks that belong to the same actor are the same color.

```{image} ../../images/compiled_graph_viz.png
:alt: Visualization of the graph structure
:align: center
```
