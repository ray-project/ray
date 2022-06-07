# Pattern: Visualize DAG during development

The example shows how to iteratively develop and visualize your deployment graph. For a runnable DAG, we will show both full and partial DAG depending on your choice of root node.

Please ensure do install dependencies in order to generate visualizations `sudo apt-get install -y graphviz` and `pip install -U pydot`.

## Code

+++

```{eval-rst}
.. literalinclude:: ../doc_code/visualize_dag_during_deployment.py
   :language: python
```

## Outputs

```{note}
The node of user choice will become the root of the graph for both execution as well as visualization, where non-reachable nodes from root will be ignored regardless if they appeared in user code.
```
In the development phase, when we picked `m1_output` as the root, we can see a visualization of the underlying execution path that's partial of the entire graph.

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/visualize_partial.svg)

Similarly, when we choose the final dag output, we will capture all nodes used in execution as they're reachable from the root.

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/visualize_full.svg)

```{tip}
If you run the code above within Jupyter notebook, we will automatically display it within cell. Otherwise you can either print the dot file as string and render it in graphviz tools such as https://dreampuf.github.io/GraphvizOnline, or save it as .dot file on disk with your choice of path.
```

+++