# Pattern: Http endpoint for dag graph

This example shows how to configure ingress component of the deployment graph, such as HTTP endpoint prefix, HTTP to python object input adapter.

## Code

+++

```{eval-rst}
.. literalinclude:: ../doc_code/deployment_graph_dag_http.py
   :language: python
```

````{note}
1. Serve provide a special driver ([DAGDriver](deployment-graph-e2e-tutorial.md)) to accept the http request and drive the dag graph execution
2. User can specify the customized http adapter to adopt the cusomized input format
````

## Outputs

Model output1: 1(input) + 0(weight) + 4(len("test")) = 5 \
Model output2: 1(input) + 1(weight) + 4(len("test")) = 6 \
So the combine sum is 11
```
11
11
```

+++