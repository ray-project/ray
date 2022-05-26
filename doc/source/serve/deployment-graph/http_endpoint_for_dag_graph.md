# Pattern: Http endpoint for dag graph

The example shows how to add endpoint for dag graph

## Code

+++

```{eval-rst}
.. literalinclude:: ../doc_code/deployment_graph_dag_http.py
   :language: python
```

````{note}
1. Serve provide a special driver (DAGDriver) to accept the http request and drive the dag graph execution
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