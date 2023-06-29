(serve-handling-dependencies)=
# Handle Dependencies

(serve-runtime-env)=
## Add a runtime environment

The import path (e.g., `fruit:deployment_graph`) must be importable by Serve at runtime.
When running locally, this path might be in your current working directory.
However, when running on a cluster you also need to make sure the path is importable.
Build the code into the cluster's container image (see [Cluster Configuration](kuberay-config) for more details) or use a `runtime_env` with a [remote URI](remote-uris) that hosts the code in remote storage.

As an example, we have [pushed a copy of the FruitStand deployment graph to GitHub](https://github.com/ray-project/test_dag/blob/40d61c141b9c37853a7014b8659fc7f23c1d04f6/fruit.py). You can use this config file to deploy the `FruitStand` deployment graph to your own Ray cluster even if you don't have the code locally:

```yaml
import_path: fruit:deployment_graph

runtime_env:
    working_dir: "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
```

:::{note}
You can also package a deployment graph into a standalone Python package that you can import using a [PYTHONPATH](https://docs.python.org/3.10/using/cmdline.html#envvar-PYTHONPATH) to provide location independence on your local machine. However, the best practice is to use a `runtime_env`, to ensure consistency across all machines in your cluster.
:::

## Dependencies per deployment

Ray Serve also supports serving deployments with different (and possibly conflicting)
Python dependencies.  For example, you can simultaneously serve one deployment
that uses legacy Tensorflow 1 and another that uses Tensorflow 2.

This is supported on Mac OS and Linux using Ray's {ref}`runtime-environments` feature.
As with all other Ray actor options, pass the runtime environment in via `ray_actor_options` in
your deployment.  Be sure to first run `pip install "ray[default]"` to ensure the
Runtime Environments feature is installed.

Example:

```{literalinclude} ../../../../python/ray/serve/examples/doc/conda_env.py
```

:::{tip}
Avoid dynamically installing packages that install from source: these can be slow and
use up all resources while installing, leading to problems with the Ray cluster.  Consider
precompiling such packages in a private repository or Docker image.
:::

The dependencies required in the deployment may be different than
the dependencies installed in the driver program (the one running Serve API
calls). In this case, you should use a delayed import within the class to avoid
importing unavailable packages in the driver.  This applies even when not
using runtime environments.

Example:

```{literalinclude} ../../../../python/ray/serve/examples/doc/delayed_import.py
```
