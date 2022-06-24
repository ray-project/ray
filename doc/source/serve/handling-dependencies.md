# Handling Dependencies

Ray Serve supports serving deployments with different (possibly conflicting)
Python dependencies.  For example, you can simultaneously serve one deployment
that uses legacy Tensorflow 1 and another that uses Tensorflow 2.

This is supported on Mac OS and Linux using Ray's {ref}`runtime-environments` feature.
As with all other Ray actor options, pass the runtime environment in via `ray_actor_options` in
your deployment.  Be sure to first run `pip install "ray[default]"` to ensure the
Runtime Environments feature is installed.

Example:

```{literalinclude} ../../../python/ray/serve/examples/doc/conda_env.py
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

```{literalinclude} ../../../python/ray/serve/examples/doc/delayed_import.py
```
