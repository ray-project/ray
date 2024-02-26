(serve-configure-deployment)=

# Configure Ray Serve deployments

Ray Serve default values for deployments are a good starting point for exploration. To further tailor scaling behavior, resource management, or performance tuning, you can configure parameters to alter the default behavior of Ray Serve deployments.

Use this guide to learn the essentials of configuring deployments:
- What parameters you can configure for a Ray Serve deployment
- The different locations where you can specify the parameters.

## Configurable parameters

You can also refer to the [API reference](../serve/api/doc/ray.serve.deployment_decorator.rst) for the `@serve.deployment` decorator.

- `name` - Name uniquely identifying this deployment within the application. If not provided, the name of the class or function is used.
- `num_replicas` - Number of replicas to run that handle requests to this deployment. Defaults to 1.
- `ray_actor_options` - Options to pass to the Ray Actor decorator, such as resource requirements. Valid options are: `accelerator_type`, `memory`, `num_cpus`, `num_gpus`, `object_store_memory`, `resources`, and `runtime_env` For more details - [Resource management in Serve](serve-cpus-gpus)
- `max_concurrent_queries` - Maximum number of queries that are sent to a replica of this deployment without receiving a response. Defaults to 100. This may be an important parameter to configure for [performance tuning](serve-perf-tuning).
- `autoscaling_config` - Parameters to configure autoscaling behavior. If this is set, you can't set `num_replicas`. For more details on configurable parameters for autoscaling, see [Ray Serve Autoscaling](serve-autoscaling). 
- `user_config` -  Config to pass to the reconfigure method of the deployment. This can be updated dynamically without restarting the replicas of the deployment. The user_config must be fully JSON-serializable. For more details, see [Serve User Config](serve-user-config). 
- `health_check_period_s` - Duration between health check calls for the replica. Defaults to 10s. The health check is by default a no-op Actor call to the replica, but you can define your own health check using the "check_health" method in your deployment that raises an exception when unhealthy.
- `health_check_timeout_s` - Duration in seconds, that replicas wait for a health check method to return before considering it as failed. Defaults to 30s.
- `graceful_shutdown_wait_loop_s` - Duration that replicas wait until there is no more work to be done before shutting down. Defaults to 2s.
- `graceful_shutdown_timeout_s` - Duration to wait for a replica to gracefully shut down before being forcefully killed. Defaults to 20s.
- `logging_config` - Logging Config for the deployment (e.g. log level, log directory, JSON log format and so on). See [LoggingConfig](../serve/api/doc/ray.serve.schema.LoggingConfig.rst) for details.

## How to specify parameters

You can specify the above mentioned parameters in two locations:
1. In your application code.
2. In the Serve Config file, which is the recommended method for production.

### Specify parameters through the application code

You can specify parameters in the application code in two ways:
- In the `@serve.deployment` decorator when you first define a deployment
- With the `options()` method when you want to modify a deployment

Use the `@serve.deployment` decorator to specify deployment parameters when you are defining a deployment for the first time:

```{literalinclude} ../serve/doc_code/configure_serve_deployment/model_deployment.py
:start-after: __deployment_start__
:end-before: __deployment_end__
:language: python
```

Use the [`.options()`](../serve/api/doc/ray.serve.Deployment.rst) method to modify deployment parameters on an already-defined deployment. Modifying an existing deployment lets you reuse deployment definitions and dynamically set parameters at runtime.

```{literalinclude} ../serve/doc_code/configure_serve_deployment/model_deployment.py
:start-after: __deployment_end__
:end-before: __options_end__
:language: python
```

### Specify parameters through the Serve config file

In production, we recommend configuring individual deployments through the Serve config file. You can change parameter values without modifying your application code. Learn more about how to use the Serve Config in the [production guide](serve-in-production-config-file).

```yaml
applications:
- name: app1
  import_path: configure_serve:translator_app
  deployments:
  - name: Translator
    num_replicas: 2
    max_concurrent_queries: 100
    graceful_shutdown_wait_loop_s: 2.0
    graceful_shutdown_timeout_s: 20.0
    health_check_period_s: 10.0
    health_check_timeout_s: 30.0
    ray_actor_options:
      num_cpus: 0.2
      num_gpus: 0.0
```

### Order of Priority

You can set parameters to different values in various locations. For each individual parameter, the order of priority is (from highest to lowest):

1. Serve Config file
2. Application code (either through the `@serve.deployment` decorator or through `.options()`)
3. Serve defaults

In other words, if you specify a parameter for a deployment in the config file and the application code, Serve uses the config file's value. If it's only specified in the code, Serve uses the value you specified in the code. If you don't specify the parameter anywhere, Serve uses the default for that parameter.

For example, the following application code contains a single deployment `ExampleDeployment`:

```python
@serve.deployment(num_replicas=2, graceful_shutdown_timeout_s=6)
class ExampleDeployment:
    ...

example_app = ExampleDeployment.bind()
```

Then you deploy the application with the following config file:

```yaml
applications:
  - name: default
    import_path: models:example_app 
    deployments:
      - name: ExampleDeployment
        num_replicas: 5
```

Serve uses `num_replicas=5` from the value set in the config file and `graceful_shutdown_timeout_s=6` from the value set in the application code. All other deployment settings use Serve defaults because you didn't specify them in the code or the config. For instance, `health_check_period_s=10` because by default Serve health checks deployments once every 10 seconds.

:::{tip}
Remember that `ray_actor_options` counts as a single setting. The entire `ray_actor_options` dictionary in the config file overrides the entire `ray_actor_options` dictionary from the graph code. If you set individual options within `ray_actor_options` (e.g. `runtime_env`, `num_gpus`, `memory`) in the code but not in the config, Serve still won't use the code settings if the config has a `ray_actor_options` dictionary. It treats these missing options as though the user never set them and uses defaults instead. This dictionary overriding behavior also applies to `user_config` and `autoscaling_config`.
:::

