(serve-configure-deployment)=

# Configure Ray Serve deployments

This guide walks through the parameters that are configurable for a Ray Serve deployment, as well as the different locations where you can specify the parameters. You can also refer to the [API reference](../serve/api/doc/ray.serve.deployment_decorator.rst) for the `@serve.deployment` decorator.

## Configurable parameters

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

## How to configure a deployment

There are 3 ways to specify the parameters mentioned above. Two ways are through your application code, and the third way is through the Serve Config file that is recommended for production.

  - In the `@serve.deployment` decorator -

```{literalinclude} ../serve/doc_code/configure_serve_deployment/model_deployment.py
:start-after: __deployment_start__
:end-before: __deployment_end__
:language: python
```

  - Through `options()` -

```{literalinclude} ../serve/doc_code/configure_serve_deployment/model_deployment.py
:start-after: __deployment_end__
:end-before: __options_end__
:language: python
```

  - Lastly, you can configure deployments through the Serve config file. The recommended way to deploy and update your applications in production is through the Serve Config file. Learn more about how to use the Serve Config in the [production guide](https://docs.ray.io/en/latest/serve/production-guide/config.html).

## Overriding deployment settings

The order of priority is (from highest to lowest):

1. Serve Config file
2. `.options()` call in python code referenced above
3. `@serve.deployment` decorator in python code
4. Serve defaults

For example, if a deployment's `num_replicas` is specified in the config file and their graph code, Serve will use the config file's value. If it's only specified in the code, Serve will use the code value. If the user doesn't specify it anywhere, Serve will use a default (which is `num_replicas=1`).

Keep in mind that this override order is applied separately to each individual parameter.
For example, if a user has a deployment `ExampleDeployment` with the following decorator:

```python
@serve.deployment(
    num_replicas=2,
    max_concurrent_queries=15,
)
class ExampleDeployment:
    ...
```

and the following config file:

```yaml
...

deployments:

    - name: ExampleDeployment
      num_replicas: 5

...
```

Serve sets `num_replicas=5`, using the config file value, and `max_concurrent_queries=15`, using the code value (because `max_concurrent_queries` wasn't specified in the config file). All other deployment settings use Serve defaults because the user didn't specify them in the code or the config.

:::{tip}
Remember that `ray_actor_options` counts as a single setting. The entire `ray_actor_options` dictionary in the config file overrides the entire `ray_actor_options` dictionary from the graph code. If there are individual options within `ray_actor_options` (e.g. `runtime_env`, `num_gpus`, `memory`) that are set in the code but not in the config, Serve still won't use the code settings if the config has a `ray_actor_options` dictionary. It treats these missing options as though the user never set them and uses defaults instead. This dictionary overriding behavior also applies to `user_config` and `autoscaling_config`.
:::

