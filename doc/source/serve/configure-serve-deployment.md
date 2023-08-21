(serve-configure-deployment)=

# Configure Ray Serve deployments

These parameters are configurable on a Ray Serve deployment. Documentation is also in the [API reference](../serve/api/doc/ray.serve.deployment_decorator.rst).

Configure the following parameters either in the Serve config file, or on the `@serve.deployment` decorator:

- `name` - Name uniquely identifying this deployment within the application. If not provided, the name of the class or function is used.
- `num_replicas` - Number of replicas to run that handle requests to this deployment. Defaults to 1.
- `route_prefix` - Requests to paths under this HTTP path prefix are routed to this deployment. Defaults to ‘/{name}’. This can only be set for the ingress (top-level) deployment of an application.
- `ray_actor_options` - Options to pass to the Ray Actor decorator, such as resource requirements. Valid options are: `accelerator_type`, `memory`, `num_cpus`, `num_gpus`, `object_store_memory`, `resources`, and `runtime_env` For more details - [Resource management in Serve](serve-cpus-gpus)
- `max_concurrent_queries` - Maximum number of queries that are sent to a replica of this deployment without receiving a response. Defaults to 100. This may be an important parameter to configure for [performance tuning](serve-perf-tuning).
- `autoscaling_config` - Parameters to configure autoscaling behavior. If this is set, num_replicas cannot be set. For more details on configurable parameters for autoscaling - [Ray Serve Autoscaling](ray-serve-autoscaling). 
- `user_config` -  Config to pass to the reconfigure method of the deployment. This can be updated dynamically without restarting the replicas of the deployment. The user_config must be fully JSON-serializable. For more details - [Serve User Config](serve-user-config). 
- `health_check_period_s` - Duration between health check calls for the replica. Defaults to 10s. The health check is by default a no-op Actor call to the replica, but you can define your own health check using the "check_health" method in your deployment that raises an exception when unhealthy.
- `health_check_timeout_s` - Duration in seconds, that replicas wait for a health check method to return before considering it as failed. Defaults to 30s.
- `graceful_shutdown_wait_loop_s` - Duration that replicas wait until there is no more work to be done before shutting down. Defaults to 2s.
- `graceful_shutdown_timeout_s` - Duration to wait for a replica to gracefully shut down before being forcefully killed. Defaults to 20s.
- `is_driver_deployment` - [EXPERIMENTAL] when set, exactly one replica of this deployment runs on every node (like a daemon set).

There are 3 ways of specifying parameters:

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

  - Using the YAML [Serve Config file](serve-in-production-config-file) -

```yaml
applications:

- name: app1

  route_prefix: /

  import_path: configure_serve:translator_app

  runtime_env: {}

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

## Overriding deployment settings

The order of priority is (from highest to lowest):

1. Serve Config file
2. `.options()` call in python code referenced above
3. `@serve.deployment` decorator in python code
4. Serve defaults

For example, if a deployment's `num_replicas` is specified in the config file and their graph code, Serve will use the config file's value. If it's only specified in the code, Serve will use the code value. If the user doesn't specify it anywhere, Serve will use a default (which is `num_replicas=1`).

Keep in mind that this override order is applied separately to each individual parameter.
For example, if a user has a deployment `ExampleDeployment` with the following decorator:

```{testcode}
from ray import serve


@serve.deployment(
    num_replicas=2,
    max_concurrent_queries=15,
)
class ExampleDeployment:
    pass
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

(serve-user-config)=
## Dynamically changing parameters without restarting your replicas (`user_config`)

You can use the `user_config` field to supply structured configuration for your deployment. You can pass arbitrary JSON serializable objects to the YAML configuration. Serve then applies it to all running and future deployment replicas. The application of user configuration *does not* restart the replica. This means you can use this field to dynamically:
- adjust model weights and versions without restarting the cluster.
- adjust traffic splitting percentage for your model composition graph.
- configure any feature flag, A/B tests, and hyper-parameters for your deployments.

To enable the `user_config` feature, you need to implement a `reconfigure` method that takes a JSON-serializable object (e.g., a Dictionary, List or String) as its only argument:

```{testcode}
from ray import serve
from typing import Any, Dict


@serve.deployment
class Model:
    def reconfigure(self, config: Dict[str, Any]):
        self.threshold = config["threshold"]
```

If the `user_config` is set when the deployment is created (e.g., in the decorator or the Serve config file), this `reconfigure` method is called right after the deployment's `__init__` method, and the `user_config` is passed in as an argument. You can also trigger the `reconfigure` method by updating your Serve config file with a new `user_config` and reapplying it to your Ray cluster. See [In-place Updates](serve-inplace-updates) for more information.

The corresponding YAML snippet is:

```yaml
...
deployments:
    - name: Model
      user_config:
        threshold: 1.5
```
