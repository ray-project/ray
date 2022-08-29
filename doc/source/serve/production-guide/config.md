(serve-in-production-config-file)=

# Serve Config Files (`serve build`)

This section should help you:

- understand the Serve config file format.
- understand how to generate and update a config file for a Serve application.

This config file can be used with the [serve deploy](serve-in-production-deploying) command CLI or embedded in a [RayService](serve-in-production-kubernetes) custom resource in Kubernetes to deploy and update your application in production.
The file is written in YAML and has the following format:

```yaml
import_path: ...

runtime_env: ...

deployments:

    - name: ...
      num_replicas: ...
      ...

    - name:
      ...

    ...
```

The file contains the following fields:

- An `import_path`, which is the path to your top-level Serve deployment (or the same path passed to `serve run`). The most minimal config file consists of only an `import_path`.
- A `runtime_env` that defines the environment that the application will run in. This is used to package application dependencies such as `pip` packages (see {ref}`Runtime Environments <runtime-environments>` for supported fields). Note that the `import_path` must be available _within_ the `runtime_env` if it's specified.
- A list of `deployments`. This is optional and allows you to override the `@serve.deployment` settings specified in the deployment graph code. Each entry in this list must include the deployment `name`, which must match one in the code. If this section is omitted, Serve launches all deployments in the graph with the settings specified in the code.

Below is an equivalent config for the [`FruitStand` example](serve-in-production-example):

```yaml
import_path: fruit:deployment_graph

runtime_env: {}

deployments:

    - name: FruitMarket
      num_replicas: 2

    - name: MangoStand
      user_config:
        price: 3

    - name: OrangeStand
      user_config:
        price: 2

    - name: PearStand
      user_config:
        price: 4

    - name: DAGDriver
```

The file uses the same `fruit:deployment_graph` import path that was used with `serve run` and it has five entries in the `deployments` list– one for each deployment. All the entries contain a `name` setting and some other configuration options such as `num_replicas` or `user_config`.

:::{tip}
Each individual entry in the `deployments` list is optional. In the example config file above, we could omit the `PearStand`, including its `name` and `user_config`, and the file would still be valid. When we deploy the file, the `PearStand` deployment will still be deployed, using the configurations set in the `@serve.deployment` decorator from the deployment graph's code.
:::

We can also auto-generate this config file from the code. The `serve build` command takes an import path to your deployment graph and it creates a config file containing all the deployments and their settings from the graph. You can tweak these settings to manage your deployments in production.

Using the `FruitStand` deployment graph example:

```console
$ ls
fruit.py

$ serve build fruit:deployment_graph -o fruit_config.yaml

$ ls
fruit.py
fruit_config.yaml
```

(fruit-config-yaml)=

The `fruit_config.yaml` file contains:

```yaml
import_path: fruit:deployment_graph

runtime_env: {}

deployments:

- name: MangoStand
  num_replicas: 2
  route_prefix: null
  max_concurrent_queries: 100
  user_config:
    price: 3
  autoscaling_config: null
  graceful_shutdown_wait_loop_s: 2.0
  graceful_shutdown_timeout_s: 20.0
  health_check_period_s: 10.0
  health_check_timeout_s: 30.0
  ray_actor_options: null

- name: OrangeStand
  num_replicas: 1
  route_prefix: null
  max_concurrent_queries: 100
  user_config:
    price: 2
  autoscaling_config: null
  graceful_shutdown_wait_loop_s: 2.0
  graceful_shutdown_timeout_s: 20.0
  health_check_period_s: 10.0
  health_check_timeout_s: 30.0
  ray_actor_options: null

- name: PearStand
  num_replicas: 1
  route_prefix: null
  max_concurrent_queries: 100
  user_config:
    price: 4
  autoscaling_config: null
  graceful_shutdown_wait_loop_s: 2.0
  graceful_shutdown_timeout_s: 20.0
  health_check_period_s: 10.0
  health_check_timeout_s: 30.0
  ray_actor_options: null

- name: FruitMarket
  num_replicas: 2
  route_prefix: null
  max_concurrent_queries: 100
  user_config: null
  autoscaling_config: null
  graceful_shutdown_wait_loop_s: 2.0
  graceful_shutdown_timeout_s: 20.0
  health_check_period_s: 10.0
  health_check_timeout_s: 30.0
  ray_actor_options: null

- name: DAGDriver
  num_replicas: 1
  route_prefix: /
  max_concurrent_queries: 100
  user_config: null
  autoscaling_config: null
  graceful_shutdown_wait_loop_s: 2.0
  graceful_shutdown_timeout_s: 20.0
  health_check_period_s: 10.0
  health_check_timeout_s: 30.0
  ray_actor_options: null

```

Note that the `runtime_env` field will always be empty when using `serve build` and must be set manually.

## Overriding deployment settings

Settings from `@serve.deployment` can be overriden with this Serve config file. The order of priority is (from highest to lowest):

1. Config File
2. Deployment graph code (either through the `@serve.deployment` decorator or a `.set_options()` call)
3. Serve defaults

For example, if a deployment's `num_replicas` is specified in the config file and their graph code, Serve will use the config file's value. If it's only specified in the code, Serve will use the code value. If the user doesn't specify it anywhere, Serve will use a default (which is `num_replicas=1`).

Keep in mind that this override order is applied separately to each individual setting.
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

Serve will set `num_replicas=5`, using the config file value, and `max_concurrent_queries=15`, using the code value (since `max_concurrent_queries` wasn't specified in the config file). All other deployment settings use Serve defaults since the user didn't specify them in the code or the config.

:::{tip}
Remember that `ray_actor_options` counts as a single setting. The entire `ray_actor_options` dictionary in the config file overrides the entire `ray_actor_options` dictionary from the graph code. If there are individual options within `ray_actor_options` (e.g. `runtime_env`, `num_gpus`, `memory`) that are set in the code but not in the config, Serve still won't use the code settings if the config has a `ray_actor_options` dictionary. It will treat these missing options as though the user never set them and will use defaults instead. This dictionary overriding behavior also applies to `user_config`.
:::
