(serve-in-production)=

# Putting Ray Serve Deployment Graphs in Production

This section should help you:

- develop and test your Serve deployment graph locally
- understand the Serve config file format
- deploy, inspect, and update your Serve application in production

```{contents}
```

(serve-in-production-testing)=

## Developing and Testing Your Serve Deployment Graph with `serve run`

You can test your Serve deployment graph using the Serve CLI's `serve run` command. The `serve run` command launches a temporary Ray cluster, deploys the graph to it, and blocks. Then, you can send HTTP requests to test your application. When your graph receives and processes these requests, it will output `print` and `logging` statements to the terminal. Once you're finished testing your graph, you can type `ctrl-C` to kill the temporary Ray cluster and tear down your graph. You can use this pattern to quickly run, debug, and iterate on your Serve deployment graph.

Let's use this graph as an example:

```{literalinclude} ../serve/doc_code/production_fruit_example.py
:language: python
:start-after: __fruit_example_begin__
:end-before: __fruit_example_end__
```

This graph is located in the `fruit.py` file and stored in the `deployment_graph` variable. It takes in requests containing a list of two values: a fruit name and an amount. It returns the total price for the batch of fruits.

To run the deployment graph, we first navigate to the same directory containing the `fruit.py` file and then run `serve run fruit.deployment_graph`. `fruit.deployment_graph` is the deployment graph's import path (assuming we are running `serve run` in the same directory as `fruit.py`).

```console
# Terminal Window 1

$ ls
fruit.py

$ serve run fruit.deployment_graph
2022-06-21 13:07:01,966  INFO scripts.py:253 -- Deploying from import path: "fruit.deployment_graph".
2022-06-21 13:07:03,774  INFO services.py:1477 -- View the Ray dashboard at http://127.0.0.1:8265
...
2022-06-21 13:07:08,076  SUCC scripts.py:266 -- Deployed successfully.
```

We can test this graph by opening a new terminal window and making requests with Python's [requests](https://requests.readthedocs.io/en/latest/) library.

```console
# Terminal Window 2

$ python3

>>> import requests
>>> requests.post("http://localhost:8000/", json=["PEAR", 2]).json()
    8
```

Once we're finished, we can close the Python interpreter by running `quit()` and terminate the Ray cluster by typing `ctrl-C` int the terminal running `serve run`. This will tear down the deployments and then the cluster.

(serve-in-production-config-file)=

## Creating Your Serve Config File with `serve build`

You can create a Serve config file to manage your deployment graphs' configurations in production. The Serve CLI can "deploy" this file, using the [serve deploy](serve-in-production-deploying) command. This will deploy or update your deployment graphs in production. The file is written in YAML and has the following format:

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

The `import_path` is the deployment graph's import path. When you deploy your config file, Serve will import your deployment graph using this path. Similarly, the `runtime_env` is the deployment graph's runtime environment. Serve will import the deployment graph inside this environment.

The `deployments` section is optional. If it's omitted, Serve will launch the deployment graph (and all its deployments). The graph will run with any deployment settings specified in the `@serve.deployment` decorators from the graph's code. If you want to override these decorator settings from the code, you can include a `deployments` section in the file. You can add an entry of deployment settings to the `deployments` list. The only required setting in each list entry is the deployment `name`, which must match one of the deployments from the graph's code. You can include any settings from the `@serve.deployment` decorator inside the entry, **except** `init_args` and `init_kwargs`, which must be set in the graph's code itself.

For example, let's take the `FruitStand` deployment graph from the [previous section](serve-in-production-testing). An equivalent config would be:

```yaml
import_path: fruit.deployment_graph

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

The file uses the same `fruit.deployment_graph` import path, and it has five entries in the `deployments` list– one for each deployment. All the entries contain a `name` setting (the only required setting when including an entry) as well as additional settings (such as `num_replicas` or `user_config`) depending on the deployment.

Note how this config specifies the same settings as the `@serve.deployment` decorators from the deployment graph's code. We can change or add to these settings to override the settings from the decorators.

:::{tip}
Each individual entry in the `deployments` list is optional. In the example config file above, we could omit the `PearStand`, including its `name` and `user_config`, and the file would still be valid. When we deploy the file, the `PearStand` deployment will still be deployed, using the configurations set in the `@serve.deployment` decorator from the deployment graph's code.
:::

We can also auto-generate this config file. The `serve build` command takes an import path to your deployment graph, and it creates a config file containing all the deployments and their settings from the graph. You can tweak these settings to manage you deployments in production.

Using the `FruitStand` deployment graph example:

```console
$ ls
fruit.py

$ serve build fruit.deployment_graph -o fruit_config.yaml

$ ls
fruit.py
fruit_config.yaml
```

(fruit-config-yaml)=

The `fruit_config.yaml` file contains:

```yaml
import_path: fruit.deployment_graph

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

Note that the `runtime_env` field will always be empty when using `serve build`. That field must be set manually.

### Overriding Deployment Settings

Settings from `@serve.deployment` can be overriden with this Serve config file. The order of priority is (from highest to lowest):

1. Config File
2. Deployment graph code (either through the `@serve.deployment` decorator or a `.set_options()` call)
3. Serve defaults

For example, if a deployment's `num_replicas` is specified in the config file and their graph code, Serve will use the config file's value. If it's only specified in the code, Serve will use the code value. If the user doesn't specify it anywhere, Serve will use a default (which is `num_replicas=1`).

Keep in mind that this override order is at the settings-level. For example, if a user has a deployment `ExampleDeployment` with the following decorator:

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
Remember that `ray_actor_options` is an independent setting. The entire `ray_actor_options` dictionary in the config file overrides the entire `ray_actor_options` dictionary from the graph code. If there are individual options within `ray_actor_options` (e.g. `runtime_env`, `num_gpus`, `memory`) that are set in the code but not in the config, Serve still won't use the code settings if the config has a `ray_actor_options` dictionary. It will treat these missing options as though the user never set them (and use defaults instead) since the entire `ray_actor_options` dictionary in the config overrides the one in the code. This dictionary overriding behavior also applies to `user_config`.
:::

(serve-in-production-deploying)=

## Deploying Your Serve Application to Production with `serve deploy`

You can deploy your Serve application to production using the config file and the `serve deploy` CLI command. `serve deploy` takes in a config file path, and it deploys that file to a Ray cluster.

Let's deploy the [fruit_config.yaml](fruit-config-yaml) file from the previous section:

```console
$ ls
fruit.py
fruit_config.yaml

$ ray start --head
...

$ serve deploy fruit_config.yaml
2022-06-20 17:26:31,106	SUCC scripts.py:139 -- 
Sent deploy request successfully!
 * Use `serve status` to check deployments' statuses.
 * Use `serve config` to see the running app's config.
```

`ray start --head` starts a long-lived Ray cluster locally. `serve deploy fruit_config.yaml` deploys the `fruit_config.yaml` file to this local cluster. To stop your Ray cluster, you can run the CLI command `ray stop`.

The message `Sent deploy request successfully!` means:
* The Ray cluster has received your config file successfully.
* It will start a new Serve application if one hasn't already started.
* The Serve application will deploy the deployments from your deployment graph, updated with the configurations from your config file.

It does **not** mean that your Serve application, including your deployments, has already started running successfully. This happens asynchronously as the Ray cluster attempts to update itself to match the settings from your config file. Check out the [next section](serve-in-production-inspecting) to learn more about how to inspect your deployments.

### Adding a Runtime Environment

If you start Ray and deploy your deployment graph from a directory that doesn't contain the graph code, your deployments will fail to run. This happens because your import path is generally location-dependent. For example, the import path `fruit.deployment_graph` assumes the current directory contains the `fruit.py` module, which contains a `deployment_graph` object.

To make your config file location-independent, you can push your deployment graph code to [a remote repository and add that repository to your config file's `runtime_env` field](remote-uris). When Serve runs your deployment graph, it will pull the code from the remote repository rather than use a local copy. **This is a best practice** because it lets you deploy your config file from any machine in any directory and share the file with other developers, making it a more standalone artifact.

As an example, we have [pushed a copy of the FruitStand deployment graph to GitHub](https://github.com/ray-project/test_dag/blob/c620251044717ace0a4c19d766d43c5099af8a77/fruit.py). You can use this config file to deploy the `FruitStand` deployment graph to your own Ray cluster even if you don't have the code locally:

```yaml
import_path: fruit.deployment_graph

runtime_env:
    working_dir: "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
```

:::{note}
As a side note, you could also package your deployment graph into a standalone Python package that can be imported using a [PYTHONPATH](https://docs.python.org/3.10/using/cmdline.html#envvar-PYTHONPATH) to provide location independence on your local machine. However, it's still best practice to use a `runtime_env`, to ensure consistency across all machines in your cluster.
:::

(serve-in-production-remote-cluster)=

### Using a Remote Cluster

By default, `serve deploy` deploys to a cluster running locally. However, you should also use `serve deploy` whenever you want to deploy your Serve application to a remote cluster. `serve deploy` takes in an optional `--address/-a` argument where you can specify the dashboard address of your remote Ray cluster. This address should be of the form:

```
[YOUR_RAY_CLUSTER_URI]:[DASHBOARD PORT]
```

As an example, the address for the local cluster started by `ray start --head` is `http://127.0.0.1:8265`. We can explicitly deploy to this address using the command

```console
$ serve deploy config_file.yaml -a http://127.0.0.1:8265
```

The Ray dashboard's default port is 8265. This port may be different if:
* You explicitly set it using the `--dashboard-port` argument when running `ray start`.
* Port 8265 was unavailable when Ray started. In that case, the dashboard port is incremented until an available port is found. E.g. if 8265 is unavailable, the port becomes 8266. If that's unavailable, it becomes 8267, and so on.

:::{tip}
By default, all the Serve CLI commands assume that you're working with a local cluster, so if you don't specify an `--address/-a` value, they use the Ray address associated with a local cluster started by `ray start --head`. However, if the `RAY_ADDRESS` environment variable is set, all Serve CLI commands will default to that value instead (unless you also specify an `--address/-a` value).

You can check this variable's value by running:

```console
$ echo $RAY_ADDRESS
```

You can set this variable by running the CLI command:

```console
$ export RAY_ADDRESS=[YOUR VALUE]
```

You can unset this variable by running the CLI command:

```console
$ unset RAY_ADDRESS
```

Check for this variable in your environment to make sure you're using your desired Ray address.
:::

(serve-in-production-inspecting)=

## Inspecting Your Serve Application in Production with `serve config` and `serve status`

The Serve CLI offers two commands to help you inspect your Serve application in production: `serve config` and `serve status`.

If you're working with a remote cluster, `serve config` and `serve status` also offer an `--address/-a` argument to access your cluster. Check out [the previous section](serve-in-production-remote-cluster) for more info on this argument.

(serve-in-production-config-command)=

### `serve config`

`serve config` gets the latest config file the Ray cluster received. This config file represents the Serve application's goal state. The Ray cluster will constantly attempt to reach and maintain this state by deploying deployments, recovering failed replicas, and more.

Using the `fruit_config.yaml` example from [an earlier section](fruit-config-yaml):

```console
$ ray start --head
$ serve deploy fruit_config.yaml
...

$ serve config
import_path: fruit.deployment_graph

runtime_env: {}

deployments:

- name: MangoStand
  num_replicas: 2
  route_prefix: null
...
```

(serve-in-production-status-command)=

### `serve status`

`serve status` gets your Serve application's current status. It's divided into two parts: the `app_status` and the `deployment_statuses`.

The `app_status` contains three fields:
* `status`: a Serve application has three possible statuses:
    * `"DEPLOYING"`: the application is currently carrying out a `serve deploy` request. It is deploying new deployments or updating existing ones.
    * `"RUNNING"`: the application is at steady-state. It has finished executing any previous `serve deploy` requests, and it is attempting to maintain the goal state set by the latest `serve deploy` request.
    * `"DEPLOY_FAILED"`: the latest `serve deploy` request has failed.
* `message`: provides context on the current status.
* `deployment_timestamp`: a unix timestamp of when Serve received the last `serve deploy` request. This is calculated using the `ServeController`'s local clock.

The `deployment_statuses` contains a list of dictionaries representing each deployment's status. Each dictionary has three fields:
* `name`: the deployment's name.
* `status`: a Serve deployment has three possible statuses:
    * `"UPDATING"`: the deployment is updating to meet the goal state set by a previous `deploy` request.
    * `"HEALTHY"`: the deployment is at the latest requests goal state.
    * `"UNHEALTHY"`: the deployment has either failed to update, or it has updated and has become unhealthy afterwards. This may be due to an error in the deployment's constructor, a crashed replica, or a general system or machine error.
* `message`: provides context on the current status.

You can use the `serve status` command to inspect your deployments after they are deployed and throughout their lifetime.

Using the `fruit_config.yaml` example from [an earlier section](fruit-config-yaml):

```console
$ ray start --head
$ serve deploy fruit_config.yaml
...

$ serve status
app_status:
  status: RUNNING
  message: ''
  deployment_timestamp: 1655771534.835145
deployment_statuses:
- name: MangoStand
  status: HEALTHY
  message: ''
- name: OrangeStand
  status: HEALTHY
  message: ''
- name: PearStand
  status: HEALTHY
  message: ''
- name: FruitMarket
  status: HEALTHY
  message: ''
- name: DAGDriver
  status: HEALTHY
  message: ''
```

`serve status` can also be used with KubeRay, a Kubernetes operator for Ray Serve, to help deploy your Serve applications with Kubernetes. There's also work in progress to provide closer integrations between some of the features from this document, like `serve status`, with Kubernetes to provide a clearer Serve deployment story.

(serve-in-production-updating)=

## Updating Your Serve Application in Production

You can also update your Serve applications once they're in production. You can update the settings in your config file and redeploy it using the `serve deploy` command.

Let's use the `FruitStand` deployment graph [from an earlier section](fruit-config-yaml) as an example. All the individual fruit deployments contain a `reconfigure()` method. [This method allows us to issue lightweight updates](managing-deployments-user-configuration) to our deployments by updating the `user_config`. These updates don't need to tear down the running deployments, meaning there's less downtime as the deployments update.

First let's deploy the graph. Make sure to stop any previous Ray cluster using the CLI command `ray stop` for this example:

```console
$ ray start --head
$ serve deploy fruit_config.yaml
...

$ python

>>> import requests
>>> requests.post("http://localhost:8000/", json=["MANGO", 2]).json()

6
```

Now, let's update the price of mangos in our deployment. We can change the `price` attribute in the `MangoStand` deployment to `5` in our config file:

```yaml
import_path: fruit.deployment_graph

runtime_env: {}

deployments:

- name: MangoStand
  num_replicas: 2
  route_prefix: null
  max_concurrent_queries: 100
  user_config:
    # price: 3 (Outdated price)
    price: 5
  autoscaling_config: null
  graceful_shutdown_wait_loop_s: 2.0
  graceful_shutdown_timeout_s: 20.0
  health_check_period_s: 10.0
  health_check_timeout_s: 30.0
  ray_actor_options: null

...
```

Without stopping the Ray cluster, we can redeploy our graph using `serve deploy`:

```console
$ serve deploy fruit_config.yaml
...
```

We can inspect our deployments with `serve status`. Once the `app_status`'s `status` returns to `"RUNNING"`, we can try our requests one more time:

```console
$ serve status
app_status:
  status: RUNNING
  message: ''
  deployment_timestamp: 1655776483.457707
deployment_statuses:
- name: MangoStand
  status: HEALTHY
  message: ''
- name: OrangeStand
  status: HEALTHY
  message: ''
- name: PearStand
  status: HEALTHY
  message: ''
- name: FruitMarket
  status: HEALTHY
  message: ''
- name: DAGDriver
  status: HEALTHY
  message: ''

$ python

>>> import requests
>>> requests.post("http://localhost:8000/", json=["MANGO", 2]).json()

10
```

The price has updated! The same request now returns `10` instead of `6`, reflecting the new price.

You can update any setting in any deployment in the config file similarly. You can also add new deployment settings or remove old deployment settings from the config. This is because `serve deploy` is **idempotent**. Your Serve application's will match the one specified in the latest config you deployed– regardless of what config files you deployed before that.

:::{warning}
Although you can update your Serve application by deploying an entirely new deployment graph using a different `import_path` and a different `runtime_env`, this is NOT recommended in production.

The best practice for large-scale code updates is to start a new Ray cluster, deploy the updated code to it using `serve deploy`, and then switch traffic from your old cluster to the new one.
:::

## Best Practices

This section summarizes the best practices when deploying to production:

* Use `serve run` to manually test and improve your deployment graph locally.
* Use `serve build` to create a Serve config file for your deployment graph.
    * Put your deployment graph's code in a remote repository and manually configure the `working_dir` or `py_modules` fields in your Serv config file's `runtime_env` to point to that repository.
* Use `serve deploy` to deploy your graph and its deployments to your Ray cluster. After the deployment is finished, you can start serving traffic from your cluster.
* Use `serve status` to track your Serve application's health and deployment progress.
* Use `serve config` to check the latest config that your Serve application received. This is its goal state.
* Make lightweight configuration updates (e.g. `num_replicas` or `user_config` changes) by modifying your Serve config file and redeploying it with `serve deploy`.
* Make heavyweight code updates (e.g. `runtime_env` changes) by starting a new Ray cluster, updating your Serve config file, and deploying the file with `serve deploy` to the new cluster. Once the new deployment is finished, switch your traffic to the new cluster.
