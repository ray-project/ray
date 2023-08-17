(serve-inplace-updates)=

# In-Place Updates to Serve

You can update your Serve applications once they're in production by updating the settings in your config file and redeploying it using the `serve deploy` command. In the redeployed config file, you can add new deployment settings or remove old deployment settings. This is because `serve deploy` is **idempotent**, meaning your Serve application's config always matches (or honors) the latest config you deployed successfully – regardless of what config files you deployed before that.

(serve-in-production-lightweight-update)=

## Lightweight Config Updates

Lightweight config updates modify running deployment replicas without tearing them down and restarting them, so there's less downtime as the deployments update. For each deployment, modifying `num_replicas`, `autoscaling_config`, and/or `user_config` is considered a lightweight config update, and won't tear down the replicas for that deployment.

:::{note}
Lightweight config updates are only possible for deployments that are included as entries under `deployments` in the config file. If a deployment is not included in the config file, replicas of that deployment will be torn down and brought up again each time you redeploy with `serve deploy`.
:::

(serve-updating-user-config)=

## Updating User Config
Let's use the `FruitStand` deployment graph [from the production guide](fruit-config-yaml) as an example. All the individual fruit deployments contain a `reconfigure()` method. This method allows us to issue lightweight updates to our deployments by updating the `user_config`.

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
import_path: fruit:deployment_graph

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

## Code Updates

Similarly, you can update any other setting in any deployment in the config file. If a deployment setting other than `num_replicas`, `autoscaling_config`, or `user_config` is changed, it is considered a code update, and the deployment replicas will be restarted. Note that the following modifications are all considered "changes", and will trigger tear down of replicas:
* changing an existing setting
* adding an override setting that was previously not present in the config file
* removing a setting from the config file

Note also that changing `import_path` or `runtime_env` is considered a code update for all deployments, and will tear down all running deployments and restart them.

:::{warning}
Although you can update your Serve application by deploying an entirely new deployment graph using a different `import_path` and a different `runtime_env`, this is NOT recommended in production.

The best practice for large-scale code updates is to start a new Ray cluster, deploy the updated code to it using `serve deploy`, and then switch traffic from your old cluster to the new one.
:::
