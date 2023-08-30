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
...

applications:

- name: app1
  route_prefix: /
  import_path: fruit:deployment_graph
  runtime_env: {}
  deployments:
  - name: MangoStand
    user_config:
      # price: 3 (outdated price)
      price: 5

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
proxies:
  0eeaadc5f16b64b8cd55aae184254406f0609370cbc79716800cb6f2: HEALTHY
applications:
  app1:
    status: RUNNING
    message: ''
    last_deployed_time_s: 1693430845.863128
    deployments:
      MangoStand:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
      OrangeStand:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
      PearStand:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
      FruitMarket:
        status: HEALTHY
        replica_states:
          RUNNING: 2
        message: ''
      DAGDriver:
        status: HEALTHY
        replica_states:
          RUNNING: 1
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
