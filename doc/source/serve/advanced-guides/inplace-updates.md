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
Let's use the text summarization and translation application [from the production guide](production-config-yaml) as an example. Both of the individual deployments contain a `reconfigure()` method. This method allows us to issue lightweight updates to our deployments by updating the `user_config`.

First let's deploy the graph. Make sure to stop any previous Ray cluster using the CLI command `ray stop` for this example:

```console
$ ray start --head
$ serve deploy serve_config.yaml
```

Then send a request to the application:
```{literalinclude} ../doc_code/production_guide/text_ml.py
:language: python
:start-after: __start_client__
:end-before: __end_client__
```

Now, let's change the language that the text is translated into from French to German. We can change the `language` attribute in the `Translator` user config:

```yaml
...

applications:
- name: default
  route_prefix: /
  import_path: text_ml:app
  runtime_env:
    pip:
      - torch
      - transformers
  deployments:
  - name: Translator
    user_config:
      language: german

...
```

Without stopping the Ray cluster, we can redeploy our app using `serve deploy`:

```console
$ serve deploy serve_config.yaml
...
```

We can inspect our deployments with `serve status`. Once the `app_status`'s `status` returns to `"RUNNING"`, we can try our requests one more time:

```console
$ serve status
proxies:
  cef533a072b0f03bf92a6b98cb4eb9153b7b7c7b7f15954feb2f38ec: HEALTHY
applications:
  default:
    status: RUNNING
    message: ''
    last_deployed_time_s: 1694041157.2211847
    deployments:
      Translator:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
      Summarizer:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
```

The language was updated! Now the returned text is in German instead of French.
```{literalinclude} ../doc_code/production_guide/text_ml.py
:language: python
:start-after: __start_second_client__
:end-before: __end_second_client__
```

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
