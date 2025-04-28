(serve-inplace-updates)=

# Updating Applications In-Place

You can update your Serve applications once they're in production by updating the settings in your config file and redeploying it using the `serve deploy` command. In the redeployed config file, you can add new deployment settings or remove old deployment settings. This is because `serve deploy` is **idempotent**, meaning your Serve application's config always matches (or honors) the latest config you deployed successfully – regardless of what config files you deployed before that.

(serve-in-production-lightweight-update)=

## Lightweight Config Updates

Lightweight config updates modify running deployment replicas without tearing them down and restarting them, so there's less downtime as the deployments update. For each deployment, modifying the following values is considered a lightweight config update, and won't tear down the replicas for that deployment:
- `num_replicas`
- `autoscaling_config`
- `user_config`
- `max_ongoing_requests`
- `graceful_shutdown_timeout_s`
- `graceful_shutdown_wait_loop_s`
- `health_check_period_s`
- `health_check_timeout_s`

(serve-updating-user-config)=

## Updating the user config
This example uses the text summarization and translation application [from the production guide](production-config-yaml). Both of the individual deployments contain a `reconfigure()` method. This method allows you to issue lightweight updates to the deployments by updating the `user_config`.

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

Change the language that the text is translated into from French to German by changing the `language` attribute in the `Translator` user config:

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
    num_replicas: 1
    user_config:
      language: german

...
```

Without stopping the Ray cluster, redeploy the app using `serve deploy`:

```console
$ serve deploy serve_config.yaml
...
```

We can inspect our deployments with `serve status`. Once the application's `status` returns to `RUNNING`, we can try our request one more time:

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

The language has updated. Now the returned text is in German instead of French.
```{literalinclude} ../doc_code/production_guide/text_ml.py
:language: python
:start-after: __start_second_client__
:end-before: __end_second_client__
```

## Code Updates

Changing the following values in a deployment's config will trigger redeployment and restart all the deployment's replicas.
- `ray_actor_options`
- `placement_group_bundles`
- `placement_group_strategy`

Changing the following application-level config values is also considered a code update, and all deployments in the application will be restarted.
- `import_path`
- `runtime_env`

:::{warning}
Although you can update your Serve application by deploying an entirely new deployment graph using a different `import_path` and a different `runtime_env`, this is NOT recommended in production.

The best practice for large-scale code updates is to start a new Ray cluster, deploy the updated code to it using `serve deploy`, and then switch traffic from your old cluster to the new one.
:::
