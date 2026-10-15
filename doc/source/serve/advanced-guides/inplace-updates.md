---
myst:
  html_meta:
    description: "Update a running Serve application in place with lightweight config updates, user_config changes, or a full code redeploy."
---

(serve-inplace-updates)=

# Update applications in place

To update a Serve application in production, change the settings in your Serve config file and redeploy it with the `serve deploy` command. The redeployed config file can add new deployment settings or remove old ones. This works because `serve deploy` is *idempotent*. Your Serve application's config always matches the latest config you deployed successfully, regardless of which config files you deployed before it.

(serve-in-production-lightweight-update)=

## Lightweight config updates

A lightweight config update modifies running replicas without tearing them down and restarting them, so the deployments have less downtime while they update. Changing any of the following values in a deployment's config is a lightweight config update and doesn't tear down that deployment's replicas:

- `num_replicas`
- `autoscaling_config`
- `user_config`
- `max_ongoing_requests`
- `graceful_shutdown_timeout_s`
- `graceful_shutdown_wait_loop_s`
- `health_check_period_s`
- `health_check_timeout_s`

(serve-updating-user-config)=

(updating-the-user-config)=
## Update the user config

This example uses the text summarization and translation application [from the production guide](production-config-yaml). Both deployments contain a `reconfigure()` method, so you can issue lightweight updates to them by changing the `user_config`.

For this example, first stop any previous Ray cluster with the `ray stop` CLI command. Then start a cluster and deploy the application:

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

To translate the text into German instead of French, change the `language` attribute in the `Translator` deployment's user config:

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

Without stopping the Ray cluster, redeploy the application with `serve deploy`:

```console
$ serve deploy serve_config.yaml
...
```

Check the deployments with `serve status`. When the application's `status` returns to `RUNNING`, send the request again:

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

The language update took effect, so the returned text is in German instead of French.

```{literalinclude} ../doc_code/production_guide/text_ml.py
:language: python
:start-after: __start_second_client__
:end-before: __end_second_client__
```

## Code updates

Changing any of the following values in a deployment's config triggers a redeployment and restarts all of the deployment's replicas:

- `ray_actor_options`
- `placement_group_bundles`
- `placement_group_strategy`

:::{note}
Code updates use a rolling restart rather than stopping all replicas at once. Ray replaces up to `max(int(rolling_update_percentage * num_replicas), 1)` replicas per rollout wave. The default `rolling_update_percentage` is `0.2`.

When a deployment has multiple replicas, the replicas that aren't restarting can continue serving traffic during the rollout. A deployment with a single replica must restart its only replica during a code update, which causes a brief service interruption.
:::

Changing either of the following application-level config values is also a code update and restarts every deployment in the application:

- `import_path`
- `runtime_env`

:::{warning}
You can update your Serve application by deploying an entirely new application with a different `import_path` and a different `runtime_env`, but avoid this approach in production.

For large-scale code updates, start a new Ray cluster, deploy the updated code to it with `serve deploy`, and then switch traffic from your old cluster to the new one.
:::
