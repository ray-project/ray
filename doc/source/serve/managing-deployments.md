(serve-managing-deployments-guide)=

# Managing Deployments

This section should help you:

- create, query, update and configure deployments
- configure resources of your deployments
- specify different Python dependencies across different deployment using Runtime Environments

:::{tip}
Get in touch with us if you're using or considering using [Ray Serve](https://docs.google.com/forms/d/1l8HT35jXMPtxVUtQPeGoe09VGp5jcvSv0TqPgyz6lGU).
:::

```{contents}
```

## Updating a Deployment

Often you want to be able to update your code or configuration options for a deployment over time.
Deployments can be updated simply by updating the code or configuration options and calling `serve.run()` again.

```{literalinclude} ../serve/doc_code/managing_deployments.py
:start-after: __updating_a_deployment_start__
:end-before: __updating_a_deployment_end__
:language: python
```

By default, each call to `serve.run()` will cause a redeployment, even if the underlying code and options didn't change.
This could be detrimental if you have many deployments in a script and and only want to update one: if you re-run the script, all of the deployments will be redeployed, not just the one you updated.
To prevent this, you may provide a `version` string for the deployment as a keyword argument in the decorator or `Deployment.options()`.
When a redeployment happens, Serve will perform a rolling update, bringing down at most 20% of the replicas at any given time.

(managing-deployments-user-configuration)=

## User Configuration

Suppose you want to update a parameter in your model without needing to restart
the replicas in your deployment.  You can do this by writing a `reconfigure` method
for the class underlying your deployment.  At runtime, you can then pass in your
new parameters by setting the `user_config` option.

The following simple example will make the usage clear:

```{literalinclude} ../../../python/ray/serve/examples/doc/snippet_reconfigure.py
```

The `reconfigure` method is called when the class is created if `user_config`
is set.  In particular, it's also called when new replicas are created in the
future if scale up your deployment later.  The `reconfigure` method is also  called
each time `user_config` is updated.

:::{note}
The `user_config` and its contents must be JSON-serializable.
:::
