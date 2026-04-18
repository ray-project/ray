(serve-deployment-scoped-actors)=
# Use deployment-scoped actors

:::{warning}
Deployment-scoped actors are experimental and may change between Ray minor versions.
:::

A deployment-scoped actor is a Ray actor that the Serve controller starts and manages for one deployment. All replicas in that deployment share the actor. The actor outlives individual replica restarts, and Ray Serve cleans it up when you delete the application or call `serve.shutdown()`.

Ray Serve already uses this pattern in the [custom request router guide](custom-request-router). The experimental centralized capacity queue router uses a deployment-scoped actor to keep shared routing state in one place while the controller manages its lifecycle.

## Decide whether to use deployment-scoped actors

Deployment-scoped actors work best when you need controller-managed shared state or coordination for one deployment, such as:

- A shared counter, cache, prefix tree, or rate limiter that every replica should see.
- A coordinator that should survive replica restarts and rolling updates.
- A helper actor that should inherit the deployment's environment and be cleaned up with the deployment.
- Centralized control-plane logic, such as a queue or router helper, where one actor prevents each replica from keeping a conflicting local view.

Deployment-scoped actors aren't a good fit when:

- The state belongs to one replica. Keep it inside the replica instead.
- The state must stay durable across cluster loss. Use an external database or cache instead.
- The actor would sit on the hot path for high-volume data-plane work and become a serialization bottleneck.
- Multiple deployments or non-Serve jobs need to share the same service. Use a dedicated Ray actor or an external service instead.

## Define a deployment-scoped actor

The following example defines a shared counter actor and attaches it to a deployment with `DeploymentActorConfig`:

```{literalinclude} ../doc_code/deployment_scoped_actors.py
:start-after: __begin_define_deployment_scoped_actor__
:end-before: __end_define_deployment_scoped_actor__
:language: python
```

Every replica of `SharedCounterDeployment` reads and updates the same `SharedCounter` actor. `serve.get_deployment_actor()` only works inside a running Serve replica, so call it from the deployment's methods or constructor, not from driver code.

## Run the example

The example file is runnable as written:

```{literalinclude} ../doc_code/deployment_scoped_actors.py
:start-after: __begin_run_deployment_scoped_actor_example__
:end-before: __end_run_deployment_scoped_actor_example__
:language: python
```

When you run the script, the two requests return increasing values because they hit the same shared actor state.

## Handle actor recreation

Ray Serve health-checks deployment-scoped actors and may recreate them after failures. A handle cached before recreation can become stale. The safest pattern is to call `serve.get_deployment_actor()` when you need the actor. If you want to cache the handle, refresh it on `RayActorError` and retry the lookup until the new actor is registered:

```{literalinclude} ../doc_code/deployment_scoped_actors.py
:start-after: __begin_cached_handle_refresh__
:end-before: __end_cached_handle_refresh__
:language: python
```

This pattern comes from the existing Serve recovery tests. It matters most when the actor manages long-lived coordination state and you want to avoid a lookup on every request.

## Understand rollout behavior

Changes to `deployment_actors` are heavyweight deployment changes. If you change actor configuration, such as the actor class, actor name, `init_args`, `init_kwargs`, or actor options, Ray Serve rolls out a new deployment version and restarts replicas in that deployment.

That rollout has a few important properties:

- If you redeploy with the same deployment-actor config, Ray Serve keeps the existing actor.
- If you change the deployment-actor config, Ray Serve creates a new actor for the new version and the old actor state doesn't carry over.
- During an incremental rollout, Ray Serve can keep both old and new deployment-scoped actors alive at the same time.
- Ray Serve starts the new version's deployment-scoped actors before it starts any replicas for the new version.
- Ray Serve doesn't stop the old version's deployment-scoped actors until every old-version replica has drained and stopped.

This ordering matters because `serve.get_deployment_actor()` resolves the actor for the replica's own code version. In other words, old replicas keep talking to old actors while new replicas talk to new actors during the rollout window.

## Understand health and status behavior

Ray Serve health-checks deployment-scoped actors, and deployment health depends on them.

- If a deployment-scoped actor can't start after retries, the deployment moves to `DEPLOY_FAILED`.
- If one actor fails to start in a deployment with multiple deployment-scoped actors, Ray Serve doesn't partially succeed. The deployment still fails.
- If a deployment actor dies and Ray Serve is still managing it, the controller recreates it and the deployment can recover to healthy.
- If the replacement actor is blocked in its constructor or otherwise can't become ready, the deployment stays `UNHEALTHY` until the actor becomes healthy again.
- A replica health check can also depend on a deployment-scoped actor. If the replica's `check_health` method calls the actor and that call fails repeatedly, Ray Serve can restart the replica even if the actor itself still exists.

## Understand lifecycle and limits

- The actor is scoped to one deployment in one application. Different apps can reuse the same logical actor name without colliding.
- Ray Serve shares the actor across all replicas of that deployment, including autoscaled replicas.
- Ray Serve cleans up the actor when you delete the application or shut Serve down.
- When you delete an application, Ray Serve keeps deployment-scoped actors alive until in-flight requests finish and the old replicas stop draining.
- The actor can survive a Serve controller restart because the controller rediscovers it.
- If the actor constructor keeps failing, the deployment fails to deploy.
- `actor_options` on the deployment actor let you reserve different resources or override parts of the deployment `runtime_env`.

:::{note}
Deployment-scoped actors follow Ray actor concurrency semantics. If the actor uses threaded execution, Ray defaults `max_concurrency` to `1`. If the actor is an async actor, Ray defaults `max_concurrency` to `1000`. If those defaults don't fit your workload, set `max_concurrency` explicitly in `actor_options`.
:::

Even though the actor can survive replica restarts and controller restarts, it isn't a substitute for durable external storage.
