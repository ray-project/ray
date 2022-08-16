# Failure Recovery

Ray Serve is resilient to any component failures within the Ray cluster out of the box.
You can read more about how worker node and process failures are handled in: {ref}`serve-ft-detail`.

(serve-health-checking)=

## Health checking

By default, each Serve deployment replica is periodically health-checked and restarted on failure.

You can customize this behavior to perform an application-level health check or to adjust the frequency/timeout.
To define a custom healthcheck, define a `check_health` method on your deployment class.
This method should take no arguments and return no result, and it should raise an exception if the replica should be considered unhealthy.
The raised exception will be logged by the Serve controller if the health check fails.
You can also customize how frequently the health check is run and the timeout after which a replica is marked unhealthy in the deployment options.

> ```python
> @serve.deployment(health_check_period_s=10, health_check_timeout_s=30)
> class MyDeployment:
>     def __init__(self, db_addr: str):
>         self._my_db_connection = connect_to_db(db_addr)
>
>     def __call__(self, request):
>         return self._do_something_cool()
>
>     # Will be called by Serve to check the health of the replica.
>     def check_health(self):
>         if not self._my_db_connection.is_connected():
>             # The specific type of exception is not important.
>             raise RuntimeError("uh-oh, DB connection is broken.")
> ```

## Head node failures

By default the Ray head node is a single point of failure: if it crashes, the entire cluster crashes and needs to be restarted.
When running on Kubernetes, the `RayService` controller health-checks the cluster and restarts it if this occurs, but this still results in some downtime.
In Ray 2.0, [KubeRay](https://ray-project.github.io/kuberay/) has added experimental support for [GCS fault tolerance](https://ray-project.github.io/kuberay/guidance/gcs-ha/#ray-gcs-ha-experimental), preventing the Ray cluster from crashing if the head node goes down.
While the head node is recovering, Serve applications can still handle traffic but cannot be updated or recover from other failures (e.g., actors or worker nodes crashing).
Once the GCS is recovered, the cluster will return to normal behavior.
