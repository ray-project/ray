package io.ray.serve.router;

import com.google.common.collect.ImmutableMap;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Metrics;
import io.ray.serve.deployment.DeploymentId;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.metrics.RayServeMetrics;
import io.ray.serve.poll.KeyListener;
import io.ray.serve.poll.KeyType;
import io.ray.serve.poll.LongPollClient;
import io.ray.serve.poll.LongPollNamespace;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Router process incoming queries: assign a replica. */
public class Router {

  private ReplicaSet replicaSet;
  private LongPollClient longPollClient;

  private Count numRouterRequests;
  private AtomicInteger numQueuedQueries = new AtomicInteger();
  private Gauge numQueuedQueriesGauge;

  public Router(BaseActorHandle controllerHandle, DeploymentId deploymentId) {
    this.replicaSet = new ReplicaSet(deploymentId.getName());

    RayServeMetrics.execute(
        () ->
            this.numRouterRequests =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_NUM_ROUTER_REQUESTS.getName())
                    .description(RayServeMetrics.SERVE_NUM_ROUTER_REQUESTS.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_DEPLOYMENT,
                            deploymentId.getName(),
                            RayServeMetrics.TAG_APPLICATION,
                            deploymentId.getApp()))
                    .register());

    RayServeMetrics.execute(
        () ->
            this.numQueuedQueriesGauge =
                Metrics.gauge()
                    .name(RayServeMetrics.SERVE_DEPLOYMENT_QUEUED_QUERIES.getName())
                    .description(RayServeMetrics.SERVE_DEPLOYMENT_QUEUED_QUERIES.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_DEPLOYMENT,
                            deploymentId.getName(),
                            RayServeMetrics.TAG_APPLICATION,
                            deploymentId.getApp()))
                    .register());

    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(
        new KeyType(LongPollNamespace.DEPLOYMENT_TARGETS, deploymentId.getName()),
        deploymentTargetInfo ->
            replicaSet.updateWorkerReplicas(deploymentTargetInfo)); // cross language
    this.longPollClient = new LongPollClient(controllerHandle, keyListeners);
  }

  /**
   * Assign a query and returns an object ref represent the result.
   *
   * @param requestMetadata the metadata of incoming queries.
   * @param requestArgs the request body of incoming queries.
   * @return ray.ObjectRef
   */
  public ObjectRef<Object> assignRequest(RequestMetadata requestMetadata, Object[] requestArgs) {
    RayServeMetrics.execute(() -> numRouterRequests.inc(1));
    numQueuedQueries.incrementAndGet();
    RayServeMetrics.execute(() -> numQueuedQueriesGauge.update(numQueuedQueries.get()));

    try {
      return replicaSet.assignReplica(new Query(requestMetadata, requestArgs));
    } finally {
      numQueuedQueries.decrementAndGet();
      RayServeMetrics.execute(() -> numQueuedQueriesGauge.update(numQueuedQueries.get()));
    }
  }

  public ReplicaSet getReplicaSet() {
    return replicaSet;
  }

  public LongPollClient getLongPollClient() {
    return longPollClient;
  }
}
