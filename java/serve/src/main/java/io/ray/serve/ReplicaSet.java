package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.metric.TagKey;
import io.ray.serve.generated.BackendConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Data structure representing a set of replica actor handles. */
public class ReplicaSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaSet.class);

  private volatile int maxConcurrentQueries = 8;

  private final Map<ActorHandle<RayServeWrappedReplica>, Set<ObjectRef<Object>>> inFlightQueries;

  private AtomicInteger numQueuedQueries = new AtomicInteger();

  private Gauge numQueuedQueriesGauge;

  public ReplicaSet(String backendTag) {
    this.inFlightQueries = new ConcurrentHashMap<>();
    RayServeMetrics.execute(
        () ->
            this.numQueuedQueriesGauge =
                Metrics.gauge()
                    .name(RayServeMetrics.SERVE_DEPLOYMENT_QUEUED_QUERIES.getName())
                    .description(RayServeMetrics.SERVE_DEPLOYMENT_QUEUED_QUERIES.getDescription())
                    .unit("")
                    .tags(ImmutableMap.of(RayServeMetrics.TAG_DEPLOYMENT, backendTag))
                    .register());
  }

  public void setMaxConcurrentQueries(Object backendConfig) {
    int newValue = ((BackendConfig) backendConfig).getMaxConcurrentQueries();
    if (newValue != this.maxConcurrentQueries) {
      this.maxConcurrentQueries = newValue;
      LOGGER.debug("ReplicaSet: changing max_concurrent_queries to {}", newValue);
      // TODO self.config_updated_event.set()
    }
  }

  @SuppressWarnings("unchecked")
  public synchronized void updateWorkerReplicas(Object workerReplicas) {
    Set<ActorHandle<RayServeWrappedReplica>> added =
        Sets.difference(
            (Set<ActorHandle<RayServeWrappedReplica>>) workerReplicas, inFlightQueries.keySet());
    Set<ActorHandle<RayServeWrappedReplica>> removed =
        Sets.difference(
            inFlightQueries.keySet(), (Set<ActorHandle<RayServeWrappedReplica>>) workerReplicas);

    added.forEach(actorHandle -> inFlightQueries.put(actorHandle, Sets.newConcurrentHashSet()));
    removed.forEach(actorHandle -> inFlightQueries.remove(actorHandle));

    if (added.size() > 0 || removed.size() > 0) {
      LOGGER.debug("ReplicaSet: +{}, -{} replicas.", added.size(), removed.size());
      // TODO self.config_updated_event.set()
    }
  }

  /**
   * Given a query, submit it to a replica and return the object ref. This method will keep track of
   * the in flight queries for each replicas and only send a query to available replicas (determined
   * by the backend max_concurrent_quries value.)
   *
   * @param query the incoming query.
   * @return ray.ObjectRef
   */
  public ObjectRef<Object> assignReplica(Query query) {
    String endpoint = query.getMetadata().getEndpoint();
    numQueuedQueries.incrementAndGet();
    RayServeMetrics.execute(
        () ->
            numQueuedQueriesGauge.update(
                numQueuedQueries.get(),
                TagKey.tagsFromMap(ImmutableMap.of(RayServeMetrics.TAG_ENDPOINT, endpoint))));
    ObjectRef<Object> assignedRef = tryAssignReplica(query);

    while (assignedRef == null) { // Can't assign a replica right now.
      LOGGER.debug("Failed to assign a replica for query {}", query.getMetadata().getRequestId());
      // Maybe there exists a free replica, we just need to refresh our query tracker.

      int numFinished = drainCompletedObjectRefs();
      // All replicas are really busy, wait for a query to complete or the config to be updated.
      if (numFinished == 0) {
        LOGGER.debug("All replicas are busy, waiting for a free replica.");

        Ray.wait(null, numFinished);

        /*await asyncio.wait(
            self._all_query_refs + [self.config_updated_event.wait()],
            return_when=asyncio.FIRST_COMPLETED)
        if self.config_updated_event. is_set():
            self.config_updated_event.clear()*/
        // TODO
      }
      assignedRef = tryAssignReplica(query);
    }

    numQueuedQueries.decrementAndGet();
    RayServeMetrics.execute(
        () ->
            numQueuedQueriesGauge.update(
                numQueuedQueries.get(),
                TagKey.tagsFromMap(ImmutableMap.of(RayServeMetrics.TAG_ENDPOINT, endpoint))));
    return assignedRef;
  }

  /**
   * Try to assign query to a replica, return the object ref if succeeded or return None if it can't
   * assign this query to any replicas.
   *
   * @param query query the incoming query.
   * @return ray.ObjectRef
   */
  private ObjectRef<Object> tryAssignReplica(Query query) {

    List<ActorHandle<RayServeWrappedReplica>> handles = new ArrayList<>(inFlightQueries.keySet());
    Collections.shuffle(handles);
    for (ActorHandle<RayServeWrappedReplica> replica : handles) {
      if (inFlightQueries.get(replica).size() >= maxConcurrentQueries) {
        // This replica is overloaded, try next one
        continue;
      }
      LOGGER.debug("Assigned query {} to replica {}.", query.getMetadata().getRequestId(), replica);
      // Directly passing args because it might contain an ObjectRef.
      ObjectRef<Object> userRef =
          replica
              .task(RayServeWrappedReplica::handleRequest, query.getMetadata(), query.getArgs())
              .remote();
      inFlightQueries.get(replica).add(userRef);
      return userRef;
    }
    return null;
  }

  private synchronized int drainCompletedObjectRefs() {
    List<ObjectRef<Object>> refs =
        inFlightQueries
            .values()
            .stream()
            .flatMap(value -> value.stream())
            .collect(Collectors.toList());
    WaitResult<Object> waitResult = Ray.wait(refs, refs.size(), 0);
    inFlightQueries
        .values()
        .stream()
        .forEach(replicaInFlightQueries -> replicaInFlightQueries.removeAll(waitResult.getReady()));

    return waitResult.getReady().size();
  }
}
