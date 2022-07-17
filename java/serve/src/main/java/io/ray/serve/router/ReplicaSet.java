package io.ray.serve.router;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.metric.TagKey;
import io.ray.serve.common.Constants;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.metrics.RayServeMetrics;
import io.ray.serve.replica.RayServeWrappedReplica;
import io.ray.serve.util.CollectionUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Data structure representing a set of replica actor handles. */
public class ReplicaSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaSet.class);

  private final Map<ActorHandle<RayServeWrappedReplica>, Set<ObjectRef<Object>>> inFlightQueries;

  private AtomicInteger numQueuedQueries = new AtomicInteger();

  private Gauge numQueuedQueriesGauge;

  private boolean hasPullReplica = false;

  public ReplicaSet(String deploymentName) {
    this.inFlightQueries = new ConcurrentHashMap<>();
    RayServeMetrics.execute(
        () ->
            this.numQueuedQueriesGauge =
                Metrics.gauge()
                    .name(RayServeMetrics.SERVE_DEPLOYMENT_QUEUED_QUERIES.getName())
                    .description(RayServeMetrics.SERVE_DEPLOYMENT_QUEUED_QUERIES.getDescription())
                    .unit("")
                    .tags(ImmutableMap.of(RayServeMetrics.TAG_DEPLOYMENT, deploymentName))
                    .register());
  }

  @SuppressWarnings("unchecked")
  public synchronized void updateWorkerReplicas(Object actorSet) {
    List<String> actorNames = ((ActorNameList) actorSet).getNamesList();
    Set<ActorHandle<RayServeWrappedReplica>> workerReplicas = new HashSet<>();
    if (!CollectionUtil.isEmpty(actorNames)) {
      actorNames.forEach(
          name ->
              workerReplicas.add(
                  (ActorHandle<RayServeWrappedReplica>)
                      Ray.getActor(name, Constants.SERVE_NAMESPACE).get()));
    }

    Set<ActorHandle<RayServeWrappedReplica>> added =
        new HashSet<>(Sets.difference(workerReplicas, inFlightQueries.keySet()));
    Set<ActorHandle<RayServeWrappedReplica>> removed =
        new HashSet<>(Sets.difference(inFlightQueries.keySet(), workerReplicas));

    added.forEach(actorHandle -> inFlightQueries.put(actorHandle, Sets.newConcurrentHashSet()));
    removed.forEach(inFlightQueries::remove);

    if (added.size() > 0 || removed.size() > 0) {
      LOGGER.info("ReplicaSet: +{}, -{} replicas.", added.size(), removed.size());
    }
    hasPullReplica = true;
  }

  /**
   * Given a query, submit it to a replica and return the object ref. This method will keep track of
   * the in flight queries for each replicas and only send a query to available replicas (determined
   * by the max_concurrent_quries value.)
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
                ImmutableMap.of(new TagKey(RayServeMetrics.TAG_ENDPOINT), endpoint)));
    ObjectRef<Object> assignedRef = tryAssignReplica(query);
    numQueuedQueries.decrementAndGet();
    RayServeMetrics.execute(
        () ->
            numQueuedQueriesGauge.update(
                numQueuedQueries.get(),
                ImmutableMap.of(new TagKey(RayServeMetrics.TAG_ENDPOINT), endpoint)));
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
    int loopCount = 0;
    while (!hasPullReplica && loopCount < 50) {
      try {
        TimeUnit.MICROSECONDS.sleep(20);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      loopCount++;
    }
    List<ActorHandle<RayServeWrappedReplica>> handles = new ArrayList<>(inFlightQueries.keySet());
    if (CollectionUtil.isEmpty(handles)) {
      throw new RayServeException("ReplicaSet found no replica.");
    }
    int randomIndex = RandomUtils.nextInt(0, handles.size());
    ActorHandle<RayServeWrappedReplica> replica =
        handles.get(randomIndex); // TODO controll concurrency using maxConcurrentQueries
    LOGGER.debug("Assigned query {} to replica {}.", query.getMetadata().getRequestId(), replica);
    return replica
        .task(RayServeWrappedReplica::handleRequest, query.getMetadata(), query.getArgs())
        .remote();
  }

  public Map<ActorHandle<RayServeWrappedReplica>, Set<ObjectRef<Object>>> getInFlightQueries() {
    return inFlightQueries;
  }
}
