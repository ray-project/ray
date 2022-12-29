package io.ray.serve.router;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorMethod;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.metric.TagKey;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.metrics.RayServeMetrics;
import io.ray.serve.replica.RayServeWrappedReplica;
import io.ray.serve.util.CollectionUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  // The key is the name of the actor, and the value is a set of all flight queries objectrefs of
  // the actor.
  private final Map<String, Set<ObjectRef<Object>>> inFlightQueries;

  // Map the actor name to the handle of the actor.
  private final Map<String, BaseActorHandle> allActorHandles;

  private DeploymentLanguage language;

  private AtomicInteger numQueuedQueries = new AtomicInteger();

  private Gauge numQueuedQueriesGauge;

  private boolean hasPullReplica = false;

  public ReplicaSet(String deploymentName) {
    this.inFlightQueries = new ConcurrentHashMap<>();
    this.allActorHandles = new ConcurrentHashMap<>();
    try {
      Deployment deployment = Serve.getDeployment(deploymentName);
      this.language = deployment.getConfig().getDeploymentLanguage();
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to get language from controller. Set it to Java as default value. The exception is ",
          e);
      this.language = DeploymentLanguage.JAVA;
    }
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
    if (null != actorSet) {
      Set<String> actorNameSet = new HashSet<>(((ActorNameList) actorSet).getNamesList());
      Set<String> added = new HashSet<>(Sets.difference(actorNameSet, inFlightQueries.keySet()));
      Set<String> removed = new HashSet<>(Sets.difference(inFlightQueries.keySet(), actorNameSet));
      added.forEach(
          name -> {
            Optional<BaseActorHandle> handleOptional =
                Ray.getActor(name, Constants.SERVE_NAMESPACE);
            if (handleOptional.isPresent()) {
              allActorHandles.put(name, handleOptional.get());
              inFlightQueries.put(name, Sets.newConcurrentHashSet());
            } else {
              LOGGER.warn("Can not get actor handle. actor name is {}", name);
            }
          });
      removed.forEach(inFlightQueries::remove);
      removed.forEach(allActorHandles::remove);
      if (added.size() > 0 || removed.size() > 0) {
        LOGGER.info("ReplicaSet: +{}, -{} replicas.", added.size(), removed.size());
      }
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
    List<BaseActorHandle> handles = new ArrayList<>(allActorHandles.values());
    if (CollectionUtil.isEmpty(handles)) {
      throw new RayServeException("ReplicaSet found no replica.");
    }
    int randomIndex = RandomUtils.nextInt(0, handles.size());
    BaseActorHandle replica =
        handles.get(randomIndex); // TODO controll concurrency using maxConcurrentQueries
    LOGGER.debug("Assigned query {} to replica {}.", query.getMetadata().getRequestId(), replica);
    if (language == DeploymentLanguage.PYTHON) {
      return ((PyActorHandle) replica)
          .task(
              PyActorMethod.of("handle_request_from_java"),
              query.getMetadata().toByteArray(),
              query.getArgs())
          .remote();
    } else {
      return ((ActorHandle<RayServeWrappedReplica>) replica)
          .task(
              RayServeWrappedReplica::handleRequest,
              query.getMetadata().toByteArray(),
              query.getArgs())
          .remote();
    }
  }

  public Map<String, Set<ObjectRef<Object>>> getInFlightQueries() {
    return inFlightQueries;
  }
}
