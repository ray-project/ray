package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.collect.Sets;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Physical plan. */
public class ExecutionGraph implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

  /** Name of the job. */
  private final String jobName;

  /** Configuration of the job. */
  private Map<String, String> jobConfig;

  /** Data map for execution job vertex. key: job vertex id. value: execution job vertex. */
  private Map<Integer, ExecutionJobVertex> executionJobVertexMap;

  /** Data map for execution vertex. key: execution vertex id. value: execution vertex. */
  private Map<Integer, ExecutionVertex> executionVertexMap;

  /** Data map for execution vertex. key: actor id. value: execution vertex. */
  private Map<ActorId, ExecutionVertex> actorIdExecutionVertexMap;

  /** key: channel ID value: actors in both sides of this channel */
  private Map<String, Set<BaseActorHandle>> channelGroupedActors;

  /** The max parallelism of the whole graph. */
  private int maxParallelism;

  /** Build time. */
  private long buildTime;

  /** A monotonic increasing number, used for vertex's id(immutable). */
  private AtomicInteger executionVertexIdGenerator = new AtomicInteger(0);

  public ExecutionGraph(String jobName) {
    this.jobName = jobName;
    this.buildTime = System.currentTimeMillis();
  }

  public String getJobName() {
    return jobName;
  }

  public List<ExecutionJobVertex> getExecutionJobVertexList() {
    return new ArrayList<>(executionJobVertexMap.values());
  }

  public Map<Integer, ExecutionJobVertex> getExecutionJobVertexMap() {
    return executionJobVertexMap;
  }

  public void setExecutionJobVertexMap(Map<Integer, ExecutionJobVertex> executionJobVertexMap) {
    this.executionJobVertexMap = executionJobVertexMap;
  }

  /**
   * generate relation mappings between actors, execution vertices and channels this method must be
   * called after worker actor is set.
   */
  public void generateActorMappings() {
    LOG.info("Setup queue actors relation.");

    channelGroupedActors = new HashMap<>();
    actorIdExecutionVertexMap = new HashMap<>();

    getAllExecutionVertices()
        .forEach(
            curVertex -> {

              // current
              actorIdExecutionVertexMap.put(curVertex.getActorId(), curVertex);

              // input
              List<ExecutionEdge> inputEdges = curVertex.getInputEdges();
              inputEdges.forEach(
                  inputEdge -> {
                    ExecutionVertex inputVertex = inputEdge.getSourceExecutionVertex();
                    String channelId = curVertex.getChannelIdByPeerVertex(inputVertex);
                    addActorToChannelGroupedActors(
                        channelGroupedActors, channelId, inputVertex.getWorkerActor());
                  });

              // output
              List<ExecutionEdge> outputEdges = curVertex.getOutputEdges();
              outputEdges.forEach(
                  outputEdge -> {
                    ExecutionVertex outputVertex = outputEdge.getTargetExecutionVertex();
                    String channelId = curVertex.getChannelIdByPeerVertex(outputVertex);
                    addActorToChannelGroupedActors(
                        channelGroupedActors, channelId, outputVertex.getWorkerActor());
                  });
            });

    LOG.debug("Channel grouped actors is: {}.", channelGroupedActors);
  }

  private void addActorToChannelGroupedActors(
      Map<String, Set<BaseActorHandle>> channelGroupedActors,
      String queueName,
      BaseActorHandle actor) {

    Set<BaseActorHandle> actorSet =
        channelGroupedActors.computeIfAbsent(queueName, k -> new HashSet<>());
    actorSet.add(actor);
  }

  public void setExecutionVertexMap(Map<Integer, ExecutionVertex> executionVertexMap) {
    this.executionVertexMap = executionVertexMap;
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
  }

  public void setJobConfig(Map<String, String> jobConfig) {
    this.jobConfig = jobConfig;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public void setMaxParallelism(int maxParallelism) {
    this.maxParallelism = maxParallelism;
  }

  public long getBuildTime() {
    return buildTime;
  }

  public int generateExecutionVertexId() {
    return executionVertexIdGenerator.getAndIncrement();
  }

  public AtomicInteger getExecutionVertexIdGenerator() {
    return executionVertexIdGenerator;
  }

  /**
   * Get all execution vertices from current execution graph.
   *
   * @return all execution vertices.
   */
  public List<ExecutionVertex> getAllExecutionVertices() {
    return executionJobVertexMap.values().stream()
        .map(ExecutionJobVertex::getExecutionVertices)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /**
   * Get all execution vertices whose status is 'TO_ADD' from current execution graph.
   *
   * @return all added execution vertices.
   */
  public List<ExecutionVertex> getAllAddedExecutionVertices() {
    return executionJobVertexMap.values().stream()
        .map(ExecutionJobVertex::getExecutionVertices)
        .flatMap(Collection::stream)
        .filter(ExecutionVertex::is2Add)
        .collect(Collectors.toList());
  }

  /**
   * Get specified execution vertex from current execution graph by execution vertex id.
   *
   * @param executionVertexId execution vertex id.
   * @return the specified execution vertex.
   */
  public ExecutionVertex getExecutionVertexByExecutionVertexId(int executionVertexId) {
    if (executionVertexMap.containsKey(executionVertexId)) {
      return executionVertexMap.get(executionVertexId);
    }
    throw new RuntimeException("Vertex " + executionVertexId + " does not exist!");
  }

  /**
   * Get specified execution vertex from current execution graph by actor id.
   *
   * @param actorId the actor id of execution vertex.
   * @return the specified execution vertex.
   */
  public ExecutionVertex getExecutionVertexByActorId(ActorId actorId) {
    return actorIdExecutionVertexMap.get(actorId);
  }

  /**
   * Get specified actor by actor id.
   *
   * @param actorId the actor id of execution vertex.
   * @return the specified actor handle.
   */
  public Optional<BaseActorHandle> getActorById(ActorId actorId) {
    return getAllActors().stream().filter(actor -> actor.getId().equals(actorId)).findFirst();
  }

  /**
   * Get the peer actor in the other side of channelName of a given actor
   *
   * @param actor actor in this side
   * @param channelName the channel name
   * @return the peer actor in the other side
   */
  public BaseActorHandle getPeerActor(BaseActorHandle actor, String channelName) {
    Set<BaseActorHandle> set = getActorsByChannelId(channelName);
    final BaseActorHandle[] res = new BaseActorHandle[1];
    set.forEach(
        anActor -> {
          if (!anActor.equals(actor)) {
            res[0] = anActor;
          }
        });
    return res[0];
  }

  /**
   * Get actors in both sides of a channelId
   *
   * @param channelId the channelId
   * @return actors in both sides
   */
  public Set<BaseActorHandle> getActorsByChannelId(String channelId) {
    return channelGroupedActors.getOrDefault(channelId, Sets.newHashSet());
  }

  /**
   * Get all actors by graph.
   *
   * @return actor list
   */
  public List<BaseActorHandle> getAllActors() {
    return getActorsFromJobVertices(getExecutionJobVertexList());
  }

  /**
   * Get source actors by graph.
   *
   * @return actor list
   */
  public List<BaseActorHandle> getSourceActors() {
    List<ExecutionJobVertex> executionJobVertices =
        getExecutionJobVertexList().stream()
            .filter(ExecutionJobVertex::isSourceVertex)
            .collect(Collectors.toList());

    return getActorsFromJobVertices(executionJobVertices);
  }

  /**
   * Get transformation and sink actors by graph.
   *
   * @return actor list
   */
  public List<BaseActorHandle> getNonSourceActors() {
    List<ExecutionJobVertex> executionJobVertices =
        getExecutionJobVertexList().stream()
            .filter(
                executionJobVertex ->
                    executionJobVertex.isTransformationVertex()
                        || executionJobVertex.isSinkVertex())
            .collect(Collectors.toList());

    return getActorsFromJobVertices(executionJobVertices);
  }

  /**
   * Get sink actors by graph.
   *
   * @return actor list
   */
  public List<BaseActorHandle> getSinkActors() {
    List<ExecutionJobVertex> executionJobVertices =
        getExecutionJobVertexList().stream()
            .filter(ExecutionJobVertex::isSinkVertex)
            .collect(Collectors.toList());

    return getActorsFromJobVertices(executionJobVertices);
  }

  /**
   * Get actors according to job vertices.
   *
   * @param executionJobVertices specified job vertices
   * @return actor list
   */
  public List<BaseActorHandle> getActorsFromJobVertices(
      List<ExecutionJobVertex> executionJobVertices) {
    return executionJobVertices.stream()
        .map(ExecutionJobVertex::getExecutionVertices)
        .flatMap(Collection::stream)
        .map(ExecutionVertex::getWorkerActor)
        .collect(Collectors.toList());
  }

  public Set<String> getActorName(Set<ActorId> actorIds) {
    return getAllExecutionVertices().stream()
        .filter(executionVertex -> actorIds.contains(executionVertex.getActorId()))
        .map(ExecutionVertex::getActorName)
        .collect(Collectors.toSet());
  }

  public String getActorName(ActorId actorId) {
    Set<ActorId> set = Sets.newHashSet();
    set.add(actorId);
    Set<String> result = getActorName(set);
    if (result.isEmpty()) {
      return null;
    }
    return result.iterator().next();
  }

  public List<ActorId> getAllActorsId() {
    return getAllActors().stream().map(BaseActorHandle::getId).collect(Collectors.toList());
  }
}
