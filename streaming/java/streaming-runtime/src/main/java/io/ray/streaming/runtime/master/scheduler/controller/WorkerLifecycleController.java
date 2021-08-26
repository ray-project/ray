package io.ray.streaming.runtime.master.scheduler.controller;

import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.api.function.PyActorClass;
import io.ray.api.id.ActorId;
import io.ray.streaming.api.Language;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.python.GraphPbBuilder;
import io.ray.streaming.runtime.rpc.RemoteCallWorker;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Worker lifecycle controller is used to control JobWorker's creation, initiation and so on. */
public class WorkerLifecycleController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerLifecycleController.class);

  public boolean createWorkers(List<ExecutionVertex> executionVertices) {
    return asyncBatchExecute(this::createWorker, executionVertices);
  }

  /**
   * Create JobWorker actor according to the execution vertex.
   *
   * @param executionVertex target execution vertex
   * @return creation result
   */
  private boolean createWorker(ExecutionVertex executionVertex) {
    LOG.info(
        "Start to create worker actor for vertex: {} with resource: {}, workeConfig: {}.",
        executionVertex.getExecutionVertexName(),
        executionVertex.getResource(),
        executionVertex.getWorkerConfig());

    Language language = executionVertex.getLanguage();

    BaseActorHandle actor;
    if (Language.JAVA == language) {
      actor =
          Ray.actor(JobWorker::new, executionVertex)
              .setResources(executionVertex.getResource())
              .setMaxRestarts(-1)
              .remote();
    } else {
      RemoteCall.ExecutionVertexContext.ExecutionVertex vertexPb =
          new GraphPbBuilder().buildVertex(executionVertex);
      actor =
          Ray.actor(
                  PyActorClass.of("ray.streaming.runtime.worker", "JobWorker"),
                  vertexPb.toByteArray())
              .setResources(executionVertex.getResource())
              .setMaxRestarts(-1)
              .remote();
    }

    if (null == actor) {
      LOG.error("Create worker actor failed.");
      return false;
    }

    executionVertex.setWorkerActor(actor);

    LOG.info(
        "Worker actor created, actor: {}, vertex: {}.",
        executionVertex.getWorkerActorId(),
        executionVertex.getExecutionVertexName());
    return true;
  }

  /**
   * Using context to init JobWorker.
   *
   * @param vertexToContextMap target JobWorker actor
   * @param timeout timeout for waiting, unit: ms
   * @return initiation result
   */
  public boolean initWorkers(
      Map<ExecutionVertex, JobWorkerContext> vertexToContextMap, int timeout) {
    LOG.info("Begin initiating workers: {}.", vertexToContextMap);
    long startTime = System.currentTimeMillis();

    Map<ObjectRef<Boolean>, ActorId> rayObjects = new HashMap<>();
    vertexToContextMap
        .entrySet()
        .forEach(
            (entry -> {
              ExecutionVertex vertex = entry.getKey();
              rayObjects.put(
                  RemoteCallWorker.initWorker(vertex.getWorkerActor(), entry.getValue()),
                  vertex.getWorkerActorId());
            }));

    List<ObjectRef<Boolean>> objectRefList = new ArrayList<>(rayObjects.keySet());

    LOG.info("Waiting for workers' initialization.");
    WaitResult<Boolean> result = Ray.wait(objectRefList, objectRefList.size(), timeout);
    if (result.getReady().size() != objectRefList.size()) {
      LOG.error("Initializing workers timeout[{} ms].", timeout);
      return false;
    }

    LOG.info("Finished waiting workers' initialization.");
    LOG.info("Workers initialized. Cost {} ms.", System.currentTimeMillis() - startTime);
    return true;
  }

  /**
   * Start JobWorkers to run task.
   *
   * @param executionGraph physical plan
   * @param timeout timeout for waiting, unit: ms
   * @return starting result
   */
  public boolean startWorkers(ExecutionGraph executionGraph, long lastCheckpointId, int timeout) {
    LOG.info("Begin starting workers.");
    long startTime = System.currentTimeMillis();
    List<ObjectRef<Object>> objectRefs = new ArrayList<>();

    // start source actors 1st
    executionGraph
        .getSourceActors()
        .forEach(actor -> objectRefs.add(RemoteCallWorker.rollback(actor, lastCheckpointId)));

    // then start non-source actors
    executionGraph
        .getNonSourceActors()
        .forEach(actor -> objectRefs.add(RemoteCallWorker.rollback(actor, lastCheckpointId)));

    WaitResult<Object> result = Ray.wait(objectRefs, objectRefs.size(), timeout);
    if (result.getReady().size() != objectRefs.size()) {
      LOG.error("Starting workers timeout[{} ms].", timeout);
      return false;
    }

    LOG.info("Workers started. Cost {} ms.", System.currentTimeMillis() - startTime);
    return true;
  }

  /**
   * Stop and destroy JobWorkers' actor.
   *
   * @param executionVertices target vertices
   * @return destroy result
   */
  public boolean destroyWorkers(List<ExecutionVertex> executionVertices) {
    return asyncBatchExecute(this::destroyWorker, executionVertices);
  }

  private boolean destroyWorker(ExecutionVertex executionVertex) {
    BaseActorHandle rayActor = executionVertex.getWorkerActor();
    LOG.info(
        "Begin destroying worker[vertex={}, actor={}].",
        executionVertex.getExecutionVertexName(),
        rayActor.getId());

    boolean destroyResult = RemoteCallWorker.shutdownWithoutReconstruction(rayActor);

    if (!destroyResult) {
      LOG.error(
          "Failed to destroy JobWorker[{}]'s actor: {}.",
          executionVertex.getExecutionVertexName(),
          rayActor);
      return false;
    }

    LOG.info("Worker destroyed, actor: {}.", rayActor);
    return true;
  }

  /**
   * Async batch execute function, for some cases that could not use Ray.wait
   *
   * @param operation the function to be executed
   */
  private boolean asyncBatchExecute(
      Function<ExecutionVertex, Boolean> operation, List<ExecutionVertex> executionVertices) {
    final Object asyncContext = Ray.getAsyncContext();

    List<CompletableFuture<Boolean>> futureResults =
        executionVertices.stream()
            .map(
                vertex ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          Ray.setAsyncContext(asyncContext);
                          return operation.apply(vertex);
                        }))
            .collect(Collectors.toList());

    List<Boolean> succeeded =
        futureResults.stream().map(CompletableFuture::join).collect(Collectors.toList());

    if (succeeded.stream().anyMatch(x -> !x)) {
      LOG.error("Not all futures return true, check ResourceManager'log the detail.");
      return false;
    }
    return true;
  }
}
