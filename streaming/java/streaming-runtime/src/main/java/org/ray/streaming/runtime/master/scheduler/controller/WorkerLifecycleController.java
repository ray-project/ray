package org.ray.streaming.runtime.master.scheduler.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.ActorId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.streaming.api.Language;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.rpc.RemoteCallWorker;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker lifecycle controller is used to control JobWorker's creation, initiation and so on.
 */
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
    LOG.info("Start to create worker actor for vertex: {} with resource: {}.",
        executionVertex.getVertexName(), executionVertex.getResources());

    Language language = executionVertex.getLanguage();

    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setResources(executionVertex.getResources())
        .setMaxReconstructions(ActorCreationOptions.INFINITE_RECONSTRUCTION)
        .createActorCreationOptions();

    RayActor<JobWorker> actor = null;
    if (Language.JAVA == language) {
      // TODO (yunye)
    } else {
      // TODO (yunye)
    }
    
    if (null == actor) {
      LOG.error("Create worker actor failed.");
      return false;
    }

    executionVertex.setWorkerActor(actor);

    if (executionVertex.getSlot() != null) {
      executionVertex.getSlot().getActorCount().incrementAndGet();
    }

    LOG.info("Create worker actor succeeded, actor: {}, vertex: {}.",
        executionVertex.getWorkerActorId(), executionVertex.getVertexName());
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
    LOG.info("Start to init workers: {}.", vertexToContextMap);
    long startTime = System.currentTimeMillis();

    Map<RayObject<Boolean>, ActorId> rayObjects = new HashMap<>();
    vertexToContextMap.entrySet().forEach((entry -> {
      ExecutionVertex vertex = entry.getKey();
      rayObjects.put(RemoteCallWorker.initWorker(vertex.getWorkerActor(), entry.getValue()),
          vertex.getWorkerActorId());
    }));

    List<RayObject<Boolean>> rayObjectList = new ArrayList<>(rayObjects.keySet());

    LOG.info("Start to wait for workers' initialization.");
    WaitResult<Boolean> result = Ray.wait(rayObjectList, rayObjectList.size(), timeout);
    if (result.getReady().size() != rayObjectList.size()) {
      LOG.error("Initialize workers timeout[{} ms].", timeout);
      return false;
    }

    LOG.info("Finish waiting workers' initialization.");
    LOG.info("Finish initializing workers. Cost {} ms.", System.currentTimeMillis() - startTime);
    return true;
  }

  /**
   * Start JobWorkers to run task.
   *
   * @param executionGraph physical plan
   * @param timeout timeout for waiting, unit: ms
   * @return starting result
   */
  public boolean startWorkers(ExecutionGraph executionGraph, int timeout) {
    LOG.info("Start to start workers.");
    long startTime = System.currentTimeMillis();
    List<RayObject<Boolean>> rayObjects = new ArrayList<>();

    // start source actors 1st
    executionGraph.getSourceActors()
        .forEach(actor -> rayObjects.add(RemoteCallWorker.startWorker(actor)));

    // then start non-source actors
    executionGraph.getNonSourceActors()
        .forEach(actor -> rayObjects.add(RemoteCallWorker.startWorker(actor)));

    WaitResult<Boolean> result = Ray.wait(rayObjects, rayObjects.size(), timeout);
    if (result.getReady().size() != rayObjects.size()) {
      LOG.error("Start workers timeout[{} ms].", timeout);
      return false;
    }

    LOG.info("Start workers succeeded. Cost {} ms.", System.currentTimeMillis() - startTime);
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
    RayActor rayActor = executionVertex.getWorkerActor();
    LOG.info("Start to destroy worker[vertex={}, actor={}].",
        executionVertex.getVertexName(), rayActor.getId());

    boolean destroyResult = RemoteCallWorker.shutdownWithoutReconstruction(rayActor);

    if (!destroyResult) {
      LOG.error("Failed to destroy JobWorker[{}]'s actor: {}.",
          executionVertex.getVertexName(), rayActor);
      return false;
    }

    LOG.info("Destroy JobWorker succeeded, actor: {}.", rayActor);
    return true;
  }

  /**
   * Async batch execute function, for some cases that could not use Ray.wait
   *
   * @param operation the function to be executed
   */
  private boolean asyncBatchExecute(
      Function<ExecutionVertex, Boolean> operation,
      List<ExecutionVertex> executionVertices) {
    final Object asyncContext = Ray.getAsyncContext();

    List<CompletableFuture<Boolean>> futureResults = executionVertices.stream()
        .map(vertex -> CompletableFuture.supplyAsync(() -> {
          Ray.setAsyncContext(asyncContext);
          return operation.apply(vertex);
        })).collect(Collectors.toList());

    List<Boolean> succeeded = futureResults.stream().map(CompletableFuture::join)
        .collect(Collectors.toList());

    if (succeeded.stream().anyMatch(x -> !x)) {
      LOG.error("Not all futures return true, check ResourceManager'log the detail.");
      return false;
    }
    return true;
  }

}
