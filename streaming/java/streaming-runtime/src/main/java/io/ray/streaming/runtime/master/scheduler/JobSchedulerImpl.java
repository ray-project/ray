package io.ray.streaming.runtime.master.scheduler;

import io.ray.api.RayActor;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.master.graphmanager.GraphManager;
import io.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import io.ray.streaming.runtime.master.resourcemanager.ViewBuilder;
import io.ray.streaming.runtime.master.scheduler.controller.WorkerLifecycleController;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job scheduler implementation.
 */
public class JobSchedulerImpl implements JobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerImpl.class);

  private StreamingConfig jobConf;

  private final JobMaster jobMaster;
  private final ResourceManager resourceManager;
  private final GraphManager graphManager;
  private final WorkerLifecycleController workerLifecycleController;

  public JobSchedulerImpl(JobMaster jobMaster) {
    this.jobMaster = jobMaster;
    this.graphManager = jobMaster.getGraphManager();
    this.resourceManager = jobMaster.getResourceManager();
    this.workerLifecycleController = new WorkerLifecycleController();
    this.jobConf = jobMaster.getRuntimeContext().getConf();

    LOG.info("Scheduler initiated.");
  }

  @Override
  public boolean scheduleJob(ExecutionGraph executionGraph) {
    LOG.info("Begin scheduling. Job: {}.", executionGraph.getJobName());

    // Allocate resource then create workers
    prepareResourceAndCreateWorker(executionGraph);

    // init worker context and start to run
    initAndStart(executionGraph);

    return true;
  }

  /**
   * Allocate job workers' resource then create job workers' actor.
   *
   * @param executionGraph the physical plan
   */
  protected void prepareResourceAndCreateWorker(ExecutionGraph executionGraph) {
    List<Container> containers = resourceManager.getRegisteredContainers();

    // Assign resource for execution vertices
    resourceManager.assignResource(containers, executionGraph);

    LOG.info("Allocating map is: {}.", ViewBuilder.buildResourceAssignmentView(containers));

    // Start all new added workers
    createWorkers(executionGraph);
  }

  /**
   * Init JobMaster and JobWorkers then start JobWorkers.
   *
   * @param executionGraph physical plan
   */
  private void initAndStart(ExecutionGraph executionGraph) {
    // generate vertex - context map
    Map<ExecutionVertex, JobWorkerContext> vertexToContextMap = buildWorkersContext(executionGraph);

    // init workers
    initWorkers(vertexToContextMap);

    // init master
    initMaster();

    // start workers
    startWorkers(executionGraph);
  }

  /**
   * Create JobWorker actors according to the physical plan.
   *
   * @param executionGraph physical plan
   * @return actor creation result
   */
  public boolean createWorkers(ExecutionGraph executionGraph) {
    LOG.info("Begin creating workers.");
    long startTs = System.currentTimeMillis();

    // create JobWorker actors
    boolean createResult = workerLifecycleController
        .createWorkers(executionGraph.getAllAddedExecutionVertices());

    if (createResult) {
      LOG.info("Finished creating workers. Cost {} ms.", System.currentTimeMillis() - startTs);
      return true;
    } else {
      LOG.error("Failed to create workers. Cost {} ms.", System.currentTimeMillis() - startTs);
      return false;
    }
  }

  /**
   * Init JobWorkers according to the vertex and context infos.
   *
   * @param vertexToContextMap vertex - context map
   */
  protected boolean initWorkers(Map<ExecutionVertex, JobWorkerContext> vertexToContextMap) {
    boolean result;
    try {
      result = workerLifecycleController.initWorkers(vertexToContextMap,
          jobConf.masterConfig.schedulerConfig.workerInitiationWaitTimeoutMs());
    } catch (Exception e) {
      LOG.error("Failed to initiate workers.", e);
      return false;
    }
    return result;
  }

  /**
   * Start JobWorkers according to the physical plan.
   */
  public boolean startWorkers(ExecutionGraph executionGraph) {
    boolean result;
    try {
      result = workerLifecycleController.startWorkers(
          executionGraph, jobConf.masterConfig.schedulerConfig.workerStartingWaitTimeoutMs());
    } catch (Exception e) {
      LOG.error("Failed to start workers.", e);
      return false;
    }
    return result;
  }

  /**
   * Build workers context.
   *
   * @param executionGraph execution graph
   * @return vertex to worker context map
   */
  protected Map<ExecutionVertex, JobWorkerContext> buildWorkersContext(
      ExecutionGraph executionGraph) {
    RayActor masterActor = jobMaster.getJobMasterActor();

    // build workers' context
    Map<ExecutionVertex, JobWorkerContext> needRegistryVertexToContextMap = new HashMap<>();
    executionGraph.getAllExecutionVertices().forEach(vertex -> {
      JobWorkerContext ctx = buildJobWorkerContext(vertex, masterActor);
      needRegistryVertexToContextMap.put(vertex, ctx);
    });
    return needRegistryVertexToContextMap;
  }

  private JobWorkerContext buildJobWorkerContext(
      ExecutionVertex executionVertex,
      RayActor<JobMaster> masterActor) {

    // create worker context
    JobWorkerContext ctx = new JobWorkerContext(
        executionVertex.getWorkerActorId(),
        masterActor,
        executionVertex
    );

    return ctx;
  }

  /**
   * Destroy JobWorkers according to the vertex infos.
   *
   * @param executionVertices specified vertices
   */
  public boolean destroyWorkers(List<ExecutionVertex> executionVertices) {
    boolean result;
    try {
      result = workerLifecycleController.destroyWorkers(executionVertices);
    } catch (Exception e) {
      LOG.error("Failed to destroy workers.", e);
      return false;
    }
    return result;
  }

  private void initMaster() {
    jobMaster.init();
  }

}
